//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"
)

// oneSession returns a single driver connection, i.e. one MiniFlake session:
// USE and the session's current database/schema only apply within it.
func oneSession(t *testing.T) *sql.Conn {
	t.Helper()
	conn, err := openDB(t).Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func connExec(t *testing.T, c *sql.Conn, query string, args ...interface{}) {
	t.Helper()
	if _, err := c.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q failed: %v", query, err)
	}
}

func connScalar(t *testing.T, c *sql.Conn, query string, args ...interface{}) string {
	t.Helper()
	var v sql.NullString
	if err := c.QueryRowContext(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	if !v.Valid {
		return "NULL"
	}
	return v.String
}

func snowflakeErr(t *testing.T, err error) *sf.SnowflakeError {
	t.Helper()
	var sfErr *sf.SnowflakeError
	if !errors.As(err, &sfErr) {
		t.Fatalf("err = %v (%T), want *gosnowflake.SnowflakeError", err, err)
	}
	return sfErr
}

// #33: USE DATABASE / USE SCHEMA change where names resolve, per session.
func TestUseSchemaScopesNameResolution(t *testing.T) {
	c := oneSession(t)
	connExec(t, c, "CREATE DATABASE IF NOT EXISTS USE_DB1")
	connExec(t, c, "CREATE SCHEMA IF NOT EXISTS USE_DB1.SCH1")
	connExec(t, c, "USE SCHEMA USE_DB1.SCH1")

	var db, schema string
	if err := c.QueryRowContext(context.Background(), "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").Scan(&db, &schema); err != nil {
		t.Fatal(err)
	}
	if db != "USE_DB1" || schema != "SCH1" {
		t.Fatalf("CURRENT_DATABASE(), CURRENT_SCHEMA() = %s, %s; want USE_DB1, SCH1", db, schema)
	}

	connExec(t, c, "CREATE OR REPLACE TABLE T1 (ID NUMBER, NAME VARCHAR)")
	connExec(t, c, "INSERT INTO T1 VALUES (1, 'a')")
	for _, q := range []string{"SELECT COUNT(*) FROM T1", "SELECT COUNT(*) FROM SCH1.T1", "SELECT COUNT(*) FROM USE_DB1.SCH1.T1"} {
		if got := connScalar(t, c, q); got != "1" {
			t.Errorf("%s = %s, want 1", q, got)
		}
	}

	// New databases have a PUBLIC schema.
	connExec(t, c, "CREATE OR REPLACE TABLE USE_DB1.PUBLIC.T3 (ID INT)")

	// USE of a missing object fails and leaves the session as it was.
	for _, q := range []string{"USE SCHEMA NO_SUCH_DB.NO_SUCH_SCHEMA", "USE DATABASE NO_SUCH_DB"} {
		_, err := c.ExecContext(context.Background(), q)
		if e := snowflakeErr(t, err); e.SQLState != "02000" {
			t.Errorf("%s: sqlState = %s, want 02000", q, e.SQLState)
		}
	}
	if got := connScalar(t, c, "SELECT CURRENT_SCHEMA()"); got != "SCH1" {
		t.Errorf("after failed USE, CURRENT_SCHEMA() = %s", got)
	}

	// Another session does not see this session's USE.
	other := oneSession(t)
	if got := connScalar(t, other, "SELECT COUNT(*) FROM USE_DB1.SCH1.T1"); got != "1" {
		t.Errorf("fully qualified from another session = %s", got)
	}
	if _, err := other.ExecContext(context.Background(), "SELECT * FROM SCH1.T1"); err == nil {
		t.Error("another session resolved SCH1.T1 without USE")
	}
}

// Connecting with database/schema set behaves like USE.
func TestLoginDatabaseSchemaScopeNames(t *testing.T) {
	setup := oneSession(t)
	connExec(t, setup, "CREATE DATABASE IF NOT EXISTS LOGIN_DB")
	connExec(t, setup, "CREATE SCHEMA IF NOT EXISTS LOGIN_DB.LOGIN_SCH")
	connExec(t, setup, "CREATE OR REPLACE TABLE LOGIN_DB.LOGIN_SCH.LT (ID INT)")
	connExec(t, setup, "INSERT INTO LOGIN_DB.LOGIN_SCH.LT VALUES (5)")

	cfg := &sf.Config{
		Account: "miniflake", User: "test", Password: "test",
		Host: "127.0.0.1", Port: testPort, Protocol: "http",
		Database: "LOGIN_DB", Schema: "LOGIN_SCH",
	}
	dsn, err := sf.DSN(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var dbName, schema string
	var id int
	if err := db.QueryRow("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), ID FROM LT").Scan(&dbName, &schema, &id); err != nil {
		t.Fatal(err)
	}
	if dbName != "LOGIN_DB" || schema != "LOGIN_SCH" || id != 5 {
		t.Errorf("got %s.%s id=%d, want LOGIN_DB.LOGIN_SCH id=5", dbName, schema, id)
	}
}

// #34: common date/time SQL.
func TestDateTimeFunctions(t *testing.T) {
	c := oneSession(t)
	for _, q := range []string{
		"SELECT CURRENT_TIMESTAMP() IS NOT NULL",
		"SELECT DATEADD(day, -1, CURRENT_TIMESTAMP()) < CURRENT_TIMESTAMP",
		"SELECT SYSDATE() IS NOT NULL AND LOCALTIMESTAMP() IS NOT NULL AND CURRENT_TIME() IS NOT NULL",
		"SELECT '2026-01-02'::TIMESTAMP_LTZ IS NOT NULL",
	} {
		var ok bool
		if err := c.QueryRowContext(context.Background(), q).Scan(&ok); err != nil || !ok {
			t.Errorf("%s = %v, %v; want true", q, ok, err)
		}
	}
	for q, want := range map[string]string{
		"SELECT TO_VARCHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', '2026-01-01 00:00:00'::TIMESTAMP), 'YYYY-MM-DD HH24:MI')":    "2025-12-31 19:00",
		"SELECT TO_VARCHAR(CONVERT_TIMEZONE('America/New_York', '2026-01-01 00:00:00 +00:00'::TIMESTAMP_TZ), 'YYYY-MM-DD HH24:MI')": "2025-12-31 19:00",
		"SELECT TO_VARCHAR('2026-01-02'::DATE, 'DD/MM/YYYY')":                                                                       "02/01/2026",
		"SELECT TO_VARCHAR(CAST('2026-01-02 13:04:05' AS TIMESTAMP_NTZ), 'Mon DD, YYYY HH12:MI AM')":                                "Jan 02, 2026 01:04 PM",
		"SELECT TO_CHAR(TO_TIMESTAMP_NTZ('2026-01-02 03:04:05.123456'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3')":                           "2026-01-02T03:04:05.123",
		"SELECT TO_VARCHAR(TO_DATE('02/01/2026', 'DD/MM/YYYY'), 'YYYY-MM-DD')":                                                      "2026-01-02",
	} {
		if got := connScalar(t, c, q); got != want {
			t.Errorf("%s = %q, want %q", q, got, want)
		}
	}

	// TIME and DATE travel with their own wire types.
	rows, err := c.QueryContext(context.Background(), "SELECT '12:34:56'::TIME AS T, '2026-01-02'::DATE AS D")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	types, _ := rows.ColumnTypes()
	if types[0].DatabaseTypeName() != "TIME" || types[1].DatabaseTypeName() != "DATE" {
		t.Errorf("column types = %s, %s; want TIME, DATE", types[0].DatabaseTypeName(), types[1].DatabaseTypeName())
	}
	if !rows.Next() {
		t.Fatal("no row")
	}
	var tm, d time.Time
	if err := rows.Scan(&tm, &d); err != nil {
		t.Fatal(err)
	}
	if tm.Hour() != 12 || tm.Minute() != 34 || tm.Second() != 56 {
		t.Errorf("TIME = %v, want 12:34:56", tm)
	}
	if d.Format("2006-01-02") != "2026-01-02" {
		t.Errorf("DATE = %v", d)
	}
}

var reUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// #35: UUID query IDs, results fetched by ID, query status, and sqlState on
// every failure.
func TestQueryIDResultAndStatus(t *testing.T) {
	c := oneSession(t)
	ctx := context.Background()

	ids := make(chan string, 1)
	var n int
	if err := c.QueryRowContext(sf.WithQueryIDChan(ctx, ids), "SELECT 42 AS N").Scan(&n); err != nil {
		t.Fatal(err)
	}
	qid := <-ids
	if !reUUID.MatchString(qid) {
		t.Fatalf("query ID %q is not UUID-shaped", qid)
	}

	// GET /queries/{id}/result
	rows, err := c.QueryContext(sf.WithFetchResultByID(ctx, qid), "")
	if err != nil {
		t.Fatalf("fetch result by ID: %v", err)
	}
	if !rows.Next() {
		t.Fatal("fetched result has no row")
	}
	var got int
	if err := rows.Scan(&got); err != nil || got != 42 {
		t.Errorf("fetched result = %d, %v; want 42", got, err)
	}
	rows.Close()

	// GET /monitoring/queries/{id}
	err = c.Raw(func(x interface{}) error {
		status, err := x.(sf.SnowflakeConnection).GetQueryStatus(ctx, qid)
		if err != nil {
			return err
		}
		if status.SQLText != "SELECT 42 AS N" {
			return fmt.Errorf("status SQLText = %q", status.SQLText)
		}
		return nil
	})
	if err != nil {
		t.Errorf("GetQueryStatus: %v", err)
	}

	for q, want := range map[string]string{
		"SELECT * FROM NO_SUCH_TABLE_35": "42S02",
		"SELEC 1":                        "42000",
		"SELECT NO_SUCH_FN_35(1)":        "42601",
	} {
		_, err := c.ExecContext(ctx, q)
		if e := snowflakeErr(t, err); e.SQLState != want {
			t.Errorf("%s: sqlState = %q, want %q (%v)", q, e.SQLState, want, err)
		}
	}
}

// itSession is a session with IT_DB.PUBLIC as its current schema.
func itSession(t *testing.T) *sql.Conn {
	t.Helper()
	c := oneSession(t)
	connExec(t, c, "CREATE DATABASE IF NOT EXISTS IT_DB")
	connExec(t, c, "USE SCHEMA IT_DB.PUBLIC")
	return c
}

// #36: SQL UDFs are callable.
func TestSQLUDFs(t *testing.T) {
	c := itSession(t)
	connExec(t, c, "CREATE OR REPLACE FUNCTION IT_F_Y(A NUMBER) RETURNS NUMBER AS $$ A + 1 $$")
	connExec(t, c, "CREATE OR REPLACE FUNCTION IT_F_Z(A INT) RETURNS INT LANGUAGE SQL AS 'A * 2'")
	connExec(t, c, "CREATE OR REPLACE FUNCTION IT_GREET(N VARCHAR, P VARCHAR DEFAULT 'Hi') RETURNS VARCHAR AS $$ P || ' ' || N $$")
	connExec(t, c, "CREATE OR REPLACE TABLE IT_UDF_NUMS (N INT)")
	connExec(t, c, "INSERT INTO IT_UDF_NUMS VALUES (1), (2), (3), (4)")
	connExec(t, c, "CREATE OR REPLACE FUNCTION IT_EVENS() RETURNS TABLE (VAL NUMBER) AS 'SELECT N FROM IT_UDF_NUMS WHERE N % 2 = 0'")

	if got := connScalar(t, c, "SELECT IT_F_Y(1) + IT_F_Z(3)"); got != "8" {
		t.Errorf("IT_F_Y(1) + IT_F_Z(3) = %s, want 8", got)
	}
	if got := connScalar(t, c, "SELECT IT_GREET(?)", "Ann"); got != "Hi Ann" {
		t.Errorf("IT_GREET(?) = %s", got)
	}
	if got := connScalar(t, c, "SELECT SUM(VAL) FROM TABLE(IT_EVENS())"); got != "6" {
		t.Errorf("SUM over TABLE(IT_EVENS()) = %s, want 6", got)
	}
	show, err := c.QueryContext(context.Background(), "SHOW USER FUNCTIONS LIKE 'IT_F_%'")
	if err != nil {
		t.Fatal(err)
	}
	cols, _ := show.Columns()
	var names []string
	for show.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := show.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		names = append(names, vals[1].String+" "+vals[8].String)
	}
	show.Close()
	if fmt.Sprint(names) != "[IT_F_Y IT_F_Y(NUMBER) RETURN NUMBER IT_F_Z IT_F_Z(NUMBER) RETURN NUMBER]" {
		t.Errorf("SHOW USER FUNCTIONS = %v", names)
	}

	_, err = c.ExecContext(context.Background(),
		"CREATE OR REPLACE FUNCTION IT_F_PY(A INT) RETURNS INT LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='f' AS $$\ndef f(a):\n    return a\n$$")
	if e := snowflakeErr(t, err); e.SQLState != "0A000" {
		t.Errorf("Python UDF: sqlState = %s, want 0A000 (unsupported)", e.SQLState)
	}

	connExec(t, c, "DROP FUNCTION IT_F_Y(NUMBER)")
	if _, err := c.ExecContext(context.Background(), "SELECT IT_F_Y(1)"); err == nil {
		t.Error("IT_F_Y callable after DROP FUNCTION")
	}
}

// #37: LANGUAGE SQL stored procedures.
func TestStoredProcedures(t *testing.T) {
	c := itSession(t)
	connExec(t, c, "CREATE OR REPLACE TABLE IT_SP_ORDERS (ID INT, STATUS VARCHAR)")
	connExec(t, c, "INSERT INTO IT_SP_ORDERS VALUES (1, 'NEW'), (2, 'NEW'), (3, 'DONE')")
	connExec(t, c, `CREATE OR REPLACE PROCEDURE IT_CLOSE(ID_ARG NUMBER, TAG VARCHAR DEFAULT 'DONE')
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  remaining INTEGER;
BEGIN
  UPDATE IT_SP_ORDERS SET STATUS = :tag WHERE ID = :id_arg AND STATUS = 'NEW';
  IF (SQLNOTFOUND) THEN
    RETURN 'no change';
  END IF;
  SELECT COUNT(*) INTO :remaining FROM IT_SP_ORDERS WHERE STATUS = 'NEW';
  RETURN 'closed ' || id_arg || ', ' || remaining || ' left';
END;
$$`)

	for _, tc := range []struct {
		args []interface{}
		call string
		want string
	}{
		{[]interface{}{1}, "CALL IT_CLOSE(?)", "closed 1, 1 left"},
		{[]interface{}{1}, "CALL IT_CLOSE(?)", "no change"},
		{[]interface{}{2, "SHIPPED"}, "CALL IT_CLOSE(?, ?)", "closed 2, 0 left"},
	} {
		rows, err := c.QueryContext(context.Background(), tc.call, tc.args...)
		if err != nil {
			t.Fatalf("%s %v: %v", tc.call, tc.args, err)
		}
		cols, _ := rows.Columns()
		var got string
		if !rows.Next() {
			t.Fatalf("%s: no row", tc.call)
		}
		if err := rows.Scan(&got); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if len(cols) != 1 || cols[0] != "IT_CLOSE" || got != tc.want {
			t.Errorf("%s %v = %v %q, want [IT_CLOSE] %q", tc.call, tc.args, cols, got, tc.want)
		}
	}
	if got := connScalar(t, c, "SELECT STATUS FROM IT_SP_ORDERS WHERE ID = 2"); got != "SHIPPED" {
		t.Errorf("order 2 status = %s, want SHIPPED", got)
	}

	connExec(t, c, "DROP PROCEDURE IT_CLOSE(NUMBER, VARCHAR)")
	connExec(t, c, "DROP PROCEDURE IF EXISTS IT_CLOSE(NUMBER, VARCHAR)")
	if _, err := c.ExecContext(context.Background(), "CALL IT_CLOSE(1)"); err == nil {
		t.Error("CALL succeeded after DROP PROCEDURE")
	}

	_, err := c.ExecContext(context.Background(), "CREATE OR REPLACE PROCEDURE IT_JS(A FLOAT) RETURNS STRING LANGUAGE JAVASCRIPT AS $$ return 'ok'; $$")
	if e := snowflakeErr(t, err); e.SQLState != "0A000" {
		t.Errorf("JavaScript procedure: sqlState = %s, want 0A000", e.SQLState)
	}
}
