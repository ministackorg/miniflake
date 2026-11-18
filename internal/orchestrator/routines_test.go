package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestSQLUDF_CreateCallDrop(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	run := func(sql string) *QueryResult {
		t.Helper()
		res, err := orch.ExecuteSQL(ctx, sess, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return res
	}
	scalar := func(sql string) string {
		t.Helper()
		res := run(sql)
		if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
			t.Fatalf("%s: rows %v", sql, res.Rows)
		}
		return fmt.Sprint(res.Rows[0][0])
	}

	// The #36 reproduction: both body quoting styles, with and without LANGUAGE SQL.
	run("CREATE OR REPLACE FUNCTION F_Y(A NUMBER) RETURNS NUMBER AS $$ A + 1 $$")
	run("CREATE FUNCTION F_Z(A INT) RETURNS INT LANGUAGE SQL AS 'A * 2'")
	if got := scalar("SELECT F_Y(1)"); got != "2" {
		t.Errorf("F_Y(1) = %s, want 2", got)
	}
	if got := scalar("SELECT F_Z(3)"); got != "6" {
		t.Errorf("F_Z(3) = %s, want 6", got)
	}
	// Snowflake functions inside the body are rewritten too.
	run("CREATE FUNCTION SIGN_WORD(X NUMBER) RETURNS VARCHAR AS $$ IFF(x >= 0, 'pos', 'neg') $$")
	if got := scalar("SELECT SIGN_WORD(-3)"); got != "neg" {
		t.Errorf("SIGN_WORD(-3) = %s, want neg", got)
	}
	// DEFAULT arguments.
	run("CREATE FUNCTION GREET(N VARCHAR, P VARCHAR DEFAULT 'Hi') RETURNS VARCHAR AS $$ P || ' ' || N $$")
	if got := scalar("SELECT GREET('Ann') || '/' || GREET('Bo', 'Yo')"); got != "Hi Ann/Yo Bo" {
		t.Errorf("GREET = %s", got)
	}
	// Query body and table function.
	run("CREATE TABLE udf_nums (n INT)")
	run("INSERT INTO udf_nums VALUES (1), (2), (3), (4)")
	run("CREATE FUNCTION NUM_COUNT() RETURNS NUMBER AS 'SELECT COUNT(*) FROM udf_nums'")
	if got := scalar("SELECT NUM_COUNT()"); got != "4" {
		t.Errorf("NUM_COUNT() = %s, want 4", got)
	}
	run("CREATE FUNCTION EVENS(LIM INT) RETURNS TABLE (VAL NUMBER) AS 'SELECT n FROM udf_nums WHERE n % 2 = 0 AND n <= LIM ORDER BY n'")
	res := run("SELECT * FROM TABLE(EVENS(10))")
	if fmt.Sprint(res.Columns, res.Rows) != "[VAL] [[2] [4]]" {
		t.Errorf("TABLE(EVENS(10)) = %v %v", res.Columns, res.Rows)
	}

	// Existing function without OR REPLACE, and IF NOT EXISTS.
	_, err := orch.ExecuteSQL(ctx, sess, "CREATE FUNCTION F_Z(A INT) RETURNS INT AS 'A * 3'")
	var sfErr *SnowflakeError
	if !errors.As(err, &sfErr) || sfErr.Code != "002002" {
		t.Errorf("duplicate CREATE FUNCTION: err = %v, want 002002", err)
	}
	run("CREATE FUNCTION IF NOT EXISTS F_Z(A INT) RETURNS INT AS 'A * 3'")
	if got := scalar("SELECT F_Z(3)"); got != "6" {
		t.Errorf("IF NOT EXISTS replaced the function: F_Z(3) = %s", got)
	}

	// SHOW USER FUNCTIONS.
	show := run("SHOW USER FUNCTIONS LIKE 'F_%'")
	if len(show.Columns) != 20 || show.Columns[8] != "arguments" {
		t.Fatalf("SHOW USER FUNCTIONS columns = %v", show.Columns)
	}
	var sigs []string
	for _, r := range show.Rows {
		sigs = append(sigs, r[8].(string))
	}
	if strings.Join(sigs, "; ") != "F_Y(NUMBER) RETURN NUMBER; F_Z(NUMBER) RETURN NUMBER" {
		t.Errorf("SHOW USER FUNCTIONS arguments = %v", sigs)
	}

	// DROP needs the argument types.
	run("DROP FUNCTION F_Y(NUMBER)")
	if _, err := orch.ExecuteSQL(ctx, sess, "SELECT F_Y(1)"); err == nil {
		t.Error("F_Y still callable after DROP FUNCTION")
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "DROP FUNCTION F_Y(NUMBER)"); !errors.As(err, &sfErr) || sfErr.SQLState != "42S02" {
		t.Errorf("DROP of a missing function: err = %v, want 42S02", err)
	}
	run("DROP FUNCTION IF EXISTS F_Y(NUMBER)")
	run("DROP FUNCTION EVENS(INT)")
}

func TestSQLUDF_FollowsCurrentSchema(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE DATABASE udf_db",
		"CREATE SCHEMA udf_db.udf_sch",
		"CREATE FUNCTION PLUS_TEN(X INT) RETURNS INT AS 'X + 10'",
	} {
		if _, err := orch.ExecuteSQL(ctx, sess, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	res, err := orch.ExecuteSQL(ctx, sess, "SELECT udf_db.udf_sch.PLUS_TEN(1), PLUS_TEN(2)")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(res.Rows) != "[[11 12]]" {
		t.Errorf("rows = %v", res.Rows)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "USE DATABASE miniflake"); err != nil {
		t.Fatal(err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "SELECT PLUS_TEN(1)"); err == nil {
		t.Error("function resolved outside its schema")
	}
}

func TestRoutines_UnsupportedLanguagesFailAtCreate(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	for _, s := range []string{
		"CREATE OR REPLACE FUNCTION F_PY(A INT) RETURNS INT LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='f' AS $$\ndef f(a):\n    return a\n$$",
		"CREATE FUNCTION F_JS(A FLOAT) RETURNS FLOAT LANGUAGE JAVASCRIPT AS 'return A;'",
		"CREATE OR REPLACE PROCEDURE SP_JS(A FLOAT) RETURNS STRING LANGUAGE JAVASCRIPT AS $$ return 'ok'; $$",
		"CREATE PROCEDURE SP_LOOP() RETURNS INT AS $$ BEGIN FOR i IN 1 TO 3 DO NULL; END FOR; RETURN 1; END $$",
		"CREATE PROCEDURE SP_TBL() RETURNS TABLE (A INT) AS $$ BEGIN RETURN 1; END $$",
	} {
		_, err := orch.ExecuteSQL(context.Background(), sess, s)
		var sfErr *SnowflakeError
		if !errors.As(err, &sfErr) || sfErr.Code != "000002" || sfErr.SQLState != "0A000" {
			t.Errorf("%.50s...: err = %v, want Unsupported feature (000002 / 0A000)", s, err)
		}
	}
	if n := len(orch.udfReg.ListRoutines("FUNCTION", "", "")) + len(orch.udfReg.ListRoutines("PROCEDURE", "", "")); n != 0 {
		t.Errorf("%d unsupported routines were registered", n)
	}
}

func TestStoredProcedure_Issue37(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()
	run := func(sql string) *QueryResult {
		t.Helper()
		res, err := orch.ExecuteSQL(ctx, sess, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return res
	}

	run("CREATE OR REPLACE PROCEDURE SP_X(A NUMBER) RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN RETURN 'ok'; END; $$")
	for _, call := range []string{"CALL SP_X(1)", "CALL miniflake.main.SP_X(1)"} {
		res := run(call)
		if fmt.Sprint(res.Columns, res.Rows) != "[SP_X] [[ok]]" {
			t.Errorf("%s = %v %v, want [SP_X] [[ok]]", call, res.Columns, res.Rows)
		}
	}

	// The issue's use case: scalar args, INSERT ... SELECT / UPDATE, a
	// status string back.
	run("CREATE TABLE sp_orders (id INT, status VARCHAR, amount NUMBER(10,2))")
	run("CREATE TABLE sp_audit (id INT, note VARCHAR)")
	run("INSERT INTO sp_orders VALUES (1, 'NEW', 10.5), (2, 'NEW', 20), (3, 'DONE', 5)")
	run(`CREATE OR REPLACE PROCEDURE CLOSE_ORDERS(MIN_AMT NUMBER(10,2), TAG VARCHAR DEFAULT 'DONE')
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  n INTEGER DEFAULT 0;
  total NUMBER(10,2);
BEGIN
  -- close the qualifying orders; the ';' in this string must not split: 'a;b'
  UPDATE sp_orders SET status = :tag WHERE status = 'NEW' AND amount >= :min_amt;
  LET updated := SQLROWCOUNT;
  IF (updated = 0) THEN
    RETURN 'nothing to do';
  END IF;
  INSERT INTO sp_audit SELECT id, 'closed as ' || :tag FROM sp_orders WHERE status = :tag;
  SELECT COUNT(*), SUM(amount) INTO :n, :total FROM sp_orders WHERE status = :tag;
  IF (n > 2) THEN
    RETURN 'closed ' || updated || ', all done, total ' || total;
  ELSEIF (n = 2) THEN
    RETURN 'closed ' || updated || ', two done';
  ELSE
    RETURN 'closed ' || updated || ', audit ' || SQLROWCOUNT;
  END IF;
END;
$$`)
	for _, tc := range []struct{ call, want string }{
		{"CALL CLOSE_ORDERS(15)", "closed 1, two done"},
		{"CALL CLOSE_ORDERS(15)", "nothing to do"},
		{"CALL CLOSE_ORDERS(min_amt => 1, tag => 'X')", "closed 1, audit NULL"},
		{"CALL CLOSE_ORDERS(0, 'DONE')", "nothing to do"},
	} {
		res := run(tc.call)
		got := fmt.Sprint(res.Rows[0][0])
		if tc.want == "closed 1, audit NULL" {
			// SQLROWCOUNT is NULL after the non-DML SELECT INTO, so the
			// concatenation is NULL, as in Snowflake.
			if res.Rows[0][0] != nil {
				t.Errorf("%s = %q, want NULL", tc.call, got)
			}
			continue
		}
		if got != tc.want {
			t.Errorf("%s = %q, want %q", tc.call, got, tc.want)
		}
	}
	if res := run("SELECT COUNT(*) FROM sp_audit"); fmt.Sprint(res.Rows) != "[[3]]" {
		t.Errorf("audit rows = %v, want [[3]]", res.Rows)
	}

	// Typed variables keep their types through substitution.
	run(`CREATE PROCEDURE DATE_LABEL(TS TIMESTAMP_NTZ) RETURNS VARCHAR AS $$
DECLARE d DATE;
BEGIN
  d := CAST(ts AS DATE);
  BEGIN
    LET label := TO_VARCHAR(d, 'YYYY/MM/DD') || ' ' || DATEDIFF(day, d, ts);
    RETURN label;
  END;
END $$`)
	if res := run("CALL DATE_LABEL('2026-03-04 05:06:07')"); fmt.Sprint(res.Rows) != "[[2026/03/04 0]]" {
		t.Errorf("DATE_LABEL = %v", res.Rows)
	}

	// SHOW PROCEDURES.
	show := run("SHOW PROCEDURES LIKE 'CLOSE%'")
	if len(show.Columns) != 14 || len(show.Rows) != 1 {
		t.Fatalf("SHOW PROCEDURES = %v %v", show.Columns, show.Rows)
	}
	if sig := show.Rows[0][8]; sig != "CLOSE_ORDERS(NUMBER, DEFAULT VARCHAR) RETURN VARCHAR" {
		t.Errorf("arguments = %v", sig)
	}
	if show.Rows[0][6] != int64(1) || show.Rows[0][7] != int64(2) {
		t.Errorf("min/max args = %v/%v, want 1/2", show.Rows[0][6], show.Rows[0][7])
	}

	// DROP PROCEDURE [IF EXISTS], and CALL of a missing procedure.
	run("DROP PROCEDURE SP_X(NUMBER)")
	run("DROP PROCEDURE IF EXISTS SP_X(NUMBER)")
	var sfErr *SnowflakeError
	if _, err := orch.ExecuteSQL(ctx, sess, "CALL SP_X(1)"); !errors.As(err, &sfErr) || sfErr.Code != "002140" {
		t.Errorf("CALL of a dropped procedure: err = %v, want 002140", err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "CALL CLOSE_ORDERS()"); err == nil {
		t.Error("CALL with too few arguments succeeded")
	}
}

func TestStoredProcedure_RecursionIsBounded(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE PROCEDURE FOREVER() RETURNS INT AS $$ BEGIN CALL FOREVER(); RETURN 1; END $$"); err != nil {
		t.Fatal(err)
	}
	_, err := orch.ExecuteSQL(ctx, sess, "CALL FOREVER()")
	if err == nil || !strings.Contains(err.Error(), "call depth") {
		t.Errorf("err = %v, want a call depth error", err)
	}
}
