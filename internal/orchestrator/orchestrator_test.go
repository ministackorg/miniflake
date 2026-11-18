package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/miniflakedb/miniflake/internal/catalog"
	"github.com/miniflakedb/miniflake/internal/clone"
	"github.com/miniflakedb/miniflake/internal/engine"
	"github.com/miniflakedb/miniflake/internal/rbac"
	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/snowpipe"
	"github.com/miniflakedb/miniflake/internal/stage"
	"github.com/miniflakedb/miniflake/internal/stream"
	"github.com/miniflakedb/miniflake/internal/task"
	"github.com/miniflakedb/miniflake/internal/timetravel"
	"github.com/miniflakedb/miniflake/internal/udf"
)

// testOrchestrator creates a fully wired orchestrator backed by a temporary
// DuckDB database. The caller must call cleanup() when done.
func testOrchestrator(t *testing.T) (*Orchestrator, *session.Session, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "miniflake-orch-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	eng, err := engine.New(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("create engine: %v", err)
	}

	cat := catalog.New()
	// Create the default database and schema in the catalog so DDL routing works.
	_ = cat.CreateDatabase("MINIFLAKE", "SYSADMIN")
	// DuckDB uses "main" as its default schema, so register it in the catalog too.
	_ = cat.CreateSchema("MINIFLAKE", "MAIN", "SYSADMIN")

	stageMgr := stage.NewManager(tmpDir)
	streamEng := stream.NewEngine()

	cloneEng := clone.NewEngine(
		func(ctx context.Context, sql string) error {
			_, err := eng.ExecNoResult(ctx, sql)
			return err
		},
		func(ctx context.Context, sql string) ([]string, [][]interface{}, error) {
			return eng.Execute(ctx, sql)
		},
	)

	taskSched := task.NewScheduler(func(ctx context.Context, sql string) error {
		_, err := eng.ExecNoResult(ctx, sql)
		return err
	})
	taskSched.SetStreamHasDataFn(func(db, schema, streamName string) bool {
		return streamEng.HasData(db, schema, streamName)
	})

	ttEng := timetravel.NewEngine(tmpDir, 24*time.Hour)
	udfReg := udf.NewRegistry()
	rbacEng := rbac.NewEngine()
	rbacEng.Init()
	snowpipeEng := snowpipe.NewEngine(func(ctx context.Context, sql string) error {
		_, err := eng.ExecNoResult(ctx, sql)
		return err
	})

	orch := New(eng, cat, stageMgr, streamEng, taskSched, ttEng, cloneEng, udfReg, rbacEng, snowpipeEng)

	sess := &session.Session{
		ID:        "test-session",
		Token:     "test-token",
		Database:  "miniflake",
		Schema:    "main",
		Warehouse: "COMPUTE_WH",
		Role:      "SYSADMIN",
		User:      "TEST_USER",
	}

	cleanup := func() {
		eng.Close()
		os.RemoveAll(tmpDir)
	}

	return orch, sess, cleanup
}

func TestUseDatabaseChangesSession(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE DATABASE my_new_db"); err != nil {
		t.Fatalf("CREATE DATABASE: %v", err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "USE DATABASE miniflake"); err != nil {
		t.Fatalf("USE DATABASE miniflake: %v", err)
	}
	result, err := orch.ExecuteSQL(ctx, sess, "USE DATABASE MY_NEW_DB")
	if err != nil {
		t.Fatalf("USE DATABASE: %v", err)
	}
	if result.StatementType != "USE" {
		t.Errorf("expected statement type USE, got %s", result.StatementType)
	}
	// The stored name is returned, and USE DATABASE selects PUBLIC.
	if sess.Database != "my_new_db" || sess.Schema != "PUBLIC" {
		t.Errorf("session = %s.%s, want my_new_db.PUBLIC", sess.Database, sess.Schema)
	}
	got, err := orch.ExecuteSQL(ctx, sess, "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
	if err != nil {
		t.Fatalf("SELECT CURRENT_DATABASE(): %v", err)
	}
	if fmt.Sprint(got.Rows) != "[[my_new_db PUBLIC]]" {
		t.Errorf("CURRENT_DATABASE(), CURRENT_SCHEMA() = %v, want [[my_new_db PUBLIC]]", got.Rows)
	}
}

func TestUseSchemaChangesSession(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE SCHEMA analytics"); err != nil {
		t.Fatalf("CREATE SCHEMA: %v", err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "USE SCHEMA main"); err != nil {
		t.Fatalf("USE SCHEMA main: %v", err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "USE SCHEMA analytics"); err != nil {
		t.Fatalf("USE SCHEMA: %v", err)
	}
	if sess.Schema != "analytics" {
		t.Errorf("expected session schema 'analytics', got %q", sess.Schema)
	}
}

func TestUseMissingObjectFails(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	for _, stmt := range []string{
		"USE DATABASE no_such_db",
		"USE SCHEMA no_such_db.no_such_schema",
		"USE SCHEMA no_such_schema",
		"USE no_such_db",
	} {
		_, err := orch.ExecuteSQL(ctx, sess, stmt)
		var sfErr *SnowflakeError
		if !errors.As(err, &sfErr) || sfErr.SQLState != "02000" {
			t.Errorf("%s: err = %v, want Snowflake 02000 object-does-not-exist", stmt, err)
		}
	}
	if sess.Database != "miniflake" || sess.Schema != "main" {
		t.Errorf("failed USE changed the session to %s.%s", sess.Database, sess.Schema)
	}
}

// TestUseSchemaResolvesNames is the #33 scenario: after USE SCHEMA, an
// unqualified CREATE lands in that schema and T1, SCH1.T1 and DB1.SCH1.T1
// all name the same table.
func TestUseSchemaResolvesNames(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	for _, stmt := range []string{
		"CREATE DATABASE DB1",
		"CREATE SCHEMA DB1.SCH1",
		"USE DATABASE miniflake",
		"USE SCHEMA DB1.SCH1",
		"CREATE TABLE T1 (ID NUMBER, NAME VARCHAR)",
		"INSERT INTO T1 VALUES (1, 'a')",
		"CREATE TABLE DB1.PUBLIC.T3 (ID INT)",
	} {
		if _, err := orch.ExecuteSQL(ctx, sess, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for _, q := range []string{"SELECT * FROM T1", "SELECT * FROM SCH1.T1", "SELECT * FROM DB1.SCH1.T1"} {
		res, err := orch.ExecuteSQL(ctx, sess, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if len(res.Rows) != 1 {
			t.Errorf("%s: %d rows, want 1", q, len(res.Rows))
		}
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "SELECT * FROM miniflake.main.T1"); err == nil {
		t.Error("T1 was created in miniflake.main, want DB1.SCH1")
	}
	// Another session is unaffected by this one's USE.
	other := &session.Session{Database: "miniflake", Schema: "main"}
	res, err := orch.ExecuteSQL(ctx, other, "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(res.Rows) != "[[miniflake main]]" {
		t.Errorf("other session sees %v, want [[miniflake main]]", res.Rows)
	}
}

// CREATE DATABASE / CREATE SCHEMA make the new object current, as in
// Snowflake.
func TestCreateSetsCurrentContext(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE DATABASE ctx_db"); err != nil {
		t.Fatal(err)
	}
	if sess.Database != "ctx_db" || sess.Schema != "PUBLIC" {
		t.Errorf("after CREATE DATABASE: %s.%s, want ctx_db.PUBLIC", sess.Database, sess.Schema)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE SCHEMA ctx_sch"); err != nil {
		t.Fatal(err)
	}
	if sess.Database != "ctx_db" || sess.Schema != "ctx_sch" {
		t.Errorf("after CREATE SCHEMA: %s.%s, want ctx_db.ctx_sch", sess.Database, sess.Schema)
	}
}

func TestUseWarehouseChangesSession(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	_, err := orch.ExecuteSQL(ctx, sess, "USE WAREHOUSE big_wh")
	if err != nil {
		t.Fatalf("USE WAREHOUSE: %v", err)
	}
	if sess.Warehouse != "big_wh" {
		t.Errorf("expected session warehouse 'big_wh', got %q", sess.Warehouse)
	}
}

func TestUseRoleChangesSession(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	_, err := orch.ExecuteSQL(ctx, sess, "USE ROLE ACCOUNTADMIN")
	if err != nil {
		t.Fatalf("USE ROLE: %v", err)
	}
	if sess.Role != "ACCOUNTADMIN" {
		t.Errorf("expected session role 'ACCOUNTADMIN', got %q", sess.Role)
	}
}

func TestCreateTableUpdatesBothDuckDBAndCatalog(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table via orchestrator.
	result, err := orch.ExecuteSQL(ctx, sess, "CREATE TABLE test_users (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if result.StatementType != "DDL" {
		t.Errorf("expected statement type DDL, got %s", result.StatementType)
	}

	// Verify it exists in DuckDB by querying it.
	selResult, err := orch.ExecuteSQL(ctx, sess, "SELECT * FROM test_users")
	if err != nil {
		t.Fatalf("SELECT from new table: %v", err)
	}
	if len(selResult.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(selResult.Columns))
	}

	// Verify it exists in catalog.
	_, catErr := orch.catalog.GetTable("miniflake", "main", "test_users")
	if catErr != nil {
		t.Errorf("table not found in catalog: %v", catErr)
	}
}

func TestInsertRecordsStreamChanges(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()

	// Create table and stream.
	_, err := orch.ExecuteSQL(ctx, sess, "CREATE TABLE events (id INTEGER, data VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = orch.ExecuteSQL(ctx, sess, "CREATE STREAM events_stream ON TABLE events")
	if err != nil {
		t.Fatalf("CREATE STREAM: %v", err)
	}

	// Verify stream has no data initially.
	if orch.streamEng.HasData(sess.Database, sess.Schema, "events_stream") {
		t.Error("stream should have no data before INSERT")
	}

	// Insert a row.
	result, err := orch.ExecuteSQL(ctx, sess, "INSERT INTO events VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if result.StatementType != "DML" {
		t.Errorf("expected statement type DML, got %s", result.StatementType)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify stream now has data.
	if !orch.streamEng.HasData(sess.Database, sess.Schema, "events_stream") {
		t.Error("stream should have data after INSERT")
	}
}

func TestSelectGoesThoughRewriter_IFF(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()

	// IFF(true, 'yes', 'no') should be rewritten to CASE WHEN and execute correctly.
	result, err := orch.ExecuteSQL(ctx, sess, "SELECT IFF(1=1, 'yes', 'no') AS result")
	if err != nil {
		t.Fatalf("SELECT IFF: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	val := result.Rows[0][0]
	if val != "yes" {
		t.Errorf("expected 'yes', got %v", val)
	}
}

func TestSelectGoesThoughRewriter_NVL(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()

	// NVL(NULL, 'fallback') should be rewritten to COALESCE and work.
	result, err := orch.ExecuteSQL(ctx, sess, "SELECT NVL(NULL, 'fallback') AS result")
	if err != nil {
		t.Fatalf("SELECT NVL: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	val := result.Rows[0][0]
	if val != "fallback" {
		t.Errorf("expected 'fallback', got %v", val)
	}
}

func TestDropTableRemovesFromCatalog(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()

	_, err := orch.ExecuteSQL(ctx, sess, "CREATE TABLE drop_me (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = orch.ExecuteSQL(ctx, sess, "DROP TABLE drop_me")
	if err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	// Verify it's gone from catalog.
	_, catErr := orch.catalog.GetTable("miniflake", "main", "drop_me")
	if catErr == nil {
		t.Error("table should not exist in catalog after DROP")
	}
}

func TestCreateAndDropStream(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()

	_, err := orch.ExecuteSQL(ctx, sess, "CREATE TABLE src (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = orch.ExecuteSQL(ctx, sess, "CREATE STREAM my_stream ON TABLE src")
	if err != nil {
		t.Fatalf("CREATE STREAM: %v", err)
	}

	// Verify stream exists.
	_, getErr := orch.streamEng.GetStream(sess.Database, sess.Schema, "my_stream")
	if getErr != nil {
		t.Errorf("stream should exist: %v", getErr)
	}

	_, err = orch.ExecuteSQL(ctx, sess, "DROP STREAM my_stream")
	if err != nil {
		t.Fatalf("DROP STREAM: %v", err)
	}

	_, getErr = orch.streamEng.GetStream(sess.Database, sess.Schema, "my_stream")
	if getErr == nil {
		t.Error("stream should not exist after DROP")
	}
}

func TestEmptySQL(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	result, err := orch.ExecuteSQL(ctx, sess, "")
	if err != nil {
		t.Fatalf("empty SQL: %v", err)
	}
	if result.StatementType != "EMPTY" {
		t.Errorf("expected EMPTY, got %s", result.StatementType)
	}
}

func TestShowDatabasesRewrite(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	// SHOW DATABASES gets rewritten to an information_schema query.
	result, err := orch.ExecuteSQL(ctx, sess, "SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES: %v", err)
	}
	if result.StatementType != "SELECT" {
		t.Errorf("expected SELECT (rewritten SHOW), got %s", result.StatementType)
	}
}
