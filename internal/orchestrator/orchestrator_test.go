package orchestrator

import (
	"context"
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
	result, err := orch.ExecuteSQL(ctx, sess, "USE DATABASE my_new_db")
	if err != nil {
		t.Fatalf("USE DATABASE: %v", err)
	}
	if result.StatementType != "USE" {
		t.Errorf("expected statement type USE, got %s", result.StatementType)
	}
	if sess.Database != "my_new_db" {
		t.Errorf("expected session database 'my_new_db', got %q", sess.Database)
	}
	if orch.engine.CurrentDatabase() != "my_new_db" {
		t.Errorf("expected engine database 'my_new_db', got %q", orch.engine.CurrentDatabase())
	}
}

func TestUseSchemaChangesSession(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	ctx := context.Background()
	_, err := orch.ExecuteSQL(ctx, sess, "USE SCHEMA analytics")
	if err != nil {
		t.Fatalf("USE SCHEMA: %v", err)
	}
	if sess.Schema != "analytics" {
		t.Errorf("expected session schema 'analytics', got %q", sess.Schema)
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
