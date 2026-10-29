package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func TestNewEngine(t *testing.T) {
	dir := tempDir(t)
	eng, err := New(dir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer eng.Close()

	dbFile := filepath.Join(dir, "miniflake.duckdb")
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		t.Fatalf("expected DB file at %s to exist", dbFile)
	}
}

func TestExecuteSelect(t *testing.T) {
	dir := tempDir(t)
	eng, err := New(dir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()
	cols, rows, err := eng.Execute(ctx, "SELECT 1 AS num")
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if len(cols) != 1 || cols[0] != "num" {
		t.Fatalf("expected columns [num], got %v", cols)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// DuckDB returns int32 for integer literals.
	val, ok := rows[0][0].(int32)
	if !ok {
		t.Fatalf("expected int32, got %T (%v)", rows[0][0], rows[0][0])
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestCreateAndQuery(t *testing.T) {
	dir := tempDir(t)
	eng, err := New(dir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()

	// CREATE TABLE
	_, err = eng.ExecNoResult(ctx, "CREATE TABLE test_tbl (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE error: %v", err)
	}

	// INSERT
	affected, err := eng.ExecNoResult(ctx, "INSERT INTO test_tbl VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT error: %v", err)
	}
	if affected != 2 {
		t.Fatalf("expected 2 rows affected, got %d", affected)
	}

	// SELECT
	cols, rows, err := eng.Execute(ctx, "SELECT id, name FROM test_tbl ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT error: %v", err)
	}

	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Verify values
	if rows[0][1] != "alice" {
		t.Fatalf("expected 'alice', got %v", rows[0][1])
	}
	if rows[1][1] != "bob" {
		t.Fatalf("expected 'bob', got %v", rows[1][1])
	}
}

func TestAttachDatabase(t *testing.T) {
	dir := tempDir(t)
	eng, err := New(dir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()

	// Attach a new database
	if err := eng.AttachDatabase("analytics"); err != nil {
		t.Fatalf("AttachDatabase() error: %v", err)
	}

	// Verify the attached database appears in the list
	dbs, err := eng.GetDatabases(ctx)
	if err != nil {
		t.Fatalf("GetDatabases() error: %v", err)
	}

	found := false
	for _, db := range dbs {
		if db == "analytics" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected 'analytics' in databases list, got %v", dbs)
	}

	// Create a table in the attached database and query it
	_, err = eng.ExecNoResult(ctx, "CREATE TABLE analytics.main.events (id INTEGER, event VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE in attached db error: %v", err)
	}

	_, err = eng.ExecNoResult(ctx, "INSERT INTO analytics.main.events VALUES (1, 'click')")
	if err != nil {
		t.Fatalf("INSERT into attached db error: %v", err)
	}

	cols, rows, err := eng.Execute(ctx, "SELECT event FROM analytics.main.events")
	if err != nil {
		t.Fatalf("SELECT from attached db error: %v", err)
	}
	if len(cols) != 1 || len(rows) != 1 {
		t.Fatalf("unexpected result: cols=%v rows=%v", cols, rows)
	}
	if rows[0][0] != "click" {
		t.Fatalf("expected 'click', got %v", rows[0][0])
	}

	// Verify GetTables works for the attached database
	tables, err := eng.GetTables(ctx, "analytics", "main")
	if err != nil {
		t.Fatalf("GetTables() error: %v", err)
	}
	if len(tables) != 1 || tables[0] != "events" {
		t.Fatalf("expected [events], got %v", tables)
	}
}

// Every pooled connection must carry the Snowflake session defaults, not just
// the first one: DuckDB SET is per-connection, the DSN config is global.
func TestSnowflakeSessionDefaults(t *testing.T) {
	eng, err := New(tempDir(t))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer eng.Close()
	ctx := context.Background()

	conns := make([]*sql.Conn, 3)
	for i := range conns {
		c, err := eng.db.Conn(ctx)
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		defer c.Close()
		conns[i] = c
	}
	for i, c := range conns {
		var tz string
		if err := c.QueryRowContext(ctx, "SELECT current_setting('TimeZone')").Scan(&tz); err != nil {
			t.Fatalf("conn %d: timezone: %v", i, err)
		}
		if tz != "America/Los_Angeles" {
			t.Errorf("conn %d: TimeZone = %q, want America/Los_Angeles", i, tz)
		}
		var v sql.NullFloat64
		err := c.QueryRowContext(ctx, "SELECT 1 / 0").Scan(&v)
		if err == nil || !strings.Contains(err.Error(), "Division by zero") {
			t.Errorf("conn %d: SELECT 1 / 0 = %v, err %v; want Division by zero error", i, v, err)
		}
	}
}
