package engine

import (
	"context"
	"strings"
	"testing"
)

func TestResetDropsSchemasAndDetachesDatabases(t *testing.T) {
	eng, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	ctx := context.Background()

	if _, err := eng.ExecNoResult(ctx, `CREATE SCHEMA probe_ci_sch`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := eng.ExecNoResult(ctx, `CREATE TABLE probe_ci_sch.t(i INT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := eng.AttachDatabase("probe_db"); err != nil {
		t.Fatalf("attach: %v", err)
	}

	if err := eng.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	_, schemas, err := eng.Execute(ctx,
		`SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'miniflake' AND NOT internal`)
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas) != 0 {
		t.Fatalf("user schemas survived reset: %v", schemas)
	}

	_, dbs, err := eng.Execute(ctx,
		`SELECT database_name FROM duckdb_databases() WHERE NOT internal ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(dbs) != 1 || dbs[0][0] != "miniflake" {
		t.Fatalf("attached DBs after reset: %v (want only miniflake)", dbs)
	}

	if _, err := eng.ExecNoResult(ctx, `CREATE SCHEMA probe_ci_sch`); err != nil {
		t.Fatalf("CREATE SCHEMA after reset must succeed: %v", err)
	}
	if err := eng.AttachDatabase("probe_db"); err != nil {
		t.Fatalf("re-ATTACH after reset: %v", err)
	}
}

func TestResetDoesNotLeaveMainTables(t *testing.T) {
	eng, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	ctx := context.Background()

	if _, err := eng.ExecNoResult(ctx, `CREATE TABLE reset_probe(i INT)`); err != nil {
		t.Fatal(err)
	}
	if err := eng.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = eng.ExecNoResult(ctx, `SELECT * FROM reset_probe`)
	if err == nil {
		t.Fatal("expected reset_probe gone")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "reset_probe") &&
		!strings.Contains(strings.ToLower(err.Error()), "does not exist") &&
		!strings.Contains(strings.ToLower(err.Error()), "not find") &&
		!strings.Contains(strings.ToLower(err.Error()), "catalog") {
		t.Logf("error was: %v", err)
	}
}
