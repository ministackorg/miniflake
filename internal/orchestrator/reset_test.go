package orchestrator

import (
	"context"
	"strings"
	"testing"
)

func TestOrchestratorReset(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE TABLE reset_probe (id INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE SCHEMA probe_ci_sch"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE DATABASE probe_db"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := orch.stageMgr.CreateStage(sess.Database, sess.Schema, "s1", "INTERNAL", ""); err != nil {
		t.Fatalf("create stage: %v", err)
	}
	if err := orch.taskSched.CreateTask(sess.Database, sess.Schema, "t1", "SELECT 1", "5 MINUTE", "", "", ""); err != nil {
		t.Fatalf("create task: %v", err)
	}

	if err := orch.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	_, err := orch.ExecuteSQL(ctx, sess, "SELECT * FROM reset_probe")
	if err == nil {
		t.Fatal("expected reset_probe to be gone after Reset")
	}

	// reset must match a fresh boot: Catalog.Init only (SNOWFLAKE_SAMPLE_DATA),
	// not the testOrchestrator MINIFLAKE fixture.
	for _, db := range orch.catalog.ListDatabases() {
		if strings.EqualFold(db.Name, "MINIFLAKE") {
			t.Fatal("Reset must not re-seed MINIFLAKE; that is a test fixture, not boot state")
		}
		if strings.EqualFold(db.Name, "PROBE_DB") {
			t.Fatal("PROBE_DB must be gone from catalog after Reset")
		}
	}

	// Schemas and databases must be gone from DuckDB so a second CI pass works.
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE SCHEMA probe_ci_sch"); err != nil {
		t.Fatalf("CREATE SCHEMA after reset: %v", err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE DATABASE probe_db"); err != nil {
		t.Fatalf("CREATE DATABASE after reset: %v", err)
	}

	// Re-seed what the unit-test session needs (same as testOrchestrator).
	_ = orch.catalog.CreateDatabase("MINIFLAKE", "SYSADMIN")
	_ = orch.catalog.CreateSchema("MINIFLAKE", "MAIN", "SYSADMIN")

	if err := orch.stageMgr.CreateStage(sess.Database, sess.Schema, "s1", "INTERNAL", ""); err != nil {
		t.Fatalf("recreate stage after reset: %v", err)
	}
	if err := orch.taskSched.CreateTask(sess.Database, sess.Schema, "t1", "SELECT 1", "5 MINUTE", "", "", ""); err != nil {
		t.Fatalf("recreate task after reset: %v", err)
	}
	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE TABLE reset_probe (id INT)"); err != nil {
		t.Fatalf("recreate table: %v", err)
	}
}
