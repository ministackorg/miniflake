package orchestrator

import (
	"context"
	"testing"
)

func TestOrchestratorReset(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE TABLE reset_probe (id INT)"); err != nil {
		t.Fatalf("create table: %v", err)
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
