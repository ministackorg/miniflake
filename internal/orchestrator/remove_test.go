package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRemoveStageDeletesFiles(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	mustExec(t, orch, sess, "CREATE STAGE rm_stage")
	seedStageFile(t, orch, sess.Database, sess.Schema, "rm_stage", "a.csv", []byte("a"))
	seedStageFile(t, orch, sess.Database, sess.Schema, "rm_stage", "b.csv", []byte("b"))

	result, err := orch.ExecuteSQL(ctx, sess, "REMOVE @rm_stage")
	if err != nil {
		t.Fatalf("REMOVE: %v", err)
	}
	if result.StatementType != "REMOVE" {
		t.Errorf("statement type = %q", result.StatementType)
	}
	if got := strings.Join(result.Columns, ","); got != "name,result" {
		t.Errorf("columns = %q, want name,result", got)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2", result.Rows)
	}
	for _, row := range result.Rows {
		if row[1] != "removed" {
			t.Errorf("result for %v = %v, want removed", row[0], row[1])
		}
	}

	// The stage must now list as empty.
	listed, err := orch.ExecuteSQL(ctx, sess, "LIST @rm_stage")
	if err != nil {
		t.Fatalf("LIST after REMOVE: %v", err)
	}
	if len(listed.Rows) != 0 {
		t.Fatalf("files survived REMOVE: %#v", listed.Rows)
	}
}

// RM is Snowflake's documented alias, and subpath plus PATTERN must narrow
// the deletion exactly the way they narrow a listing.
func TestRemoveStageAliasSubpathAndPattern(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	mustExec(t, orch, sess, "CREATE STAGE rm_filter")
	seedStageFile(t, orch, sess.Database, sess.Schema, "rm_filter", "keep/a.csv", []byte("a"))
	seedStageFile(t, orch, sess.Database, sess.Schema, "rm_filter", "keep/b.txt", []byte("b"))
	seedStageFile(t, orch, sess.Database, sess.Schema, "rm_filter", "other/c.csv", []byte("c"))

	result, err := orch.ExecuteSQL(ctx, sess, `RM @rm_filter/keep PATTERN = '.*[.]csv'`)
	if err != nil {
		t.Fatalf("RM: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "rm_filter/keep/a.csv" {
		t.Fatalf("removed %#v, want only rm_filter/keep/a.csv", result.Rows)
	}

	listed, err := orch.ExecuteSQL(ctx, sess, "LIST @rm_filter")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Rows) != 2 {
		t.Fatalf("expected the two unmatched files to survive, got %#v", listed.Rows)
	}
}

func TestRemoveStageMissingStage(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	if _, err := orch.ExecuteSQL(context.Background(), sess, "REMOVE @no_such_stage"); err == nil {
		t.Fatal("expected an error for a missing stage")
	}
}

// An empty stage is not an error: Snowflake reports nothing removed.
func TestRemoveStageEmpty(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	mustExec(t, orch, sess, "CREATE STAGE rm_empty")
	result, err := orch.ExecuteSQL(context.Background(), sess, "REMOVE @rm_empty")
	if err != nil {
		t.Fatalf("REMOVE on empty stage: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %#v, want none", result.Rows)
	}
}

func TestRemoveUserStage(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	meta := orch.stageMgr.GetUserStage(sess.User)
	if err := os.WriteFile(filepath.Join(meta.LocalPath, "u.csv"), []byte("u"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := orch.ExecuteSQL(context.Background(), sess, "REMOVE @~")
	if err != nil {
		t.Fatalf("REMOVE @~: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "~/u.csv" {
		t.Fatalf("rows = %#v", result.Rows)
	}
}
