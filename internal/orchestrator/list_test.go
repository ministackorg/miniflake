package orchestrator

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func seedStageFile(t *testing.T, orch *Orchestrator, db, schema, stageName, rel string, content []byte) {
	t.Helper()
	meta, err := orch.stageMgr.GetStage(db, schema, stageName)
	if err != nil {
		t.Fatalf("get stage: %v", err)
	}
	path := filepath.Join(meta.LocalPath, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestListStageNamedAndQualified(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE STAGE list_named"); err != nil {
		t.Fatal(err)
	}
	content := []byte("hello\n")
	seedStageFile(t, orch, sess.Database, sess.Schema, "list_named", "data.csv", content)
	wantMD5 := hex.EncodeToString(md5Sum(content))

	for _, stmt := range []string{
		"LIST @list_named",
		"LS @list_named",
		"LIST @main.list_named",
		"LIST @miniflake.main.list_named",
	} {
		result, err := orch.ExecuteSQL(ctx, sess, stmt)
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		if result.StatementType != "LIST" {
			t.Fatalf("%s: statement type = %q", stmt, result.StatementType)
		}
		if got := strings.Join(result.Columns, ","); got != "name,size,md5,last_modified" {
			t.Fatalf("%s: columns = %q", stmt, got)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("%s: rows = %d", stmt, len(result.Rows))
		}
		if result.Rows[0][0] != "list_named/data.csv" {
			t.Errorf("%s: name = %v", stmt, result.Rows[0][0])
		}
		if result.Rows[0][2] != wantMD5 {
			t.Errorf("%s: md5 = %v, want %s", stmt, result.Rows[0][2], wantMD5)
		}
		if result.Rows[0][3] == "" {
			t.Errorf("%s: empty last_modified", stmt)
		}
	}
}

func TestListStageSubpathAndPattern(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE STAGE list_filter"); err != nil {
		t.Fatal(err)
	}
	seedStageFile(t, orch, sess.Database, sess.Schema, "list_filter", "keep/a.csv", []byte("a"))
	seedStageFile(t, orch, sess.Database, sess.Schema, "list_filter", "keep/b.txt", []byte("b"))
	seedStageFile(t, orch, sess.Database, sess.Schema, "list_filter", "other/c.csv", []byte("c"))

	result, err := orch.ExecuteSQL(ctx, sess, "LIST @list_filter/keep")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("subpath rows = %d, want 2", len(result.Rows))
	}

	result, err = orch.ExecuteSQL(ctx, sess, "LIST @list_filter PATTERN = '.*\\.csv$'")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("pattern rows = %d, want 2", len(result.Rows))
	}

	result, err = orch.ExecuteSQL(ctx, sess, "LIST @list_filter/keep PATTERN = '.*\\.csv$'")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "list_filter/keep/a.csv" {
		t.Fatalf("combined filter = %#v", result.Rows)
	}
}

func TestListStageUserAndTable(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	userMeta := orch.stageMgr.GetUserStage(sess.User)
	if err := os.WriteFile(filepath.Join(userMeta.LocalPath, "u.csv"), []byte("u"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := orch.ExecuteSQL(ctx, sess, "LS @~")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "~/u.csv" {
		t.Fatalf("user stage list = %#v", result.Rows)
	}

	tableMeta := orch.stageMgr.GetTableStage(sess.Database, sess.Schema, "orders")
	if err := os.WriteFile(filepath.Join(tableMeta.LocalPath, "t.csv"), []byte("t"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err = orch.ExecuteSQL(ctx, sess, "LIST @%orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "%orders/t.csv" {
		t.Fatalf("table stage list = %#v", result.Rows)
	}
}

// Clients that submit a whole script (DBeaver, snowsql) keep the trailing
// semicolon, so it must not end up inside the resolved stage name.
func TestListStageTrailingSemicolon(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE STAGE list_semi"); err != nil {
		t.Fatal(err)
	}
	seedStageFile(t, orch, sess.Database, sess.Schema, "list_semi", "a.csv", []byte("a"))

	for _, stmt := range []string{"LIST @list_semi;", "LIST @list_semi/;"} {
		result, err := orch.ExecuteSQL(ctx, sess, stmt)
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0] != "list_semi/a.csv" {
			t.Fatalf("%s: rows = %#v", stmt, result.Rows)
		}
	}
}

// A stage directory removed out from under the server lists as empty, the way
// Snowflake reports a stage with no files.
func TestListStageMissingBackingDir(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := orch.ExecuteSQL(ctx, sess, "CREATE STAGE list_gone"); err != nil {
		t.Fatal(err)
	}
	meta, err := orch.stageMgr.GetStage(sess.Database, sess.Schema, "list_gone")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(meta.LocalPath); err != nil {
		t.Fatal(err)
	}

	result, err := orch.ExecuteSQL(ctx, sess, "LIST @list_gone")
	if err != nil {
		t.Fatalf("LIST on missing dir: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %#v, want none", result.Rows)
	}
}

func TestListStageMissingNamed(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()

	_, err := orch.ExecuteSQL(context.Background(), sess, "LIST @no_such_stage")
	if err == nil {
		t.Fatal("expected error for missing stage")
	}
}

func TestSplitStageRef(t *testing.T) {
	ref, sub, err := splitStageRef("mystage/sub/dir")
	if err != nil || ref != "mystage" || sub != "sub/dir" {
		t.Fatalf("splitStageRef = %q %q %v", ref, sub, err)
	}
	ref, sub, err = splitStageRef("~")
	if err != nil || ref != "~" || sub != "" {
		t.Fatalf("split ~ = %q %q %v", ref, sub, err)
	}
	ref, sub, err = splitStageRef("%orders/path")
	if err != nil || ref != "%orders" || sub != "path" {
		t.Fatalf("split table = %q %q %v", ref, sub, err)
	}
}

func md5Sum(b []byte) []byte {
	sum := md5.Sum(b)
	return sum[:]
}
