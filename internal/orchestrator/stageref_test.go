package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/miniflakedb/miniflake/internal/session"
)

// COPY INTO from a bare @stage used to build the stage key from empty
// database and schema parts, producing "..MY_STAGE" and always failing with
// "stage does not exist". Every existing copyinto test passed a fully
// qualified path, so the most common form in the wild went uncovered.
func TestCopyIntoResolvesBareStageRef(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	mustExec(t, orch, sess, "CREATE TABLE copy_bare (id INTEGER, name VARCHAR)")
	mustExec(t, orch, sess, "CREATE STAGE copy_bare_stage")

	meta, err := orch.stageMgr.GetStage(sess.Database, sess.Schema, "copy_bare_stage")
	if err != nil {
		t.Fatalf("get stage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(meta.LocalPath, "d.csv"), []byte("1,alice\n2,bob\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := orch.ExecuteSQL(ctx, sess,
		"COPY INTO copy_bare FROM @copy_bare_stage FILE_FORMAT = (TYPE = 'CSV')")
	if err != nil {
		t.Fatalf("COPY INTO from bare @stage: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one file row, got %#v", result.Rows)
	}
	if status := result.Rows[0][1]; status != "LOADED" {
		t.Fatalf("status = %v, want LOADED (first_error=%v)", status, result.Rows[0][5])
	}

	assertRowCount(t, orch, sess, "copy_bare", 2)
}

// The qualified form has to keep working, and now resolves through the same
// helper as the bare form.
func TestCopyIntoResolvesQualifiedStageRef(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	mustExec(t, orch, sess, "CREATE TABLE copy_qual (id INTEGER, name VARCHAR)")
	mustExec(t, orch, sess, "CREATE STAGE copy_qual_stage")

	meta, err := orch.stageMgr.GetStage(sess.Database, sess.Schema, "copy_qual_stage")
	if err != nil {
		t.Fatalf("get stage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(meta.LocalPath, "d.csv"), []byte("7,carol\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stmt := "COPY INTO copy_qual FROM @" + sess.Database + "." + sess.Schema +
		".copy_qual_stage FILE_FORMAT = (TYPE = 'CSV')"
	if _, err := orch.ExecuteSQL(ctx, sess, stmt); err != nil {
		t.Fatalf("COPY INTO from qualified @stage: %v", err)
	}
	assertRowCount(t, orch, sess, "copy_qual", 1)
}

// The path after the stage name is a prefix in Snowflake, so a subdirectory
// reference must load only what lives under it.
func TestCopyIntoStageSubpathIsAPrefix(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	mustExec(t, orch, sess, "CREATE TABLE copy_sub (id INTEGER, name VARCHAR)")
	mustExec(t, orch, sess, "CREATE STAGE copy_sub_stage")

	meta, err := orch.stageMgr.GetStage(sess.Database, sess.Schema, "copy_sub_stage")
	if err != nil {
		t.Fatalf("get stage: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(meta.LocalPath, "wanted"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(meta.LocalPath, "wanted", "in.csv"), []byte("1,in\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(meta.LocalPath, "out.csv"), []byte("2,out\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := orch.ExecuteSQL(ctx, sess,
		"COPY INTO copy_sub FROM @copy_sub_stage/wanted FILE_FORMAT = (TYPE = 'CSV')"); err != nil {
		t.Fatalf("COPY INTO with subpath: %v", err)
	}
	assertRowCount(t, orch, sess, "copy_sub", 1)
}

// PUT and GET resolve through the shared helper too, so a qualified reference
// now reaches the same stage LIST would.
func TestPutGetResolveQualifiedStageRef(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	mustExec(t, orch, sess, "CREATE STAGE putget_stage")

	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "up.csv")
	if err := os.WriteFile(srcFile, []byte("payload\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	qualified := "@" + sess.Database + "." + sess.Schema + ".putget_stage"
	result, err := orch.ExecuteSQL(ctx, sess, "PUT file://"+srcFile+" "+qualified)
	if err != nil {
		t.Fatalf("PUT to qualified stage: %v", err)
	}
	if status := result.Rows[0][6]; status != "UPLOADED" {
		t.Fatalf("PUT status = %v, want UPLOADED (message=%v)", status, result.Rows[0][8])
	}

	downDir := t.TempDir()
	result, err = orch.ExecuteSQL(ctx, sess, "GET "+qualified+" file://"+downDir)
	if err != nil {
		t.Fatalf("GET from qualified stage: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][3] != "DOWNLOADED" {
		t.Fatalf("GET rows = %#v", result.Rows)
	}
	if _, err := os.Stat(filepath.Join(downDir, "up.csv")); err != nil {
		t.Fatalf("downloaded file missing: %v", err)
	}
}

// A stage reference must not be able to address files outside its own
// directory, however many "../" segments it carries.
func TestStagePathCannotEscapeStage(t *testing.T) {
	orch, sess, cleanup := testOrchestrator(t)
	defer cleanup()
	ctx := context.Background()

	mustExec(t, orch, sess, "CREATE STAGE escape_stage")

	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "payload.csv")
	if err := os.WriteFile(srcFile, []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := orch.ExecuteSQL(ctx, sess,
		"PUT file://"+srcFile+" @escape_stage/../../escaped")
	if err != nil {
		// A hard error is an acceptable rejection.
		return
	}
	if status := result.Rows[0][6]; status != "ERROR" {
		t.Fatalf("PUT outside the stage reported %v, want rejection", status)
	}
}

func mustExec(t *testing.T, orch *Orchestrator, sess *session.Session, sql string) {
	t.Helper()
	if _, err := orch.ExecuteSQL(context.Background(), sess, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func assertRowCount(t *testing.T, orch *Orchestrator, sess *session.Session, table string, want int64) {
	t.Helper()
	result, err := orch.ExecuteSQL(context.Background(), sess, "SELECT COUNT(*) FROM "+table)
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	got, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("unexpected count type %T", result.Rows[0][0])
	}
	if got != want {
		t.Fatalf("%s has %d rows, want %d", table, got, want)
	}
}
