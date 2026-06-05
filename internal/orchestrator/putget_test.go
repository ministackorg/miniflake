package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/stage"
)

// Server-side PUT/GET coverage. The Snowflake protocol normally has the
// driver intercept these commands and exchange presigned-URL responses with
// the server — gosnowflake does so before our server is reached. We still
// implement the server-side handlers because the Python connector and JDBC
// driver dispatch PUT/GET through the SQL endpoint, and an external HTTP
// client targeting the wire format directly works too.

func newTestOrch(t *testing.T) (*Orchestrator, *stage.Manager, string) {
	t.Helper()
	stageDir := t.TempDir()
	stageMgr := stage.NewManager(stageDir)
	o := &Orchestrator{stageMgr: stageMgr}
	return o, stageMgr, stageDir
}

func mockSess() *session.Session {
	return &session.Session{Database: "miniflake", Schema: "main"}
}

func TestHandlePut_CopiesFileIntoStage(t *testing.T) {
	t.Parallel()
	o, stageMgr, _ := newTestOrch(t)

	if err := stageMgr.CreateStage("miniflake", "main", "put_stage", stage.StageInternal, ""); err != nil {
		t.Fatal(err)
	}

	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "data.csv")
	payload := []byte("a,b\n1,2\n")
	if err := os.WriteFile(srcFile, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	res, handled, err := o.handlePut(mockSess(), "PUT file://"+srcFile+" @put_stage")
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("handlePut did not mark handled")
	}
	if res.StatementType != "PUT" {
		t.Errorf("StatementType=%q", res.StatementType)
	}
	if len(res.Rows) != 1 || res.Rows[0][6] != "UPLOADED" {
		t.Errorf("rows=%v", res.Rows)
	}

	// Verify the file actually landed in the stage backing dir.
	meta, err := stageMgr.GetStage("miniflake", "main", "put_stage")
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(meta.LocalPath, "data.csv"))
	if err != nil {
		t.Fatalf("stage file: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("stage contents: %q", string(got))
	}
}

func TestHandleGet_DownloadsAllFiles(t *testing.T) {
	t.Parallel()
	o, stageMgr, _ := newTestOrch(t)

	if err := stageMgr.CreateStage("miniflake", "main", "get_stage", stage.StageInternal, ""); err != nil {
		t.Fatal(err)
	}

	// Seed the stage with two files.
	tmp := t.TempDir()
	for _, name := range []string{"a.csv", "b.csv"} {
		path := filepath.Join(tmp, name)
		if err := os.WriteFile(path, []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := stageMgr.PutFile("miniflake", "main", "get_stage", path, name); err != nil {
			t.Fatal(err)
		}
	}

	downloadDir := filepath.Join(t.TempDir(), "downloads")
	res, handled, err := o.handleGet(mockSess(), "GET @get_stage file://"+downloadDir+"/")
	if err != nil {
		t.Fatal(err)
	}
	if !handled || res.StatementType != "GET" {
		t.Fatalf("handled=%v st=%q", handled, res.StatementType)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows=%d", len(res.Rows))
	}
	for _, name := range []string{"a.csv", "b.csv"} {
		got, err := os.ReadFile(filepath.Join(downloadDir, name))
		if err != nil {
			t.Errorf("download %s: %v", name, err)
			continue
		}
		if string(got) != name {
			t.Errorf("download %s content: %q", name, string(got))
		}
	}
}

func TestHandlePut_InvalidSourceFile(t *testing.T) {
	t.Parallel()
	o, stageMgr, _ := newTestOrch(t)
	if err := stageMgr.CreateStage("miniflake", "main", "s", stage.StageInternal, ""); err != nil {
		t.Fatal(err)
	}
	_, _, err := o.handlePut(mockSess(), "PUT file:///does/not/exist @s")
	if err == nil {
		t.Error("expected error for missing source file")
	}
}
