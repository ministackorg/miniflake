package stage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateDropStage(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)

	if err := m.CreateStage("mydb", "public", "my_stage", StageInternal, ""); err != nil {
		t.Fatalf("create stage: %v", err)
	}

	// Verify directory was created.
	meta, err := m.GetStage("mydb", "public", "my_stage")
	if err != nil {
		t.Fatalf("get stage: %v", err)
	}
	if _, err := os.Stat(meta.LocalPath); err != nil {
		t.Fatalf("stage directory not created: %v", err)
	}

	// Duplicate.
	if err := m.CreateStage("mydb", "public", "MY_STAGE", StageInternal, ""); err == nil {
		t.Fatal("expected error on duplicate stage")
	}

	// Drop.
	if err := m.DropStage("mydb", "public", "my_stage"); err != nil {
		t.Fatalf("drop stage: %v", err)
	}
	if _, err := m.GetStage("mydb", "public", "my_stage"); err == nil {
		t.Fatal("expected error after drop")
	}

	// Drop non-existent.
	if err := m.DropStage("mydb", "public", "nope"); err == nil {
		t.Fatal("expected error dropping non-existent stage")
	}
}

func TestPutGetFile(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	_ = m.CreateStage("db", "sch", "s1", StageInternal, "")

	// Create a source file.
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "data.csv")
	if err := os.WriteFile(srcFile, []byte("id,name\n1,alice\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// PUT
	if err := m.PutFile("db", "sch", "s1", srcFile, "data.csv"); err != nil {
		t.Fatalf("put file: %v", err)
	}

	// LIST
	files, err := m.ListFiles("db", "sch", "s1", "")
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Name != "data.csv" {
		t.Fatalf("unexpected file name: %s", files[0].Name)
	}

	// GET
	destDir := t.TempDir()
	destFile := filepath.Join(destDir, "out.csv")
	if err := m.GetFile("db", "sch", "s1", "data.csv", destFile); err != nil {
		t.Fatalf("get file: %v", err)
	}
	content, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "id,name\n1,alice\n" {
		t.Fatalf("unexpected content: %s", content)
	}

	// REMOVE
	if err := m.RemoveFile("db", "sch", "s1", "data.csv"); err != nil {
		t.Fatalf("remove file: %v", err)
	}
	files, _ = m.ListFiles("db", "sch", "s1", "")
	if len(files) != 0 {
		t.Fatalf("expected 0 files after remove, got %d", len(files))
	}
}

func TestListFilesWithPattern(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	_ = m.CreateStage("db", "sch", "s2", StageInternal, "")

	meta, _ := m.GetStage("db", "sch", "s2")
	// Create files directly in the stage directory.
	for _, name := range []string{"a.csv", "b.csv", "c.json"} {
		_ = os.WriteFile(filepath.Join(meta.LocalPath, name), []byte("x"), 0o644)
	}

	csvFiles, err := m.ListFiles("db", "sch", "s2", "*.csv")
	if err != nil {
		t.Fatalf("list with pattern: %v", err)
	}
	if len(csvFiles) != 2 {
		t.Fatalf("expected 2 csv files, got %d", len(csvFiles))
	}

	allFiles, _ := m.ListFiles("db", "sch", "s2", "")
	if len(allFiles) != 3 {
		t.Fatalf("expected 3 total files, got %d", len(allFiles))
	}
}

func TestUserStage(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)

	s := m.GetUserStage("alice")
	if s.Type != StageUser {
		t.Fatalf("expected USER stage, got %s", s.Type)
	}
	if _, err := os.Stat(s.LocalPath); err != nil {
		t.Fatalf("user stage directory not created: %v", err)
	}

	// Calling again returns the same stage.
	s2 := m.GetUserStage("alice")
	if s.LocalPath != s2.LocalPath {
		t.Fatal("expected same stage on second call")
	}
}

func TestTableStage(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)

	s := m.GetTableStage("db", "public", "orders")
	if s.Type != StageTable {
		t.Fatalf("expected TABLE stage, got %s", s.Type)
	}
	if _, err := os.Stat(s.LocalPath); err != nil {
		t.Fatalf("table stage directory not created: %v", err)
	}
}
