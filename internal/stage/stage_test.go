package stage

import (
	"crypto/md5"
	"encoding/hex"
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

func TestListFilesPopulatesMD5AndModTime(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	_ = m.CreateStage("db", "sch", "s3", StageInternal, "")

	meta, _ := m.GetStage("db", "sch", "s3")
	content := []byte("hello miniflake\n")
	if err := os.WriteFile(filepath.Join(meta.LocalPath, "f.txt"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Default ListFiles must not hash: COPY INTO / GET share this path.
	plain, err := m.ListFiles("db", "sch", "s3", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) != 1 {
		t.Fatalf("expected 1 file, got %d", len(plain))
	}
	if plain[0].MD5 != "" {
		t.Errorf("default ListFiles MD5 = %q, want empty", plain[0].MD5)
	}
	if plain[0].ModTime.IsZero() {
		t.Error("ModTime should not be zero")
	}

	files, err := m.ListFilesWithOptions("db", "sch", "s3", ListOptions{Checksum: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	want := md5.Sum(content)
	wantHex := hex.EncodeToString(want[:])
	if files[0].MD5 != wantHex {
		t.Errorf("MD5 = %q, want %q", files[0].MD5, wantHex)
	}
}

func TestListMetaFilesPrefixAndRegex(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	_ = m.CreateStage("db", "sch", "s4", StageInternal, "")
	meta, _ := m.GetStage("db", "sch", "s4")

	write := func(rel, body string) {
		t.Helper()
		path := filepath.Join(meta.LocalPath, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("keep/a.csv", "a")
	write("keep/b.txt", "b")
	write("other/c.csv", "c")

	files, err := m.ListMetaFiles(meta, ListOptions{Prefix: "keep", Regex: `.*\.csv$`})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Name != "keep/a.csv" {
		t.Fatalf("got %#v, want keep/a.csv only", files)
	}
}

// Snowflake treats @stage/<path> as a literal string prefix on the file path,
// not a whole-component match: `data` matches `data.csv`, `database.csv` and
// `data/x.csv`, but not `other.csv`. A trailing slash narrows to the folder.
func TestListMetaFilesPrefixIsLiteral(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	_ = m.CreateStage("db", "sch", "sp", StageInternal, "")
	meta, _ := m.GetStage("db", "sch", "sp")

	write := func(rel string) {
		t.Helper()
		path := filepath.Join(meta.LocalPath, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("data.csv")
	write("database.csv")
	write("data/x.csv")
	write("other.csv")

	names := func(opts ListOptions) []string {
		t.Helper()
		files, err := m.ListMetaFiles(meta, opts)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, len(files))
		for i, f := range files {
			out[i] = f.Name
		}
		return out
	}

	// Literal prefix "data" catches the partial-name matches too.
	if got := len(names(ListOptions{Prefix: "data"})); got != 3 {
		t.Errorf("prefix 'data' matched %d files, want 3 (data.csv, database.csv, data/x.csv)", got)
	}
	// A trailing slash scopes to the folder only.
	folder := names(ListOptions{Prefix: "data/"})
	if len(folder) != 1 || folder[0] != "data/x.csv" {
		t.Errorf("prefix 'data/' = %#v, want [data/x.csv]", folder)
	}
}

// Snowflake's PATTERN must match the whole path, not just occur inside it.
func TestListMetaFilesRegexIsWholePathMatch(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	_ = m.CreateStage("db", "sch", "s5", StageInternal, "")
	meta, _ := m.GetStage("db", "sch", "s5")

	path := filepath.Join(meta.LocalPath, "keep")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.csv"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Matches a substring of "keep/a.csv" but not the whole path.
	files, err := m.ListMetaFiles(meta, ListOptions{Regex: `a[.]csv`})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("partial pattern matched %#v, want no rows", files)
	}

	files, err = m.ListMetaFiles(meta, ListOptions{Regex: `.*a[.]csv`})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Errorf("full-path pattern matched %#v, want keep/a.csv", files)
	}
}

func TestListMetaFilesInvalidRegex(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	_ = m.CreateStage("db", "sch", "s6", StageInternal, "")
	meta, _ := m.GetStage("db", "sch", "s6")

	if _, err := m.ListMetaFiles(meta, ListOptions{Regex: `[unclosed`}); err == nil {
		t.Fatal("expected an error for an invalid PATTERN")
	}
}

// A stage directory that never existed, or was removed behind the server's
// back, lists as empty rather than surfacing a filesystem error.
func TestListMetaFilesMissingDirectory(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	_ = m.CreateStage("db", "sch", "s7", StageInternal, "")
	meta, _ := m.GetStage("db", "sch", "s7")

	if err := os.RemoveAll(meta.LocalPath); err != nil {
		t.Fatal(err)
	}

	files, err := m.ListMetaFiles(meta, ListOptions{Checksum: true})
	if err != nil {
		t.Fatalf("missing stage dir: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("got %#v, want no rows", files)
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
