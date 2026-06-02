package timetravel

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mockQuerier implements the Querier interface for testing.
// Instead of running real DuckDB queries, it creates an empty Parquet placeholder file.
type mockQuerier struct{}

func (m *mockQuerier) Execute(ctx context.Context, query string, args ...interface{}) ([]string, [][]interface{}, error) {
	// Extract file path from the COPY command.
	// Format: COPY "db"."schema"."table" TO '/path/to/file.parquet' (FORMAT PARQUET)
	// We just need to create the file so the snapshot has something to reference.
	start := indexOf(query, "'")
	end := indexOf(query[start+1:], "'") + start + 1
	if start >= 0 && end > start {
		filePath := query[start+1 : end]
		os.MkdirAll(filepath.Dir(filePath), 0o755)
		os.WriteFile(filePath, []byte("mock-parquet"), 0o644)
	}
	return nil, nil, nil
}

func indexOf(s string, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestCaptureSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	e := NewEngine(tmpDir, 24*time.Hour)
	q := &mockQuerier{}

	err := e.CaptureSnapshot("testdb", "public", "users", q)
	if err != nil {
		t.Fatalf("CaptureSnapshot: %v", err)
	}

	snaps := e.ListSnapshots("testdb", "public", "users")
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}

	// File should exist.
	if _, err := os.Stat(snaps[0].DataFile); os.IsNotExist(err) {
		t.Fatalf("snapshot file does not exist: %s", snaps[0].DataFile)
	}

	// Capture a second snapshot.
	err = e.CaptureSnapshot("testdb", "public", "users", q)
	if err != nil {
		t.Fatalf("CaptureSnapshot (2nd): %v", err)
	}
	snaps = e.ListSnapshots("testdb", "public", "users")
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}
}

func TestQueryAtTimestamp(t *testing.T) {
	tmpDir := t.TempDir()
	e := NewEngine(tmpDir, 24*time.Hour)

	// Manually insert snapshots with controlled timestamps.
	t1 := time.Now().Add(-10 * time.Minute)
	t2 := time.Now().Add(-5 * time.Minute)
	t3 := time.Now().Add(-1 * time.Minute)

	fqn := fqTableName("db", "public", "orders")
	file1 := filepath.Join(tmpDir, "snap1.parquet")
	file2 := filepath.Join(tmpDir, "snap2.parquet")
	file3 := filepath.Join(tmpDir, "snap3.parquet")
	os.WriteFile(file1, []byte("data1"), 0o644)
	os.WriteFile(file2, []byte("data2"), 0o644)
	os.WriteFile(file3, []byte("data3"), 0o644)

	e.mu.Lock()
	e.snapshots[fqn] = []Snapshot{
		{ID: 1, TableName: fqn, Timestamp: t1, DataFile: file1},
		{ID: 2, TableName: fqn, Timestamp: t2, DataFile: file2},
		{ID: 3, TableName: fqn, Timestamp: t3, DataFile: file3},
	}
	e.mu.Unlock()

	// Query at a time between t1 and t2 should return t1's snapshot.
	queryTime := t1.Add(2 * time.Minute) // between t1 and t2
	result, err := e.QueryAtTimestamp("db", "public", "orders", queryTime)
	if err != nil {
		t.Fatalf("QueryAtTimestamp: %v", err)
	}
	if result != file1 {
		t.Fatalf("expected %s, got %s", file1, result)
	}

	// Query at t2 exactly should return t2's snapshot.
	result, err = e.QueryAtTimestamp("db", "public", "orders", t2)
	if err != nil {
		t.Fatalf("QueryAtTimestamp: %v", err)
	}
	if result != file2 {
		t.Fatalf("expected %s, got %s", file2, result)
	}

	// Query at a future time should return the latest (t3).
	result, err = e.QueryAtTimestamp("db", "public", "orders", time.Now())
	if err != nil {
		t.Fatalf("QueryAtTimestamp: %v", err)
	}
	if result != file3 {
		t.Fatalf("expected %s, got %s", file3, result)
	}

	// Query before any snapshot should error.
	_, err = e.QueryAtTimestamp("db", "public", "orders", t1.Add(-1*time.Hour))
	if err == nil {
		t.Fatal("expected error for query before any snapshot")
	}

	// Query for non-existent table should error.
	_, err = e.QueryAtTimestamp("db", "public", "nonexistent", time.Now())
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

func TestUndrop(t *testing.T) {
	tmpDir := t.TempDir()
	e := NewEngine(tmpDir, 24*time.Hour)

	fqn := fqTableName("db", "public", "dropped_table")
	file := filepath.Join(tmpDir, "latest.parquet")
	os.WriteFile(file, []byte("data"), 0o644)

	e.mu.Lock()
	e.snapshots[fqn] = []Snapshot{
		{ID: 1, TableName: fqn, Timestamp: time.Now(), DataFile: file},
	}
	e.mu.Unlock()

	result, err := e.Undrop("db", "public", "dropped_table")
	if err != nil {
		t.Fatalf("Undrop: %v", err)
	}
	if result != file {
		t.Fatalf("expected %s, got %s", file, result)
	}

	// Undrop on table with no snapshots should fail.
	_, err = e.Undrop("db", "public", "no_snapshots")
	if err == nil {
		t.Fatal("expected error for table with no snapshots")
	}
}

func TestCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	e := NewEngine(tmpDir, 1*time.Minute)

	fqn := fqTableName("db", "public", "old_table")
	oldFile := filepath.Join(tmpDir, "old.parquet")
	newFile := filepath.Join(tmpDir, "new.parquet")
	os.WriteFile(oldFile, []byte("old"), 0o644)
	os.WriteFile(newFile, []byte("new"), 0o644)

	e.mu.Lock()
	e.snapshots[fqn] = []Snapshot{
		{ID: 1, TableName: fqn, Timestamp: time.Now().Add(-2 * time.Minute), DataFile: oldFile},
		{ID: 2, TableName: fqn, Timestamp: time.Now(), DataFile: newFile},
	}
	e.mu.Unlock()

	e.Cleanup()

	snaps := e.ListSnapshots("db", "public", "old_table")
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot after cleanup, got %d", len(snaps))
	}
	if snaps[0].DataFile != newFile {
		t.Fatalf("expected new snapshot to survive cleanup")
	}

	// Old file should be deleted.
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatal("expected old parquet file to be deleted")
	}
}
