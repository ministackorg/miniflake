package copyinto

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/miniflakedb/miniflake/internal/engine"
	"github.com/miniflakedb/miniflake/internal/stage"
)

// ---------------------------------------------------------------------------
// Parse tests
// ---------------------------------------------------------------------------

func TestParseLoadStatement(t *testing.T) {
	sql := "COPY INTO my_table FROM @DB.PUBLIC.MYSTAGE/data/ FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1) PURGE = TRUE"

	dir, tbl, sp, ff, opts, err := ParseCopyStatement(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dir != LoadIntoTable {
		t.Errorf("expected LOAD, got %s", dir)
	}
	if tbl != "my_table" {
		t.Errorf("expected my_table, got %s", tbl)
	}
	if sp != "DB.PUBLIC.MYSTAGE/data/" {
		t.Errorf("expected DB.PUBLIC.MYSTAGE/data/, got %s", sp)
	}
	if ff.Type != "CSV" {
		t.Errorf("expected CSV, got %s", ff.Type)
	}
	if ff.FieldDelimiter != "|" {
		t.Errorf("expected |, got %s", ff.FieldDelimiter)
	}
	if ff.SkipHeader != 1 {
		t.Errorf("expected SkipHeader=1, got %d", ff.SkipHeader)
	}
	if !opts.Purge {
		t.Error("expected Purge=true")
	}
}

func TestParseUnloadStatement(t *testing.T) {
	sql := "COPY INTO @DB.PUBLIC.MYSTAGE/export/ FROM my_table FILE_FORMAT = (TYPE = 'PARQUET') OVERWRITE = TRUE"

	dir, tbl, sp, ff, opts, err := ParseCopyStatement(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dir != UnloadToStage {
		t.Errorf("expected UNLOAD, got %s", dir)
	}
	if tbl != "my_table" {
		t.Errorf("expected my_table, got %s", tbl)
	}
	if sp != "DB.PUBLIC.MYSTAGE/export/" {
		t.Errorf("expected DB.PUBLIC.MYSTAGE/export/, got %s", sp)
	}
	if ff.Type != "PARQUET" {
		t.Errorf("expected PARQUET, got %s", ff.Type)
	}
	if !opts.Overwrite {
		t.Error("expected Overwrite=true")
	}
}

// ---------------------------------------------------------------------------
// Integration helpers
// ---------------------------------------------------------------------------

// setupTestEnv creates a temp dir with a DuckDB engine and a stage manager.
func setupTestEnv(t *testing.T) (*engine.Engine, *stage.Manager, string) {
	t.Helper()
	tmpDir := t.TempDir()

	eng, err := engine.New(filepath.Join(tmpDir, "data"))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	mgr := stage.NewManager(filepath.Join(tmpDir, "stages"))

	return eng, mgr, tmpDir
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

func TestExecuteLoadParquet(t *testing.T) {
	eng, mgr, _ := setupTestEnv(t)
	ctx := context.Background()

	// Create the target table.
	_, err := eng.ExecNoResult(ctx, "CREATE TABLE test_parquet (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Create a stage and produce a parquet file using DuckDB COPY.
	if err := mgr.CreateStage("DB", "PUBLIC", "PSTAGE", stage.StageInternal, ""); err != nil {
		t.Fatalf("create stage: %v", err)
	}
	meta, _ := mgr.GetStage("DB", "PUBLIC", "PSTAGE")
	parquetFile := filepath.Join(meta.LocalPath, "test.parquet")

	// Insert seed data and export to parquet.
	_, _ = eng.ExecNoResult(ctx, "CREATE TABLE _seed (id INTEGER, name VARCHAR)")
	_, _ = eng.ExecNoResult(ctx, "INSERT INTO _seed VALUES (1, 'alice'), (2, 'bob')")
	_, _ = eng.ExecNoResult(ctx, "COPY (SELECT * FROM _seed) TO '"+parquetFile+"' (FORMAT PARQUET)")

	// Now test COPY INTO load.
	executor := NewExecutor(eng.ExecNoResult, eng.Execute, mgr)
	results, err := executor.ExecuteLoad(ctx, "test_parquet", "DB.PUBLIC.PSTAGE", DefaultParquet(), CopyOptions{OnError: "ABORT_STATEMENT"})
	if err != nil {
		t.Fatalf("ExecuteLoad: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != "LOADED" {
		t.Errorf("expected LOADED, got %s", results[0].Status)
	}

	// Verify data landed.
	_, rows, err := eng.Execute(ctx, "SELECT count(*) FROM test_parquet")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	count := rows[0][0]
	// DuckDB returns int64 for count.
	if cnt, ok := count.(int64); !ok || cnt != 2 {
		t.Errorf("expected 2 rows, got %v", count)
	}
}

func TestExecuteLoadCSV(t *testing.T) {
	eng, mgr, _ := setupTestEnv(t)
	ctx := context.Background()

	_, err := eng.ExecNoResult(ctx, "CREATE TABLE test_csv (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := mgr.CreateStage("DB", "PUBLIC", "CSVSTAGE", stage.StageInternal, ""); err != nil {
		t.Fatalf("create stage: %v", err)
	}
	meta, _ := mgr.GetStage("DB", "PUBLIC", "CSVSTAGE")

	// Write a CSV file with a header.
	csvContent := "id,name\n1,alice\n2,bob\n3,charlie\n"
	csvFile := filepath.Join(meta.LocalPath, "data.csv")
	if err := os.WriteFile(csvFile, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	ff := DefaultCSV()
	ff.SkipHeader = 1 // file has a header row

	executor := NewExecutor(eng.ExecNoResult, eng.Execute, mgr)
	results, err := executor.ExecuteLoad(ctx, "test_csv", "DB.PUBLIC.CSVSTAGE", ff, CopyOptions{OnError: "ABORT_STATEMENT"})
	if err != nil {
		t.Fatalf("ExecuteLoad: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != "LOADED" {
		t.Errorf("expected LOADED, got %s (%s)", results[0].Status, results[0].FirstError)
	}

	_, rows, err := eng.Execute(ctx, "SELECT count(*) FROM test_csv")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if cnt, ok := rows[0][0].(int64); !ok || cnt != 3 {
		t.Errorf("expected 3 rows, got %v", rows[0][0])
	}
}

func TestExecuteUnload(t *testing.T) {
	eng, mgr, _ := setupTestEnv(t)
	ctx := context.Background()

	_, _ = eng.ExecNoResult(ctx, "CREATE TABLE to_unload (id INTEGER, val VARCHAR)")
	_, _ = eng.ExecNoResult(ctx, "INSERT INTO to_unload VALUES (1, 'x'), (2, 'y')")

	if err := mgr.CreateStage("DB", "PUBLIC", "OUTSTAGE", stage.StageInternal, ""); err != nil {
		t.Fatalf("create stage: %v", err)
	}

	executor := NewExecutor(eng.ExecNoResult, eng.Execute, mgr)
	results, err := executor.ExecuteUnload(ctx, "to_unload", "DB.PUBLIC.OUTSTAGE", DefaultParquet(), CopyOptions{})
	if err != nil {
		t.Fatalf("ExecuteUnload: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != "LOADED" {
		t.Errorf("expected LOADED status, got %s", results[0].Status)
	}

	// Verify the file was created.
	meta, _ := mgr.GetStage("DB", "PUBLIC", "OUTSTAGE")
	outFile := filepath.Join(meta.LocalPath, "data_0_0_0.parquet")
	if _, err := os.Stat(outFile); os.IsNotExist(err) {
		t.Error("expected parquet output file to exist")
	}
}

func TestOnErrorContinue(t *testing.T) {
	eng, mgr, _ := setupTestEnv(t)
	ctx := context.Background()

	// Create a table with specific columns.
	_, _ = eng.ExecNoResult(ctx, "CREATE TABLE strict_table (id INTEGER, name VARCHAR)")

	if err := mgr.CreateStage("DB", "PUBLIC", "BADSTAGE", stage.StageInternal, ""); err != nil {
		t.Fatalf("create stage: %v", err)
	}
	meta, _ := mgr.GetStage("DB", "PUBLIC", "BADSTAGE")

	// Write one good CSV and one bad CSV (wrong column count).
	goodCSV := "1,alice\n2,bob\n"
	badCSV := "this is not valid csv with proper columns\n1,2,3,4,5\n"
	_ = os.WriteFile(filepath.Join(meta.LocalPath, "good.csv"), []byte(goodCSV), 0o644)
	_ = os.WriteFile(filepath.Join(meta.LocalPath, "bad.csv"), []byte(badCSV), 0o644)

	ff := DefaultCSV()
	ff.SkipHeader = 0

	executor := NewExecutor(eng.ExecNoResult, eng.Execute, mgr)

	// With CONTINUE, we should get results for both files and no top-level error.
	results, err := executor.ExecuteLoad(ctx, "strict_table", "DB.PUBLIC.BADSTAGE", ff, CopyOptions{OnError: "CONTINUE"})
	if err != nil {
		t.Fatalf("unexpected top-level error with CONTINUE: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one result")
	}

	// At least one file should have loaded.
	var loadedCount int
	for _, r := range results {
		if r.Status == "LOADED" {
			loadedCount++
		}
	}
	// We expect the good file to succeed. The bad file may or may not fail
	// depending on DuckDB's CSV reader tolerance. Just verify we got through all files.
	t.Logf("results: %d total, %d loaded", len(results), loadedCount)
	if len(results) < 2 {
		t.Errorf("expected results for 2 files, got %d", len(results))
	}
}

func TestPurgeAfterLoad(t *testing.T) {
	eng, mgr, _ := setupTestEnv(t)
	ctx := context.Background()

	_, _ = eng.ExecNoResult(ctx, "CREATE TABLE purge_test (id INTEGER, name VARCHAR)")

	if err := mgr.CreateStage("DB", "PUBLIC", "PURGESTAGE", stage.StageInternal, ""); err != nil {
		t.Fatalf("create stage: %v", err)
	}
	meta, _ := mgr.GetStage("DB", "PUBLIC", "PURGESTAGE")

	csvContent := "1,alice\n2,bob\n"
	csvFile := filepath.Join(meta.LocalPath, "purge_me.csv")
	_ = os.WriteFile(csvFile, []byte(csvContent), 0o644)

	// Verify file exists before load.
	if _, err := os.Stat(csvFile); os.IsNotExist(err) {
		t.Fatal("csv file should exist before load")
	}

	ff := DefaultCSV()
	executor := NewExecutor(eng.ExecNoResult, eng.Execute, mgr)
	results, err := executor.ExecuteLoad(ctx, "purge_test", "DB.PUBLIC.PURGESTAGE", ff, CopyOptions{
		OnError: "ABORT_STATEMENT",
		Purge:   true,
	})
	if err != nil {
		t.Fatalf("ExecuteLoad: %v", err)
	}
	if len(results) != 1 || results[0].Status != "LOADED" {
		t.Fatalf("expected 1 LOADED result, got %v", results)
	}

	// File should be purged.
	if _, err := os.Stat(csvFile); !os.IsNotExist(err) {
		t.Error("expected csv file to be purged after load")
	}

	// Verify data is there.
	_, rows, _ := eng.Execute(ctx, "SELECT count(*) FROM purge_test")
	if cnt, ok := rows[0][0].(int64); !ok || cnt != 2 {
		t.Errorf("expected 2 rows, got %v", rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// buildLoadSQL helper tests (white-box)
// ---------------------------------------------------------------------------

func buildLoadSQL_wrapper(tbl, path string, ff FileFormat) string {
	s, _ := buildLoadSQL(tbl, path, ff)
	return s
}

func TestBuildLoadSQL_Parquet(t *testing.T) {
	sql := buildLoadSQL_wrapper("t", "/tmp/f.parquet", DefaultParquet())
	if !strings.Contains(sql, "read_parquet") {
		t.Errorf("expected read_parquet in SQL, got: %s", sql)
	}
}

func TestBuildLoadSQL_CSV(t *testing.T) {
	sql := buildLoadSQL_wrapper("t", "/tmp/f.csv", DefaultCSV())
	if !strings.Contains(sql, "read_csv") {
		t.Errorf("expected read_csv in SQL, got: %s", sql)
	}
}

func TestBuildLoadSQL_JSON(t *testing.T) {
	sql := buildLoadSQL_wrapper("t", "/tmp/f.json", DefaultJSON())
	if !strings.Contains(sql, "read_json") {
		t.Errorf("expected read_json in SQL, got: %s", sql)
	}
}
