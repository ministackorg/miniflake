//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/miniflakedb/miniflake/internal/catalog"
	"github.com/miniflakedb/miniflake/internal/clone"
	"github.com/miniflakedb/miniflake/internal/engine"
	"github.com/miniflakedb/miniflake/internal/orchestrator"
	"github.com/miniflakedb/miniflake/internal/rbac"
	"github.com/miniflakedb/miniflake/internal/server"
	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/snowpipe"
	"github.com/miniflakedb/miniflake/internal/stage"
	"github.com/miniflakedb/miniflake/internal/stream"
	"github.com/miniflakedb/miniflake/internal/task"
	"github.com/miniflakedb/miniflake/internal/timetravel"
	"github.com/miniflakedb/miniflake/internal/udf"
	sf "github.com/snowflakedb/gosnowflake"
)

var (
	testPort int
	testSrv  *server.Server
	testEng  *engine.Engine
	sessMgr  *session.Manager
	dataDir  string
)

func TestMain(m *testing.M) {
	// Find a free port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to find free port: %v\n", err)
		os.Exit(1)
	}
	testPort = listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Create temporary directories.
	dataDir, err = os.MkdirTemp("", "miniflake-integration-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dataDir)

	stageDir, err := os.MkdirTemp("", "miniflake-stages-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create stage dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(stageDir)

	// Initialize engine.
	testEng, err = engine.New(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create engine: %v\n", err)
		os.Exit(1)
	}
	defer testEng.Close()

	sessMgr = session.NewManager()
	defer sessMgr.Stop()

	// Initialize all subsystems for the orchestrator.
	cat := catalog.New()
	cat.Init()
	stageMgr := stage.NewManager(stageDir)
	streamEng := stream.NewEngine()
	ttEng := timetravel.NewEngine(dataDir+"/snapshots", 24*time.Hour)
	udfReg := udf.NewRegistry()
	rbacEng := rbac.NewEngine()
	rbacEng.Init()
	snowpipeEng := snowpipe.NewEngine(func(ctx context.Context, sql string) error {
		_, err := testEng.ExecNoResult(ctx, sql)
		return err
	})

	cloneExec := func(ctx context.Context, sql string) error {
		_, err := testEng.ExecNoResult(ctx, sql)
		return err
	}
	cloneQuery := func(ctx context.Context, sql string) ([]string, [][]interface{}, error) {
		return testEng.Execute(ctx, sql)
	}
	cloneEng := clone.NewEngine(cloneExec, cloneQuery)

	taskExec := func(ctx context.Context, sql string) error {
		_, err := testEng.ExecNoResult(ctx, sql)
		return err
	}
	taskSched := task.NewScheduler(taskExec)

	orch := orchestrator.New(
		testEng, cat, stageMgr, streamEng, taskSched,
		ttEng, cloneEng, udfReg, rbacEng, snowpipeEng,
	)

	// Start the server WITH the orchestrator.
	testSrv = server.New(testEng, sessMgr, "127.0.0.1", testPort, stageDir, orch)
	go func() {
		if err := testSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		}
	}()

	// Wait for the health endpoint to respond.
	healthURL := fmt.Sprintf("http://127.0.0.1:%d/_miniflake/health", testPort)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	code := m.Run()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	testSrv.Shutdown(ctx)

	os.Exit(code)
}

// openDB returns a *sql.DB connected to the local MiniFlake via the gosnowflake driver.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	cfg := &sf.Config{
		Account:   "miniflake",
		User:      "test",
		Password:  "test",
		Host:      "127.0.0.1",
		Port:      testPort,
		Protocol:  "http",
		Database:  "TESTDB",
		Schema:    "PUBLIC",
		Warehouse: "COMPUTE_WH",
	}
	dsn, err := sf.DSN(cfg)
	if err != nil {
		t.Fatalf("failed to build DSN: %v", err)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("failed to open snowflake connection: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func execSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatalf("exec %q failed: %v", query, err)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestSetup(t *testing.T) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/_miniflake/health", testPort))
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from health, got %d", resp.StatusCode)
	}
}

func TestConnect(t *testing.T) {
	db := openDB(t)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestCreateDatabase(t *testing.T) {
	db := openDB(t)
	execSQL(t, db, "CREATE DATABASE IF NOT EXISTS testdb")
}

func TestCreateTable(t *testing.T) {
	db := openDB(t)
	execSQL(t, db, `CREATE TABLE IF NOT EXISTS test_table (
		id INTEGER,
		name VARCHAR,
		amount DOUBLE,
		active BOOLEAN,
		created_at TIMESTAMP
	)`)
}

func TestInsertAndSelect(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "DROP TABLE IF EXISTS insert_test")
	execSQL(t, db, "CREATE TABLE insert_test (id INTEGER, name VARCHAR, amount DOUBLE)")

	execSQL(t, db, "INSERT INTO insert_test VALUES (1, 'alice', 100.50)")
	execSQL(t, db, "INSERT INTO insert_test VALUES (2, 'bob', 200.75)")
	execSQL(t, db, "INSERT INTO insert_test VALUES (3, 'charlie', 300.00)")

	rows, err := db.QueryContext(context.Background(), "SELECT id, name, amount FROM insert_test ORDER BY id")
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	defer rows.Close()

	type row struct {
		ID     int
		Name   string
		Amount float64
	}

	var results []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ID, &r.Name, &r.Amount); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}
	if results[0].Name != "alice" || results[0].Amount != 100.50 {
		t.Errorf("row 0 mismatch: %+v", results[0])
	}
	if results[2].Name != "charlie" || results[2].Amount != 300.00 {
		t.Errorf("row 2 mismatch: %+v", results[2])
	}
}

func TestMultipleTypes(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "DROP TABLE IF EXISTS type_test")
	execSQL(t, db, `CREATE TABLE type_test (
		num_col INTEGER,
		str_col VARCHAR,
		bool_col BOOLEAN,
		float_col DOUBLE,
		variant_col VARCHAR
	)`)

	execSQL(t, db, `INSERT INTO type_test VALUES (
		42,
		'hello world',
		true,
		3.14159,
		'{"key": "value"}'
	)`)

	var (
		numCol     int
		strCol     string
		boolCol    bool
		floatCol   float64
		variantCol string
	)

	err := db.QueryRowContext(context.Background(),
		"SELECT num_col, str_col, bool_col, float_col, variant_col FROM type_test",
	).Scan(&numCol, &strCol, &boolCol, &floatCol, &variantCol)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if numCol != 42 {
		t.Errorf("num_col: expected 42, got %d", numCol)
	}
	if strCol != "hello world" {
		t.Errorf("str_col: expected 'hello world', got %q", strCol)
	}
	if !boolCol {
		t.Errorf("bool_col: expected true, got false")
	}
	if floatCol < 3.14 || floatCol > 3.15 {
		t.Errorf("float_col: expected ~3.14159, got %f", floatCol)
	}
	if variantCol != `{"key": "value"}` {
		t.Errorf("variant_col: expected JSON string, got %q", variantCol)
	}
}

func TestCTEAndWindowFunctions(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "DROP TABLE IF EXISTS window_test")
	execSQL(t, db, `CREATE TABLE window_test (
		department VARCHAR,
		employee VARCHAR,
		salary INTEGER
	)`)

	execSQL(t, db, "INSERT INTO window_test VALUES ('eng', 'alice', 120000)")
	execSQL(t, db, "INSERT INTO window_test VALUES ('eng', 'bob', 110000)")
	execSQL(t, db, "INSERT INTO window_test VALUES ('sales', 'charlie', 90000)")
	execSQL(t, db, "INSERT INTO window_test VALUES ('sales', 'diana', 95000)")

	query := `
		WITH ranked AS (
			SELECT
				department,
				employee,
				salary,
				ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
			FROM window_test
		)
		SELECT department, employee, salary, rn
		FROM ranked
		WHERE rn = 1
		ORDER BY department
	`

	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("CTE/window query failed: %v", err)
	}
	defer rows.Close()

	type result struct {
		Dept     string
		Employee string
		Salary   int
		RN       int
	}

	var results []result
	for rows.Next() {
		var r result
		if err := rows.Scan(&r.Dept, &r.Employee, &r.Salary, &r.RN); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
	if results[0].Employee != "alice" {
		t.Errorf("eng top earner: expected alice, got %s", results[0].Employee)
	}
	if results[1].Employee != "diana" {
		t.Errorf("sales top earner: expected diana, got %s", results[1].Employee)
	}
}

func TestShowCommands(t *testing.T) {
	db := openDB(t)

	t.Run("SHOW DATABASES", func(t *testing.T) {
		rows, err := db.QueryContext(context.Background(), "SHOW DATABASES")
		if err != nil {
			t.Fatalf("SHOW DATABASES failed: %v", err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("SHOW DATABASES returned no rows")
		}
	})

	t.Run("SHOW TABLES", func(t *testing.T) {
		rows, err := db.QueryContext(context.Background(), "SHOW TABLES")
		if err != nil {
			t.Fatalf("SHOW TABLES failed: %v", err)
		}
		defer rows.Close()
	})

	t.Run("SHOW SCHEMAS", func(t *testing.T) {
		rows, err := db.QueryContext(context.Background(), "SHOW SCHEMAS")
		if err != nil {
			t.Fatalf("SHOW SCHEMAS failed: %v", err)
		}
		defer rows.Close()
	})
}

func TestDescribe(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "DROP TABLE IF EXISTS desc_test")
	execSQL(t, db, "CREATE TABLE desc_test (id INTEGER, name VARCHAR, active BOOLEAN)")

	rows, err := db.QueryContext(context.Background(), "DESCRIBE TABLE desc_test")
	if err != nil {
		t.Fatalf("DESCRIBE TABLE failed: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}
	if len(cols) == 0 {
		t.Fatal("DESCRIBE returned no columns")
	}

	count := 0
	for rows.Next() {
		count++
	}
	if count < 3 {
		t.Errorf("expected at least 3 columns described, got %d", count)
	}
}

func TestCreateAndDropSchema(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "CREATE SCHEMA IF NOT EXISTS test_schema_lifecycle")
	execSQL(t, db, "CREATE TABLE test_schema_lifecycle.schema_probe (id INTEGER)")
	execSQL(t, db, "DROP TABLE test_schema_lifecycle.schema_probe")
	execSQL(t, db, "DROP SCHEMA IF EXISTS test_schema_lifecycle")
}

func TestInformationSchema(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "DROP TABLE IF EXISTS info_schema_test")
	execSQL(t, db, "CREATE TABLE info_schema_test (id INTEGER, val VARCHAR)")

	rows, err := db.QueryContext(context.Background(),
		"SELECT table_name FROM information_schema.tables WHERE table_name = 'info_schema_test'",
	)
	if err != nil {
		t.Fatalf("information_schema query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected to find info_schema_test in information_schema.tables")
	}
	var tableName string
	if err := rows.Scan(&tableName); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if tableName != "info_schema_test" {
		t.Errorf("expected table name 'info_schema_test', got %q", tableName)
	}
}

func TestNullHandling(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "DROP TABLE IF EXISTS null_test")
	execSQL(t, db, "CREATE TABLE null_test (id INTEGER, name VARCHAR, amount DOUBLE)")

	execSQL(t, db, "INSERT INTO null_test VALUES (1, NULL, NULL)")
	execSQL(t, db, "INSERT INTO null_test VALUES (2, 'present', 99.9)")

	rows, err := db.QueryContext(context.Background(), "SELECT id, name, amount FROM null_test ORDER BY id")
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected row 1")
	}
	var id int
	var name sql.NullString
	var amount sql.NullFloat64
	if err := rows.Scan(&id, &name, &amount); err != nil {
		t.Fatalf("scan row 1 failed: %v", err)
	}
	if id != 1 {
		t.Errorf("row 1 id: expected 1, got %d", id)
	}
	if name.Valid {
		t.Errorf("row 1 name: expected NULL, got %q", name.String)
	}
	if amount.Valid {
		t.Errorf("row 1 amount: expected NULL, got %f", amount.Float64)
	}

	if !rows.Next() {
		t.Fatal("expected row 2")
	}
	if err := rows.Scan(&id, &name, &amount); err != nil {
		t.Fatalf("scan row 2 failed: %v", err)
	}
	if !name.Valid || name.String != "present" {
		t.Errorf("row 2 name: expected 'present', got %v", name)
	}
	if !amount.Valid || amount.Float64 != 99.9 {
		t.Errorf("row 2 amount: expected 99.9, got %v", amount)
	}
}

func TestTransactions(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "DROP TABLE IF EXISTS tx_test")
	execSQL(t, db, "CREATE TABLE tx_test (id INTEGER, val VARCHAR)")

	// Test COMMIT.
	execSQL(t, db, "BEGIN")
	execSQL(t, db, "INSERT INTO tx_test VALUES (1, 'committed')")
	execSQL(t, db, "COMMIT")

	var count int
	err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM tx_test").Scan(&count)
	if err != nil {
		t.Fatalf("count after commit failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row after commit, got %d", count)
	}

	// Test ROLLBACK.
	execSQL(t, db, "BEGIN")
	execSQL(t, db, "INSERT INTO tx_test VALUES (2, 'rolled_back')")
	execSQL(t, db, "ROLLBACK")

	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM tx_test").Scan(&count)
	if err != nil {
		t.Fatalf("count after rollback failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row after rollback, got %d", count)
	}
}

func TestConcurrentQueries(t *testing.T) {
	db := openDB(t)

	execSQL(t, db, "DROP TABLE IF EXISTS concurrent_test")
	execSQL(t, db, "CREATE TABLE concurrent_test (id INTEGER, goroutine_id INTEGER)")

	const numGoroutines = 10
	const rowsPerGoroutine = 5

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*rowsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < rowsPerGoroutine; i++ {
				_, err := db.ExecContext(context.Background(),
					fmt.Sprintf("INSERT INTO concurrent_test VALUES (%d, %d)", gID*100+i, gID))
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d insert %d: %w", gID, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent insert error: %v", err)
	}

	var count int
	err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM concurrent_test").Scan(&count)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	expected := numGoroutines * rowsPerGoroutine
	if count != expected {
		t.Errorf("expected %d rows, got %d", expected, count)
	}

	var readWg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		readWg.Add(1)
		go func(gID int) {
			defer readWg.Done()
			var c int
			err := db.QueryRowContext(context.Background(),
				fmt.Sprintf("SELECT COUNT(*) FROM concurrent_test WHERE goroutine_id = %d", gID)).Scan(&c)
			if err != nil {
				t.Errorf("goroutine %d read failed: %v", gID, err)
				return
			}
			if c != rowsPerGoroutine {
				t.Errorf("goroutine %d: expected %d rows, got %d", gID, rowsPerGoroutine, c)
			}
		}(g)
	}
	readWg.Wait()
}
