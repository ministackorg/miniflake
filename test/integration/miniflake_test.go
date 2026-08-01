//go:build integration

package integration

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
	stageMgr *stage.Manager
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
	stageMgr = stage.NewManager(stageDir)
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

	// Start the server WITH the orchestrator. TLS is enabled so the same port
	// serves both plain HTTP (the existing protocol=http tests) and HTTPS
	// (TestTLSConnectionHTTPS) — proving the auto-detect listener doesn't
	// regress the plain-HTTP path.
	testSrv = server.New(testEng, sessMgr, "127.0.0.1", testPort, stageDir, orch)
	if err := testSrv.EnableTLS("", "", dataDir); err != nil {
		fmt.Fprintf(os.Stderr, "failed to enable tls: %v\n", err)
		os.Exit(1)
	}
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

// ---------------------------------------------------------------------------
// Snowflake-specific features wired through the orchestrator
// ---------------------------------------------------------------------------

func TestMergeUpsert(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP TABLE IF EXISTS merge_target")
	execSQL(t, db, "DROP TABLE IF EXISTS merge_source")
	// DuckDB's ON CONFLICT requires a UNIQUE or PRIMARY KEY constraint on
	// the conflict target column. The MERGE rewriter emits a single-column
	// ON CONFLICT clause, so the target needs PRIMARY KEY on that column.
	execSQL(t, db, "CREATE TABLE merge_target (id INT PRIMARY KEY, v VARCHAR)")
	execSQL(t, db, "CREATE TABLE merge_source (id INT, v VARCHAR)")
	execSQL(t, db, "INSERT INTO merge_target VALUES (1, 'old')")
	execSQL(t, db, "INSERT INTO merge_source VALUES (1, 'new'), (2, 'fresh')")

	execSQL(t, db, `MERGE INTO merge_target t USING merge_source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)`)

	rows, err := db.Query("SELECT id, v FROM merge_target ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[int]string{}
	for rows.Next() {
		var id int
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatal(err)
		}
		got[id] = v
	}
	if got[1] != "new" {
		t.Errorf("expected update id=1 to 'new', got %q", got[1])
	}
	if got[2] != "fresh" {
		t.Errorf("expected insert id=2='fresh', got %q", got[2])
	}
}

func TestCreateStreamAndDropTableSnapshot(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP TABLE IF EXISTS streamed")
	execSQL(t, db, "CREATE TABLE streamed (id INT, v VARCHAR)")
	execSQL(t, db, "INSERT INTO streamed VALUES (1, 'a')")
	execSQL(t, db, "CREATE STREAM streamed_stream ON TABLE streamed")
	execSQL(t, db, "INSERT INTO streamed VALUES (2, 'b')")

	rows, err := db.Query("SHOW STREAMS")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) < 3 {
		t.Fatalf("SHOW STREAMS columns: %v", cols)
	}
	found := false
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		// Column 1 (index 1) is `name`.
		if name, ok := vals[1].(string); ok && strings.EqualFold(name, "streamed_stream") {
			found = true
		}
	}
	if !found {
		t.Error("streamed_stream not in SHOW STREAMS output")
	}

	execSQL(t, db, "DROP STREAM streamed_stream")
}

func TestUndropTableRestoresData(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP TABLE IF EXISTS undrop_me")
	execSQL(t, db, "CREATE TABLE undrop_me (id INT, name VARCHAR)")
	execSQL(t, db, "INSERT INTO undrop_me VALUES (1, 'a'), (2, 'b')")
	execSQL(t, db, "DROP TABLE undrop_me")
	execSQL(t, db, "UNDROP TABLE undrop_me")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM undrop_me").Scan(&count); err != nil {
		t.Fatalf("query after UNDROP: %v", err)
	}
	if count != 2 {
		t.Errorf("after UNDROP expected 2 rows, got %d", count)
	}
}

func TestCreateAndShowTask(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP TASK IF EXISTS test_task")
	execSQL(t, db, "DROP TABLE IF EXISTS task_target")
	execSQL(t, db, "CREATE TABLE task_target (n INT)")
	execSQL(t, db, "CREATE TASK test_task WAREHOUSE = wh SCHEDULE = '5 MINUTE' AS INSERT INTO task_target VALUES (42)")

	rows, err := db.Query("SHOW TASKS")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	found := false
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		if name, ok := vals[1].(string); ok && strings.EqualFold(name, "test_task") {
			found = true
		}
	}
	if !found {
		t.Error("test_task not in SHOW TASKS")
	}

	// EXECUTE TASK runs the body once.
	execSQL(t, db, "EXECUTE TASK test_task")
	var n int
	if err := db.QueryRow("SELECT n FROM task_target").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Errorf("expected EXECUTE TASK to insert 42, got %d", n)
	}

	execSQL(t, db, "DROP TASK test_task")
}

func TestCreateAndShowPipe(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP PIPE IF EXISTS test_pipe")
	execSQL(t, db, "CREATE PIPE test_pipe AS COPY INTO ingest_t FROM @ingest_stage")

	rows, err := db.Query("SHOW PIPES")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	found := false
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		if name, ok := vals[1].(string); ok && strings.EqualFold(name, "test_pipe") {
			found = true
		}
	}
	if !found {
		t.Error("test_pipe not in SHOW PIPES")
	}
	execSQL(t, db, "DROP PIPE test_pipe")
}

// TestSnowpipeRESTIngest verifies the v1 insertFiles endpoint reaches the
// snowpipe engine. We use stdlib HTTP because the gosnowflake driver doesn't
// expose this endpoint — clients hit it directly with their account creds.
func TestSnowpipeRESTIngest(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP PIPE IF EXISTS rest_pipe")
	execSQL(t, db, "DROP TABLE IF EXISTS rest_target")
	execSQL(t, db, "CREATE TABLE rest_target (n INT)")
	execSQL(t, db, "CREATE PIPE rest_pipe AS COPY INTO rest_target FROM @some_stage")

	url := fmt.Sprintf("http://127.0.0.1:%d/v1/data/pipes/TESTDB/PUBLIC/rest_pipe/insertFiles", testPort)
	body := `{"files":[{"path":"a.csv","size":12},{"path":"b.csv","size":34}]}`
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		buf := make([]byte, 1024)
		n, _ := resp.Body.Read(buf)
		t.Fatalf("status %d: %s", resp.StatusCode, buf[:n])
	}
}

func TestCreateTableAsSelect(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP TABLE IF EXISTS ctas_src")
	execSQL(t, db, "DROP TABLE IF EXISTS ctas_dst")
	execSQL(t, db, "CREATE TABLE ctas_src (id INT, v VARCHAR)")
	execSQL(t, db, "INSERT INTO ctas_src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	execSQL(t, db, "CREATE TABLE ctas_dst AS SELECT id, v FROM ctas_src WHERE id > 1")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM ctas_dst").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("CTAS expected 2 rows, got %d", n)
	}
}

func TestLateralFlatten(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT t.value::INTEGER AS v FROM (SELECT [10, 20, 30] AS arr) src, LATERAL FLATTEN(input => src.arr) AS t ORDER BY v`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if len(got) != 3 || got[0] != 10 || got[1] != 20 || got[2] != 30 {
		t.Errorf("FLATTEN result: %v", got)
	}
}

func TestQualifyClause(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP TABLE IF EXISTS qual_t")
	execSQL(t, db, "CREATE TABLE qual_t (g INT, v INT)")
	execSQL(t, db, "INSERT INTO qual_t VALUES (1, 10), (1, 20), (1, 30), (2, 99)")

	rows, err := db.Query(`SELECT g, v FROM qual_t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) = 1 ORDER BY g`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type kv struct{ g, v int }
	var out []kv
	for rows.Next() {
		var k kv
		if err := rows.Scan(&k.g, &k.v); err != nil {
			t.Fatal(err)
		}
		out = append(out, k)
	}
	if len(out) != 2 || out[0] != (kv{1, 30}) || out[1] != (kv{2, 99}) {
		t.Errorf("QUALIFY output: %v", out)
	}
}

// PUT/GET are implemented server-side (see internal/orchestrator/putget.go +
// internal/orchestrator/putget_test.go), but the gosnowflake driver
// intercepts these commands client-side to do its own presigned-URL flow,
// which never reaches our server. The Python and JDBC drivers behave
// differently — server-side tests live in the orchestrator package.
//
// LIST/LS is a plain metadata query (no client-side interception), so it's
// exercised end-to-end here through gosnowflake. The file is seeded
// directly into the stage's backing directory, the same place PUT would
// have dropped it, because PUT itself can't be driven from this test.
func TestListStage(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP STAGE IF EXISTS list_test_stage")
	execSQL(t, db, "CREATE STAGE list_test_stage")

	// Ask the stage manager where the stage lives rather than rebuilding its
	// on-disk layout here, so a layout change can't fail this test for the
	// wrong reason.
	meta, err := stageMgr.GetStage("TESTDB", "PUBLIC", "list_test_stage")
	if err != nil {
		t.Fatalf("resolve stage: %v", err)
	}
	localPath := meta.LocalPath
	content := []byte("id,name\n1,alice\n")
	if err := os.MkdirAll(filepath.Join(localPath, "nested"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(localPath, "data.csv"), content, 0o644); err != nil {
		t.Fatalf("seed stage file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(localPath, "nested", "more.csv"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed nested file: %v", err)
	}

	sum := md5.Sum(content)
	wantMD5 := hex.EncodeToString(sum[:])

	for _, stmt := range []string{
		"LIST @list_test_stage",
		"LS @list_test_stage",
		"LIST @PUBLIC.list_test_stage",
		"LIST @TESTDB.PUBLIC.list_test_stage",
	} {
		assertListHasFile(t, db, stmt, "list_test_stage/data.csv", int64(len(content)), wantMD5)
	}

	// Subpath + PATTERN must filter, not silently return the whole stage.
	rows, err := db.Query("LIST @list_test_stage/nested PATTERN = '.*\\.csv$'")
	if err != nil {
		t.Fatalf("LIST subpath+pattern: %v", err)
	}
	defer rows.Close()
	var name, gotMD5, lastModified string
	var size int64
	if !rows.Next() {
		t.Fatal("expected 1 nested row")
	}
	if err := rows.Scan(&name, &size, &gotMD5, &lastModified); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if name != "list_test_stage/nested/more.csv" {
		t.Errorf("name = %q", name)
	}
	if rows.Next() {
		t.Error("expected exactly 1 nested row")
	}

	execSQL(t, db, "DROP STAGE list_test_stage")
}

// REMOVE is driven end-to-end through gosnowflake alongside LIST, since the
// two have to agree on which files a reference names.
func TestRemoveStage(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP STAGE IF EXISTS remove_test_stage")
	execSQL(t, db, "CREATE STAGE remove_test_stage")

	meta, err := stageMgr.GetStage("TESTDB", "PUBLIC", "remove_test_stage")
	if err != nil {
		t.Fatalf("resolve stage: %v", err)
	}
	for _, name := range []string{"gone.csv", "kept.txt"} {
		if werr := os.WriteFile(filepath.Join(meta.LocalPath, name), []byte("x"), 0o644); werr != nil {
			t.Fatal(werr)
		}
	}

	rows, err := db.Query("REMOVE @remove_test_stage PATTERN = '.*[.]csv'")
	if err != nil {
		t.Fatalf("REMOVE: %v", err)
	}
	cols, _ := rows.Columns()
	if got := strings.Join(cols, ","); got != "name,result" {
		t.Errorf("columns = %q, want name,result", got)
	}
	removed := 0
	for rows.Next() {
		var name, result string
		if serr := rows.Scan(&name, &result); serr != nil {
			t.Fatalf("scan: %v", serr)
		}
		removed++
		if name != "remove_test_stage/gone.csv" {
			t.Errorf("removed unexpected file %q", name)
		}
		if result != "removed" {
			t.Errorf("result = %q, want removed", result)
		}
	}
	rows.Close()
	if removed != 1 {
		t.Fatalf("removed %d files, want 1", removed)
	}

	// Only the file the PATTERN matched may be gone.
	if _, serr := os.Stat(filepath.Join(meta.LocalPath, "gone.csv")); !os.IsNotExist(serr) {
		t.Error("gone.csv should have been deleted")
	}
	if _, serr := os.Stat(filepath.Join(meta.LocalPath, "kept.txt")); serr != nil {
		t.Errorf("kept.txt should have survived: %v", serr)
	}

	execSQL(t, db, "DROP STAGE remove_test_stage")
}

// COPY INTO from a bare @stage reference, the form the README documents for
// Snowpipe, has to resolve against the session database and schema.
func TestCopyIntoBareStageRef(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP STAGE IF EXISTS copy_ref_stage")
	execSQL(t, db, "CREATE STAGE copy_ref_stage")
	execSQL(t, db, "CREATE OR REPLACE TABLE copy_ref_target (id INTEGER, name VARCHAR)")

	meta, err := stageMgr.GetStage("TESTDB", "PUBLIC", "copy_ref_stage")
	if err != nil {
		t.Fatalf("resolve stage: %v", err)
	}
	if werr := os.WriteFile(filepath.Join(meta.LocalPath, "rows.csv"), []byte("1,alice\n2,bob\n"), 0o644); werr != nil {
		t.Fatal(werr)
	}

	execSQL(t, db, "COPY INTO copy_ref_target FROM @copy_ref_stage FILE_FORMAT = (TYPE = 'CSV')")

	var count int
	if qerr := db.QueryRow("SELECT COUNT(*) FROM copy_ref_target").Scan(&count); qerr != nil {
		t.Fatalf("count: %v", qerr)
	}
	if count != 2 {
		t.Fatalf("loaded %d rows, want 2", count)
	}

	execSQL(t, db, "DROP STAGE copy_ref_stage")
}

func TestListUserStage(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Integration sessions authenticate as user "test" (see openDB), and the
	// user stage is created on first reference.
	userPath := stageMgr.GetUserStage("test").LocalPath
	content := []byte("user-stage\n")
	if err := os.WriteFile(filepath.Join(userPath, "u.csv"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	sum := md5.Sum(content)
	assertListHasFile(t, db, "LS @~", "~/u.csv", int64(len(content)), hex.EncodeToString(sum[:]))
}

func assertListHasFile(t *testing.T, db *sql.DB, stmt, wantName string, wantSize int64, wantMD5 string) {
	t.Helper()
	rows, err := db.Query(stmt)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	if got := strings.Join(cols, ","); got != "name,size,md5,last_modified" {
		t.Errorf("%s: columns = %q", stmt, got)
	}

	found := false
	for rows.Next() {
		var name, gotMD5, lastModified string
		var size int64
		if err := rows.Scan(&name, &size, &gotMD5, &lastModified); err != nil {
			t.Fatalf("%s: scan: %v", stmt, err)
		}
		if name != wantName {
			continue
		}
		found = true
		if size != wantSize {
			t.Errorf("%s: size = %d, want %d", stmt, size, wantSize)
		}
		if gotMD5 != wantMD5 {
			t.Errorf("%s: md5 = %q, want %q", stmt, gotMD5, wantMD5)
		}
		if lastModified == "" {
			t.Errorf("%s: last_modified was empty", stmt)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: rows: %v", stmt, err)
	}
	if !found {
		t.Fatalf("%s: missing row %q", stmt, wantName)
	}
}

func TestTimeTravelAtOffset(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	execSQL(t, db, "DROP TABLE IF EXISTS tt_t")
	execSQL(t, db, "CREATE TABLE tt_t (id INT)")
	execSQL(t, db, "INSERT INTO tt_t VALUES (1)")
	execSQL(t, db, "INSERT INTO tt_t VALUES (2)")

	// AT(OFFSET => -N) returns the snapshot at-or-before `now - N seconds`.
	// The snapshots captured pre-INSERT are <2s old, so a 2-second sleep
	// here puts all of them on the eligible side of the cutoff.
	time.Sleep(2 * time.Second)

	rows, err := db.Query(`SELECT id FROM tt_t AT(OFFSET => -1) ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		got = append(got, n)
	}
	// The most-recent snapshot at-or-before (now-1s) is the one taken right
	// before the second INSERT — table state {1}.
	if len(got) != 1 || got[0] != 1 {
		t.Errorf("AT(OFFSET) result: %v (want [1])", got)
	}
}
