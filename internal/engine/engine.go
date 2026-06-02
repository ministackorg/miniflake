package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/marcboeker/go-duckdb"
)

// Engine is the core SQL execution layer backed by DuckDB.
// It is safe for concurrent use by multiple goroutines.
type Engine struct {
	db      *sql.DB
	dataDir string

	mu            sync.RWMutex
	currentDB     string
	currentSchema string
}

// New creates a new Engine. It ensures dataDir exists and opens a DuckDB
// database file at dataDir/miniflake.duckdb.
func New(dataDir string) (*Engine, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: create data dir: %w", err)
	}

	dbPath := dataDir + "/miniflake.duckdb"
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("engine: open duckdb: %w", err)
	}

	// Verify the connection is alive.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("engine: ping duckdb: %w", err)
	}

	return &Engine{
		db:            db,
		dataDir:       dataDir,
		currentDB:     "miniflake",
		currentSchema: "main",
	}, nil
}

// Close closes the underlying DuckDB connection pool.
func (e *Engine) Close() error {
	return e.db.Close()
}

// Execute runs a query that returns rows (SELECT, SHOW, etc.).
// It returns column names, row data, and any error.
func (e *Engine) Execute(ctx context.Context, query string, args ...interface{}) ([]string, [][]interface{}, error) {
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("engine: query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("engine: columns: %w", err)
	}

	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, fmt.Errorf("engine: scan: %w", err)
		}
		result = append(result, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("engine: rows iteration: %w", err)
	}

	return columns, result, nil
}

// ExecNoResult runs a statement that does not return rows (DDL/DML).
// It returns the number of rows affected and any error.
func (e *Engine) ExecNoResult(ctx context.Context, query string, args ...interface{}) (int64, error) {
	res, err := e.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("engine: exec: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		// Some DDL statements don't support RowsAffected; treat as 0.
		return 0, nil
	}
	return affected, nil
}

// AttachDatabase attaches an additional DuckDB database file under the given
// logical name. The file is stored in the engine's data directory.
func (e *Engine) AttachDatabase(name string) error {
	dbPath := e.dataDir + "/" + name + ".duckdb"
	query := fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS \"%s\"", dbPath, name)
	_, err := e.db.Exec(query)
	if err != nil {
		return fmt.Errorf("engine: attach database %q: %w", name, err)
	}
	return nil
}

// SetCurrentDatabase sets the active database for catalog queries.
func (e *Engine) SetCurrentDatabase(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.currentDB = name
}

// SetCurrentSchema sets the active schema for catalog queries.
func (e *Engine) SetCurrentSchema(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.currentSchema = name
}

// CurrentDatabase returns the active database name.
func (e *Engine) CurrentDatabase() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentDB
}

// CurrentSchema returns the active schema name.
func (e *Engine) CurrentSchema() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentSchema
}

// GetDatabases returns the list of attached database names.
func (e *Engine) GetDatabases(ctx context.Context) ([]string, error) {
	cols, rows, err := e.Execute(ctx, "SELECT database_name FROM duckdb_databases() ORDER BY database_name")
	_ = cols
	if err != nil {
		return nil, err
	}
	var dbs []string
	for _, row := range rows {
		if name, ok := row[0].(string); ok {
			dbs = append(dbs, name)
		}
	}
	return dbs, nil
}

// GetSchemas returns the list of schema names in the given database.
func (e *Engine) GetSchemas(ctx context.Context, dbName string) ([]string, error) {
	query := "SELECT schema_name FROM duckdb_schemas() WHERE database_name = ? ORDER BY schema_name"
	cols, rows, err := e.Execute(ctx, query, dbName)
	_ = cols
	if err != nil {
		return nil, err
	}
	var schemas []string
	for _, row := range rows {
		if name, ok := row[0].(string); ok {
			schemas = append(schemas, name)
		}
	}
	return schemas, nil
}

// GetTables returns the list of table names in the given database and schema.
func (e *Engine) GetTables(ctx context.Context, dbName, schemaName string) ([]string, error) {
	query := "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? ORDER BY table_name"
	cols, rows, err := e.Execute(ctx, query, dbName, schemaName)
	_ = cols
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, row := range rows {
		if name, ok := row[0].(string); ok {
			tables = append(tables, name)
		}
	}
	return tables, nil
}
