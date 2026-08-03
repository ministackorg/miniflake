package clone

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CloneType represents the kind of object being cloned.
type CloneType string

const (
	CloneDatabase CloneType = "DATABASE"
	CloneSchema   CloneType = "SCHEMA"
	CloneTable    CloneType = "TABLE"
)

// CloneRecord tracks a clone operation.
type CloneRecord struct {
	SourceName string
	CloneName  string
	Type       CloneType
	CreatedAt  time.Time
}

// QueryFn executes a SQL query and returns column names, rows, and any error.
type QueryFn func(ctx context.Context, sql string) ([]string, [][]interface{}, error)

// Engine manages zero-copy clone operations backed by DuckDB.
type Engine struct {
	mu      sync.RWMutex
	clones  []CloneRecord
	execFn  func(ctx context.Context, sql string) error
	queryFn QueryFn
}

// NewEngine creates a new clone engine. execFn runs DDL/DML statements.
// queryFn runs SELECT queries and returns results.
func NewEngine(execFn func(ctx context.Context, sql string) error, queryFn QueryFn) *Engine {
	return &Engine{
		execFn:  execFn,
		queryFn: queryFn,
	}
}

// Reset clears clone records. Table data itself is wiped by engine.Reset.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clones = nil
}

// CloneTable creates a copy of a table using CREATE TABLE ... AS SELECT *.
func (e *Engine) CloneTable(ctx context.Context, srcDB, srcSchema, srcTable, dstDB, dstSchema, dstTable string) error {
	src := fmt.Sprintf(`"%s"."%s"."%s"`, srcDB, srcSchema, srcTable)
	dst := fmt.Sprintf(`"%s"."%s"."%s"`, dstDB, dstSchema, dstTable)
	sql := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", dst, src)
	if err := e.execFn(ctx, sql); err != nil {
		return fmt.Errorf("clone table: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.clones = append(e.clones, CloneRecord{
		SourceName: src,
		CloneName:  dst,
		Type:       CloneTable,
		CreatedAt:  time.Now(),
	})
	return nil
}

// CloneSchema clones all tables from a source schema into a destination schema.
func (e *Engine) CloneSchema(ctx context.Context, srcDB, srcSchema, dstDB, dstSchema string) error {
	// Create destination schema.
	createSQL := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"."%s"`, dstDB, dstSchema)
	if err := e.execFn(ctx, createSQL); err != nil {
		return fmt.Errorf("clone schema: create dst schema: %w", err)
	}

	// List tables in source schema.
	tables, err := e.listTables(ctx, srcDB, srcSchema)
	if err != nil {
		return fmt.Errorf("clone schema: list tables: %w", err)
	}

	for _, tbl := range tables {
		if err := e.CloneTable(ctx, srcDB, srcSchema, tbl, dstDB, dstSchema, tbl); err != nil {
			return fmt.Errorf("clone schema: table %s: %w", tbl, err)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.clones = append(e.clones, CloneRecord{
		SourceName: fmt.Sprintf(`"%s"."%s"`, srcDB, srcSchema),
		CloneName:  fmt.Sprintf(`"%s"."%s"`, dstDB, dstSchema),
		Type:       CloneSchema,
		CreatedAt:  time.Now(),
	})
	return nil
}

// CloneDatabase clones all schemas and their tables from one database to another.
func (e *Engine) CloneDatabase(ctx context.Context, srcDB, dstDB string) error {
	// Create destination database.
	createSQL := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"."main"`, dstDB)
	if err := e.execFn(ctx, createSQL); err != nil {
		return fmt.Errorf("clone database: create dst: %w", err)
	}

	// List schemas in source database.
	schemas, err := e.listSchemas(ctx, srcDB)
	if err != nil {
		return fmt.Errorf("clone database: list schemas: %w", err)
	}

	for _, schema := range schemas {
		if err := e.CloneSchema(ctx, srcDB, schema, dstDB, schema); err != nil {
			return fmt.Errorf("clone database: schema %s: %w", schema, err)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.clones = append(e.clones, CloneRecord{
		SourceName: srcDB,
		CloneName:  dstDB,
		Type:       CloneDatabase,
		CreatedAt:  time.Now(),
	})
	return nil
}

// ListClones returns all recorded clone operations.
func (e *Engine) ListClones() []CloneRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]CloneRecord, len(e.clones))
	copy(out, e.clones)
	return out
}

// listTables queries DuckDB for table names in a given database.schema.
func (e *Engine) listTables(ctx context.Context, db, schema string) ([]string, error) {
	sql := fmt.Sprintf(
		"SELECT table_name FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = '%s' ORDER BY table_name",
		db, schema,
	)
	_, rows, err := e.queryFn(ctx, sql)
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

// listSchemas queries DuckDB for schema names in a given database.
func (e *Engine) listSchemas(ctx context.Context, db string) ([]string, error) {
	sql := fmt.Sprintf(
		"SELECT schema_name FROM duckdb_schemas() WHERE database_name = '%s' ORDER BY schema_name",
		db,
	)
	_, rows, err := e.queryFn(ctx, sql)
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
