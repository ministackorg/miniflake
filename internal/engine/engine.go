package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// snowflakeEngineConfig is applied as DuckDB global config so every pooled
// connection behaves like a default Snowflake session:
//   - TimeZone matches the TIMEZONE session default (America/Los_Angeles)
//     instead of the host's local zone.
//   - ieee_floating_point_ops=false makes x / 0 raise "Division by zero" like
//     Snowflake; DuckDB 2.0 otherwise returns inf.
const snowflakeEngineConfig = "TimeZone=America%2FLos_Angeles&ieee_floating_point_ops=false"

// DefaultDatabase and DefaultSchema are the catalog and schema a session
// starts in, and what statements without a session resolve against.
const (
	DefaultDatabase = "miniflake"
	DefaultSchema   = "main"
)

// Engine is the core SQL execution layer backed by DuckDB.
// It is safe for concurrent use by multiple goroutines.
//
// A Snowflake session maps onto one DuckDB connection. DuckDB keeps the
// current catalog/schema and the open transaction per connection, so a
// session's statements must not be spread over the pool: each session ID
// (WithSession) gets a pinned connection that stays pointed at the session's
// current database/schema, and BEGIN ... COMMIT on it is a real transaction.
// Statements without a session borrow a pooled connection, pointed at the
// default miniflake.main or at the WithSession path for that one statement.
type Engine struct {
	db      *sql.DB
	dataDir string

	mu sync.RWMutex

	sessMu   sync.Mutex
	sessions map[string]*sessionConn
}

// sessionConnIdleTimeout closes a session's pinned connection after this long
// without use. It matches the session manager's own expiry.
const sessionConnIdleTimeout = 4 * time.Hour

type sessionConn struct {
	mu       sync.Mutex // one statement at a time per session
	conn     *sql.Conn
	path     searchPath // what the connection currently points at
	lastUsed time.Time
}

type sessionKey struct{}

type sessionInfo struct {
	id   string
	path searchPath
}

type searchPath struct{ database, schema string }

// WithSession returns a context whose statements run on the session's own
// connection and resolve unqualified and two-part names against
// database.schema, like a Snowflake session after USE SCHEMA
// database.schema. An empty id still sets the search path, for one
// statement at a time.
func WithSession(ctx context.Context, id, database, schema string) context.Context {
	return context.WithValue(ctx, sessionKey{}, sessionInfo{id, searchPath{database, schema}})
}

// WithSearchPath is WithSession without a session connection.
func WithSearchPath(ctx context.Context, database, schema string) context.Context {
	return WithSession(ctx, "", database, schema)
}

func sessionFrom(ctx context.Context) sessionInfo {
	info, _ := ctx.Value(sessionKey{}).(sessionInfo)
	if info.path.database == "" {
		info.path = searchPath{DefaultDatabase, DefaultSchema}
	}
	if info.path.schema == "" {
		info.path.schema = DefaultSchema
	}
	return info
}

func (p searchPath) isDefault() bool {
	return strings.EqualFold(p.database, DefaultDatabase) && strings.EqualFold(p.schema, DefaultSchema)
}

func (p searchPath) equal(q searchPath) bool {
	return p.database == q.database && p.schema == q.schema
}

func quoteIdent(s string) string {
	return `"` + escapeIdent(s) + `"`
}

var defaultPath = searchPath{DefaultDatabase, DefaultSchema}

// usePath points c at path. If the database or schema no longer exists (it
// was dropped), it falls back to the database alone and then to the
// default, the way Snowflake keeps a session usable after its current schema
// is dropped. It returns the path actually in effect.
func usePath(ctx context.Context, c *sql.Conn, path searchPath) searchPath {
	if _, err := c.ExecContext(ctx, "USE "+quoteIdent(path.database)+"."+quoteIdent(path.schema)); err == nil {
		return path
	}
	if _, err := c.ExecContext(ctx, "USE "+quoteIdent(path.database)); err == nil {
		return searchPath{path.database, DefaultSchema}
	}
	_, _ = c.ExecContext(ctx, "USE "+quoteIdent(DefaultDatabase)+"."+quoteIdent(DefaultSchema))
	return defaultPath
}

// conn returns the connection a statement in ctx runs on, already pointed at
// the context's search path, and the func that releases it.
func (e *Engine) conn(ctx context.Context) (*sql.Conn, func(), error) {
	info := sessionFrom(ctx)
	if info.id != "" {
		return e.sessionConn(ctx, info)
	}

	c, err := e.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	if info.path.isDefault() {
		return c, func() { c.Close() }, nil
	}
	usePath(ctx, c, info.path)
	release := func() { returnToPool(c) }
	return c, release, nil
}

// returnToPool hands c back to the pool pointed at the default path, with no
// transaction open. sql.Conn.Close does not close the driver connection, so a
// connection returned with another catalog selected would leak that path to
// the next borrower (and DuckDB refuses to DETACH a catalog some connection
// uses as its default). A connection that cannot be reset is discarded.
func returnToPool(c *sql.Conn) {
	ctx := context.Background()
	_, _ = c.ExecContext(ctx, "ROLLBACK") // fails harmlessly when no transaction is open
	if _, err := c.ExecContext(ctx, "USE "+quoteIdent(DefaultDatabase)+"."+quoteIdent(DefaultSchema)); err != nil {
		_ = c.Raw(func(any) error { return driver.ErrBadConn })
	}
	c.Close()
}

func (e *Engine) sessionConn(ctx context.Context, info sessionInfo) (*sql.Conn, func(), error) {
	e.sessMu.Lock()
	if e.sessions == nil {
		e.sessions = make(map[string]*sessionConn)
	}
	sc, ok := e.sessions[info.id]
	if !ok {
		e.closeIdleLocked()
		c, err := e.db.Conn(context.Background())
		if err != nil {
			e.sessMu.Unlock()
			return nil, nil, err
		}
		sc = &sessionConn{conn: c, path: defaultPath}
		e.sessions[info.id] = sc
	}
	e.sessMu.Unlock()

	sc.mu.Lock()
	sc.lastUsed = time.Now()
	if !sc.path.equal(info.path) {
		// Remember what was asked for, not the fallback, so a dropped and
		// re-created schema is picked up again on the next statement.
		usePath(ctx, sc.conn, info.path)
		sc.path = info.path
	}
	return sc.conn, sc.mu.Unlock, nil
}

// closeIdleLocked closes pinned connections of sessions that have been idle
// past the session timeout. Caller holds sessMu.
func (e *Engine) closeIdleLocked() {
	for id, sc := range e.sessions {
		if !sc.mu.TryLock() {
			continue // in use
		}
		if time.Since(sc.lastUsed) > sessionConnIdleTimeout {
			returnToPool(sc.conn)
			delete(e.sessions, id)
		}
		sc.mu.Unlock()
	}
}

// CloseSession releases the pinned connection of a session (logout). An open
// transaction on it is rolled back.
func (e *Engine) CloseSession(id string) {
	e.sessMu.Lock()
	sc, ok := e.sessions[id]
	delete(e.sessions, id)
	e.sessMu.Unlock()
	if ok {
		sc.mu.Lock()
		returnToPool(sc.conn)
		sc.mu.Unlock()
	}
}

// closeAllSessions releases every pinned connection (reset / shutdown).
func (e *Engine) closeAllSessions() {
	e.sessMu.Lock()
	sessions := e.sessions
	e.sessions = nil
	e.sessMu.Unlock()
	for _, sc := range sessions {
		sc.mu.Lock()
		returnToPool(sc.conn)
		sc.mu.Unlock()
	}
}

// New creates a new Engine. It ensures dataDir exists and opens a DuckDB
// database file at dataDir/miniflake.duckdb.
func New(dataDir string) (*Engine, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: create data dir: %w", err)
	}

	dbPath := dataDir + "/miniflake.duckdb"
	db, err := sql.Open("duckdb", dbPath+"?"+snowflakeEngineConfig)
	if err != nil {
		return nil, fmt.Errorf("engine: open duckdb: %w", err)
	}

	// Verify the connection is alive.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("engine: ping duckdb: %w", err)
	}

	return &Engine{
		db:      db,
		dataDir: dataDir,
	}, nil
}

// Close closes the underlying DuckDB connection pool.
func (e *Engine) Close() error {
	e.closeAllSessions()
	return e.db.Close()
}

// Reset drops user tables, views and schemas, detaches non-primary databases,
// and restores the default database/schema context. Used by POST
// /_miniflake/reset for CI isolation. Attached .duckdb files on disk are left
// in place (DETACH only); the next CREATE DATABASE re-ATTACHes them.
func (e *Engine) Reset(ctx context.Context) error {
	// Sessions are wiped with the rest of the state; their connections may
	// hold a detached catalog as default or an open transaction.
	e.closeAllSessions()

	e.mu.Lock()
	defer e.mu.Unlock()

	type obj struct{ db, schema, name string }

	dropAll := func(query, kind string) error {
		rows, err := e.db.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("engine: reset list %s: %w", kind, err)
		}
		defer rows.Close()
		var objs []obj
		for rows.Next() {
			var o obj
			if err := rows.Scan(&o.db, &o.schema, &o.name); err != nil {
				return fmt.Errorf("engine: reset scan %s: %w", kind, err)
			}
			objs = append(objs, o)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, o := range objs {
			stmt := fmt.Sprintf(`DROP %s IF EXISTS "%s"."%s"."%s" CASCADE`,
				kind, escapeIdent(o.db), escapeIdent(o.schema), escapeIdent(o.name))
			if _, err := e.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("engine: reset drop %s %s.%s.%s: %w", kind, o.db, o.schema, o.name, err)
			}
		}
		return nil
	}

	// DuckDB marks system/internal relations; skip those.
	if err := dropAll(
		`SELECT database_name, schema_name, table_name FROM duckdb_tables() WHERE NOT internal`,
		"TABLE",
	); err != nil {
		return err
	}
	if err := dropAll(
		`SELECT database_name, schema_name, view_name FROM duckdb_views() WHERE NOT internal`,
		"VIEW",
	); err != nil {
		return err
	}

	// User macros (SQL UDFs). Those in user schemas would go with DROP SCHEMA
	// CASCADE, but the ones in each catalog's main schema would survive.
	macroRows, err := e.db.QueryContext(ctx,
		`SELECT DISTINCT database_name, schema_name, function_name, function_type FROM duckdb_functions()
		 WHERE NOT internal AND function_type IN ('macro', 'table_macro')`)
	if err != nil {
		return fmt.Errorf("engine: reset list macros: %w", err)
	}
	type macro struct {
		obj
		kind string
	}
	var macros []macro
	for macroRows.Next() {
		var m macro
		if err := macroRows.Scan(&m.db, &m.schema, &m.name, &m.kind); err != nil {
			macroRows.Close()
			return fmt.Errorf("engine: reset scan macros: %w", err)
		}
		macros = append(macros, m)
	}
	if err := macroRows.Err(); err != nil {
		macroRows.Close()
		return err
	}
	macroRows.Close()
	for _, m := range macros {
		kind := "MACRO"
		if m.kind == "table_macro" {
			kind = "MACRO TABLE"
		}
		stmt := fmt.Sprintf(`DROP %s IF EXISTS "%s"."%s"."%s"`, kind,
			escapeIdent(m.db), escapeIdent(m.schema), escapeIdent(m.name))
		if _, err := e.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("engine: reset drop macro %s.%s.%s: %w", m.db, m.schema, m.name, err)
		}
	}

	// User schemas (NOT internal). Built-ins like main / information_schema /
	// pg_catalog stay; without this, CREATE SCHEMA fails on the next CI pass.
	schemaRows, err := e.db.QueryContext(ctx,
		`SELECT database_name, schema_name FROM duckdb_schemas() WHERE NOT internal`)
	if err != nil {
		return fmt.Errorf("engine: reset list schemas: %w", err)
	}
	var schemas []obj
	for schemaRows.Next() {
		var o obj
		if err := schemaRows.Scan(&o.db, &o.schema); err != nil {
			schemaRows.Close()
			return fmt.Errorf("engine: reset scan schemas: %w", err)
		}
		schemas = append(schemas, o)
	}
	if err := schemaRows.Err(); err != nil {
		schemaRows.Close()
		return err
	}
	schemaRows.Close()
	for _, o := range schemas {
		stmt := fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s"."%s" CASCADE`,
			escapeIdent(o.db), escapeIdent(o.schema))
		if _, err := e.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("engine: reset drop schema %s.%s: %w", o.db, o.schema, err)
		}
	}

	// Detach every non-internal database except the primary miniflake file.
	// system/temp are internal; detaching miniflake would close the process DB.
	dbRows, err := e.db.QueryContext(ctx,
		`SELECT database_name FROM duckdb_databases() WHERE NOT internal`)
	if err != nil {
		return fmt.Errorf("engine: reset list databases: %w", err)
	}
	var dbs []string
	for dbRows.Next() {
		var name string
		if err := dbRows.Scan(&name); err != nil {
			dbRows.Close()
			return fmt.Errorf("engine: reset scan databases: %w", err)
		}
		dbs = append(dbs, name)
	}
	if err := dbRows.Err(); err != nil {
		dbRows.Close()
		return err
	}
	dbRows.Close()
	for _, name := range dbs {
		if strings.EqualFold(name, "miniflake") {
			continue
		}
		stmt := fmt.Sprintf(`DETACH "%s"`, escapeIdent(name))
		if _, err := e.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("engine: reset detach %s: %w", name, err)
		}
	}

	return nil
}

func escapeIdent(s string) string {
	return strings.ReplaceAll(s, `"`, `""`)
}

// Execute runs a query that returns rows (SELECT, SHOW, etc.).
// It returns column names, row data, and any error.
func (e *Engine) Execute(ctx context.Context, query string, args ...interface{}) ([]string, [][]interface{}, error) {
	cols, _, rows, err := e.ExecuteTyped(ctx, query, args...)
	return cols, rows, err
}

// ExecuteTyped is Execute plus the DuckDB type name of each result column
// (e.g. "TIME", "DATE", "TIMESTAMP WITH TIME ZONE"). The wire layer needs the
// declared type because a TIME and a TIMESTAMP both scan into time.Time.
func (e *Engine) ExecuteTyped(ctx context.Context, query string, args ...interface{}) ([]string, []string, [][]interface{}, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	c, release, err := e.conn(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("engine: query: %w", err)
	}
	defer release()

	rows, err := c.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("engine: query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("engine: columns: %w", err)
	}
	types := make([]string, len(columns))
	if cts, err := rows.ColumnTypes(); err == nil {
		for i, ct := range cts {
			if i < len(types) {
				types[i] = ct.DatabaseTypeName()
			}
		}
	}

	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, nil, fmt.Errorf("engine: scan: %w", err)
		}
		result = append(result, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("engine: rows iteration: %w", err)
	}

	return columns, types, result, nil
}

// ExecNoResult runs a statement that does not return rows (DDL/DML).
// It returns the number of rows affected and any error.
func (e *Engine) ExecNoResult(ctx context.Context, query string, args ...interface{}) (int64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	c, release, err := e.conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("engine: exec: %w", err)
	}
	defer release()

	res, err := c.ExecContext(ctx, query, args...)
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
	e.mu.Lock()
	defer e.mu.Unlock()

	dbPath := e.dataDir + "/" + name + ".duckdb"
	query := fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS \"%s\"", dbPath, name)
	_, err := e.db.Exec(query)
	if err != nil {
		return fmt.Errorf("engine: attach database %q: %w", name, err)
	}
	return nil
}

// ResolveDatabase returns the stored name of the attached database that
// matches name case-insensitively (DuckDB's own identifier rule), or false.
func (e *Engine) ResolveDatabase(ctx context.Context, name string) (string, bool) {
	_, rows, err := e.Execute(ctx,
		"SELECT database_name FROM duckdb_databases() WHERE lower(database_name) = lower(?) AND database_name NOT IN ('system', 'temp')", name)
	if err != nil || len(rows) == 0 {
		return "", false
	}
	s, ok := rows[0][0].(string)
	return s, ok
}

// ResolveSchema returns the stored names of database.schema, matched
// case-insensitively, or false when either does not exist.
func (e *Engine) ResolveSchema(ctx context.Context, database, schema string) (string, string, bool) {
	_, rows, err := e.Execute(ctx,
		"SELECT database_name, schema_name FROM duckdb_schemas() WHERE lower(database_name) = lower(?) AND lower(schema_name) = lower(?)",
		database, schema)
	if err != nil || len(rows) == 0 {
		return "", "", false
	}
	db, _ := rows[0][0].(string)
	sc, _ := rows[0][1].(string)
	return db, sc, db != "" && sc != ""
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
