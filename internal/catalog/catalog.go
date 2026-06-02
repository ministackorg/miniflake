package catalog

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Catalog manages the Snowflake object hierarchy:
// Account -> Database -> Schema -> Table/View/Stage/Stream/Task/Pipe
type Catalog struct {
	mu         sync.RWMutex
	databases  map[string]*Database
	warehouses map[string]*Warehouse
}

// Database represents a Snowflake database.
type Database struct {
	Name      string
	Schemas   map[string]*Schema
	CreatedAt time.Time
	Owner     string
}

// Schema represents a Snowflake schema within a database.
type Schema struct {
	Name      string
	Tables    map[string]*TableMeta
	Views     map[string]*ViewMeta
	Stages    map[string]*StageMeta
	Streams   map[string]*StreamMeta
	Tasks     map[string]*TaskMeta
	Pipes     map[string]*PipeMeta
	CreatedAt time.Time
	Owner     string
}

// TableMeta holds metadata about a table (actual data lives in DuckDB).
type TableMeta struct {
	Name        string
	Columns     []ColumnMeta
	ClusterBy   []string
	Comment     string
	IsTemporary bool
	IsTransient bool
	CreatedAt   time.Time
	Owner       string
}

// ColumnMeta describes a single column.
type ColumnMeta struct {
	Name     string
	Type     string // Snowflake type name
	Nullable bool
	Default  string
	Comment  string
}

// ViewMeta holds metadata about a view.
type ViewMeta struct {
	Name       string
	Definition string
	Columns    []ColumnMeta
	CreatedAt  time.Time
}

// StageMeta holds metadata about a stage (catalog-level reference).
type StageMeta struct {
	Name      string
	StageType string
	URL       string
	CreatedAt time.Time
}

// StreamMeta holds metadata about a stream.
type StreamMeta struct {
	Name      string
	TableName string
	CreatedAt time.Time
}

// TaskMeta holds metadata about a task.
type TaskMeta struct {
	Name       string
	Definition string
	Schedule   string
	State      string
	CreatedAt  time.Time
}

// PipeMeta holds metadata about a pipe.
type PipeMeta struct {
	Name       string
	Definition string
	CreatedAt  time.Time
}

// Warehouse represents a virtual warehouse.
type Warehouse struct {
	Name        string
	Size        string // X-Small, Small, Medium, Large, etc.
	State       string // STARTED, SUSPENDED
	AutoSuspend int    // seconds
	CreatedAt   time.Time
}

// New creates a new empty Catalog.
func New() *Catalog {
	return &Catalog{
		databases:  make(map[string]*Database),
		warehouses: make(map[string]*Warehouse),
	}
}

// Init creates the default database and warehouse that Snowflake provides.
func (c *Catalog) Init() {
	_ = c.CreateDatabase("SNOWFLAKE_SAMPLE_DATA", "SYSADMIN")
	_ = c.CreateWarehouse("COMPUTE_WH", "X-Small", 600)
}

// --- Database operations ---

func (c *Catalog) CreateDatabase(name, owner string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := strings.ToUpper(name)
	if _, exists := c.databases[key]; exists {
		return fmt.Errorf("database '%s' already exists", key)
	}
	db := &Database{
		Name:      key,
		Schemas:   make(map[string]*Schema),
		CreatedAt: time.Now(),
		Owner:     owner,
	}
	// Every database gets PUBLIC and INFORMATION_SCHEMA by default.
	db.Schemas["PUBLIC"] = newSchema("PUBLIC", owner)
	db.Schemas["INFORMATION_SCHEMA"] = newSchema("INFORMATION_SCHEMA", "")
	c.databases[key] = db
	return nil
}

func (c *Catalog) DropDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := strings.ToUpper(name)
	if _, exists := c.databases[key]; !exists {
		return fmt.Errorf("database '%s' does not exist", key)
	}
	delete(c.databases, key)
	return nil
}

func (c *Catalog) GetDatabase(name string) (*Database, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := strings.ToUpper(name)
	db, ok := c.databases[key]
	if !ok {
		return nil, fmt.Errorf("database '%s' does not exist", key)
	}
	return db, nil
}

func (c *Catalog) ListDatabases() []*Database {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]*Database, 0, len(c.databases))
	for _, db := range c.databases {
		result = append(result, db)
	}
	return result
}

// --- Schema operations ---

func (c *Catalog) CreateSchema(dbName, schemaName, owner string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	dbKey := strings.ToUpper(dbName)
	db, ok := c.databases[dbKey]
	if !ok {
		return fmt.Errorf("database '%s' does not exist", dbKey)
	}
	sKey := strings.ToUpper(schemaName)
	if _, exists := db.Schemas[sKey]; exists {
		return fmt.Errorf("schema '%s' already exists in database '%s'", sKey, dbKey)
	}
	db.Schemas[sKey] = newSchema(sKey, owner)
	return nil
}

func (c *Catalog) DropSchema(dbName, schemaName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	dbKey := strings.ToUpper(dbName)
	db, ok := c.databases[dbKey]
	if !ok {
		return fmt.Errorf("database '%s' does not exist", dbKey)
	}
	sKey := strings.ToUpper(schemaName)
	if _, exists := db.Schemas[sKey]; !exists {
		return fmt.Errorf("schema '%s' does not exist in database '%s'", sKey, dbKey)
	}
	delete(db.Schemas, sKey)
	return nil
}

func (c *Catalog) GetSchema(dbName, schemaName string) (*Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbKey := strings.ToUpper(dbName)
	db, ok := c.databases[dbKey]
	if !ok {
		return nil, fmt.Errorf("database '%s' does not exist", dbKey)
	}
	sKey := strings.ToUpper(schemaName)
	s, ok := db.Schemas[sKey]
	if !ok {
		return nil, fmt.Errorf("schema '%s' does not exist in database '%s'", sKey, dbKey)
	}
	return s, nil
}

func (c *Catalog) ListSchemas(dbName string) ([]*Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbKey := strings.ToUpper(dbName)
	db, ok := c.databases[dbKey]
	if !ok {
		return nil, fmt.Errorf("database '%s' does not exist", dbKey)
	}
	result := make([]*Schema, 0, len(db.Schemas))
	for _, s := range db.Schemas {
		result = append(result, s)
	}
	return result, nil
}

// --- Table operations (metadata only) ---

func (c *Catalog) RegisterTable(dbName, schemaName string, table *TableMeta) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.getSchemaLocked(dbName, schemaName)
	if err != nil {
		return err
	}
	key := strings.ToUpper(table.Name)
	if _, exists := s.Tables[key]; exists {
		return fmt.Errorf("table '%s' already exists in '%s.%s'", key, strings.ToUpper(dbName), strings.ToUpper(schemaName))
	}
	table.Name = key
	if table.CreatedAt.IsZero() {
		table.CreatedAt = time.Now()
	}
	s.Tables[key] = table
	return nil
}

func (c *Catalog) DropTable(dbName, schemaName, tableName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.getSchemaLocked(dbName, schemaName)
	if err != nil {
		return err
	}
	key := strings.ToUpper(tableName)
	if _, exists := s.Tables[key]; !exists {
		return fmt.Errorf("table '%s' does not exist in '%s.%s'", key, strings.ToUpper(dbName), strings.ToUpper(schemaName))
	}
	delete(s.Tables, key)
	return nil
}

func (c *Catalog) GetTable(dbName, schemaName, tableName string) (*TableMeta, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, err := c.getSchemaRLocked(dbName, schemaName)
	if err != nil {
		return nil, err
	}
	key := strings.ToUpper(tableName)
	t, ok := s.Tables[key]
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist in '%s.%s'", key, strings.ToUpper(dbName), strings.ToUpper(schemaName))
	}
	return t, nil
}

func (c *Catalog) ListTables(dbName, schemaName string) ([]*TableMeta, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, err := c.getSchemaRLocked(dbName, schemaName)
	if err != nil {
		return nil, err
	}
	result := make([]*TableMeta, 0, len(s.Tables))
	for _, t := range s.Tables {
		result = append(result, t)
	}
	return result, nil
}

// --- Warehouse operations ---

func (c *Catalog) CreateWarehouse(name, size string, autoSuspend int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := strings.ToUpper(name)
	if _, exists := c.warehouses[key]; exists {
		return fmt.Errorf("warehouse '%s' already exists", key)
	}
	c.warehouses[key] = &Warehouse{
		Name:        key,
		Size:        size,
		State:       "STARTED",
		AutoSuspend: autoSuspend,
		CreatedAt:   time.Now(),
	}
	return nil
}

func (c *Catalog) GetWarehouse(name string) (*Warehouse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := strings.ToUpper(name)
	wh, ok := c.warehouses[key]
	if !ok {
		return nil, fmt.Errorf("warehouse '%s' does not exist", key)
	}
	return wh, nil
}

func (c *Catalog) ListWarehouses() []*Warehouse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]*Warehouse, 0, len(c.warehouses))
	for _, wh := range c.warehouses {
		result = append(result, wh)
	}
	return result
}

// --- SHOW command helpers ---

// ShowDatabases returns rows formatted like Snowflake's SHOW DATABASES output.
func (c *Catalog) ShowDatabases() [][]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rows := make([][]interface{}, 0, len(c.databases))
	for _, db := range c.databases {
		rows = append(rows, []interface{}{
			db.CreatedAt.Format(time.RFC3339),
			db.Name,
			"N",  // is_default
			"N",  // is_current
			db.Owner,
			"",   // comment
			"",   // options
			len(db.Schemas), // retention_time (placeholder: schema count)
		})
	}
	return rows
}

// ShowSchemas returns rows formatted like Snowflake's SHOW SCHEMAS output.
func (c *Catalog) ShowSchemas(dbName string) ([][]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbKey := strings.ToUpper(dbName)
	db, ok := c.databases[dbKey]
	if !ok {
		return nil, fmt.Errorf("database '%s' does not exist", dbKey)
	}
	rows := make([][]interface{}, 0, len(db.Schemas))
	for _, s := range db.Schemas {
		rows = append(rows, []interface{}{
			s.CreatedAt.Format(time.RFC3339),
			s.Name,
			"N", // is_default
			"N", // is_current
			dbKey,
			s.Owner,
			"", // comment
			"", // options
		})
	}
	return rows, nil
}

// ShowTables returns rows formatted like Snowflake's SHOW TABLES output.
func (c *Catalog) ShowTables(dbName, schemaName string) ([][]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, err := c.getSchemaRLocked(dbName, schemaName)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0, len(s.Tables))
	for _, t := range s.Tables {
		kind := "TABLE"
		if t.IsTransient {
			kind = "TRANSIENT"
		} else if t.IsTemporary {
			kind = "TEMPORARY"
		}
		rows = append(rows, []interface{}{
			t.CreatedAt.Format(time.RFC3339),
			t.Name,
			strings.ToUpper(dbName),
			strings.ToUpper(schemaName),
			kind,
			t.Comment,
			len(t.Columns),
			t.Owner,
		})
	}
	return rows, nil
}

// ShowColumns returns rows formatted like Snowflake's SHOW COLUMNS / DESCRIBE TABLE.
func (c *Catalog) ShowColumns(dbName, schemaName, tableName string) ([][]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, err := c.getSchemaRLocked(dbName, schemaName)
	if err != nil {
		return nil, err
	}
	tKey := strings.ToUpper(tableName)
	t, ok := s.Tables[tKey]
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist", tKey)
	}
	rows := make([][]interface{}, 0, len(t.Columns))
	for _, col := range t.Columns {
		rows = append(rows, []interface{}{
			t.Name,
			col.Name,
			col.Type,
			col.Nullable,
			col.Default,
			col.Comment,
		})
	}
	return rows, nil
}

// --- internal helpers ---

func newSchema(name, owner string) *Schema {
	return &Schema{
		Name:      name,
		Tables:    make(map[string]*TableMeta),
		Views:     make(map[string]*ViewMeta),
		Stages:    make(map[string]*StageMeta),
		Streams:   make(map[string]*StreamMeta),
		Tasks:     make(map[string]*TaskMeta),
		Pipes:     make(map[string]*PipeMeta),
		CreatedAt: time.Now(),
		Owner:     owner,
	}
}

// getSchemaLocked returns a schema; caller must hold c.mu (write lock).
func (c *Catalog) getSchemaLocked(dbName, schemaName string) (*Schema, error) {
	dbKey := strings.ToUpper(dbName)
	db, ok := c.databases[dbKey]
	if !ok {
		return nil, fmt.Errorf("database '%s' does not exist", dbKey)
	}
	sKey := strings.ToUpper(schemaName)
	s, ok := db.Schemas[sKey]
	if !ok {
		return nil, fmt.Errorf("schema '%s' does not exist in database '%s'", sKey, dbKey)
	}
	return s, nil
}

// getSchemaRLocked returns a schema; caller must hold c.mu (read lock).
func (c *Catalog) getSchemaRLocked(dbName, schemaName string) (*Schema, error) {
	return c.getSchemaLocked(dbName, schemaName)
}
