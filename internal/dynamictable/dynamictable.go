package dynamictable

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// DynamicTable is an auto-refreshing materialized view.
type DynamicTable struct {
	Name        string
	Database    string
	Schema      string
	Query       string
	TargetLag   time.Duration
	Warehouse   string
	RefreshMode string // AUTO, FULL, INCREMENTAL
	LastRefresh *time.Time
	NextRefresh *time.Time
	RowCount    int64
	CreatedAt   time.Time
}

// DynamicTableInfo is the read-only metadata returned by Show.
type DynamicTableInfo struct {
	Name        string
	Database    string
	Schema      string
	Query       string
	TargetLag   time.Duration
	Warehouse   string
	RefreshMode string
	LastRefresh *time.Time
	NextRefresh *time.Time
	RowCount    int64
	CreatedAt   time.Time
}

// Manager manages dynamic tables and their background refresh loops.
type Manager struct {
	mu      sync.RWMutex
	tables  map[string]*DynamicTable // key: DB.SCHEMA.NAME
	execFn  func(ctx context.Context, sql string) error
	queryFn func(ctx context.Context, sql string) ([]string, [][]interface{}, error)
	stopChs map[string]chan struct{}
}

// tableKey returns the map key for a dynamic table.
func tableKey(db, schema, name string) string {
	return strings.ToUpper(fmt.Sprintf("%s.%s.%s", db, schema, name))
}

// NewManager creates a new dynamic table manager.
// execFn is used to execute DDL/DML statements.
// queryFn is used to run SELECT queries and return columns + rows.
func NewManager(
	execFn func(ctx context.Context, sql string) error,
	queryFn func(ctx context.Context, sql string) ([]string, [][]interface{}, error),
) *Manager {
	return &Manager{
		tables:  make(map[string]*DynamicTable),
		execFn:  execFn,
		queryFn: queryFn,
		stopChs: make(map[string]chan struct{}),
	}
}

// Create creates a dynamic table backed by the given query and starts a background refresh loop.
func (m *Manager) Create(db, schema, name, query, warehouse string, targetLag time.Duration, refreshMode string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tableKey(db, schema, name)
	if _, exists := m.tables[key]; exists {
		return fmt.Errorf("dynamic table '%s' already exists", key)
	}

	fqn := fmt.Sprintf("%s.%s.%s", db, schema, name)
	createSQL := fmt.Sprintf("CREATE TABLE %s AS %s", fqn, query)
	if err := m.execFn(context.Background(), createSQL); err != nil {
		return fmt.Errorf("failed to create dynamic table: %w", err)
	}

	now := time.Now()
	next := now.Add(targetLag)
	dt := &DynamicTable{
		Name:        name,
		Database:    db,
		Schema:      schema,
		Query:       query,
		TargetLag:   targetLag,
		Warehouse:   warehouse,
		RefreshMode: refreshMode,
		LastRefresh: &now,
		NextRefresh: &next,
		CreatedAt:   now,
	}
	m.tables[key] = dt

	stopCh := make(chan struct{})
	m.stopChs[key] = stopCh
	go m.refreshLoop(key, stopCh)

	return nil
}

// refreshLoop runs periodic refreshes for a dynamic table.
func (m *Manager) refreshLoop(key string, stopCh chan struct{}) {
	for {
		m.mu.RLock()
		dt, exists := m.tables[key]
		if !exists {
			m.mu.RUnlock()
			return
		}
		lag := dt.TargetLag
		m.mu.RUnlock()

		select {
		case <-stopCh:
			return
		case <-time.After(lag):
			_ = m.Refresh(context.Background(), "", "", key)
		}
	}
}

// Drop stops the refresh loop and drops the dynamic table.
func (m *Manager) Drop(db, schema, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tableKey(db, schema, name)
	dt, exists := m.tables[key]
	if !exists {
		return fmt.Errorf("dynamic table '%s' does not exist", key)
	}

	// Stop the refresh goroutine.
	if ch, ok := m.stopChs[key]; ok {
		close(ch)
		delete(m.stopChs, key)
	}

	fqn := fmt.Sprintf("%s.%s.%s", dt.Database, dt.Schema, dt.Name)
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", fqn)
	if err := m.execFn(context.Background(), dropSQL); err != nil {
		return fmt.Errorf("failed to drop dynamic table: %w", err)
	}

	delete(m.tables, key)
	return nil
}

// Refresh manually refreshes a dynamic table.
// If db/schema are empty, key is used directly as the table key.
func (m *Manager) Refresh(ctx context.Context, db, schema, name string) error {
	var key string
	if db == "" && schema == "" {
		key = name // already a key
	} else {
		key = tableKey(db, schema, name)
	}

	m.mu.RLock()
	dt, exists := m.tables[key]
	if !exists {
		m.mu.RUnlock()
		return fmt.Errorf("dynamic table '%s' does not exist", key)
	}
	query := dt.Query
	fqn := fmt.Sprintf("%s.%s.%s", dt.Database, dt.Schema, dt.Name)
	lag := dt.TargetLag
	m.mu.RUnlock()

	// Full refresh: drop and recreate from query.
	deleteSQL := fmt.Sprintf("DELETE FROM %s", fqn)
	if err := m.execFn(ctx, deleteSQL); err != nil {
		return fmt.Errorf("refresh failed (delete): %w", err)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s %s", fqn, query)
	if err := m.execFn(ctx, insertSQL); err != nil {
		return fmt.Errorf("refresh failed (insert): %w", err)
	}

	now := time.Now()
	next := now.Add(lag)

	m.mu.Lock()
	if dt, ok := m.tables[key]; ok {
		dt.LastRefresh = &now
		dt.NextRefresh = &next
	}
	m.mu.Unlock()

	return nil
}

// Get returns a copy of a dynamic table's metadata.
func (m *Manager) Get(db, schema, name string) (*DynamicTable, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := tableKey(db, schema, name)
	dt, exists := m.tables[key]
	if !exists {
		return nil, fmt.Errorf("dynamic table '%s' does not exist", key)
	}

	cp := *dt
	return &cp, nil
}

// Show returns metadata for all dynamic tables in the given database and schema.
func (m *Manager) Show(db, schema string) []DynamicTableInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := strings.ToUpper(fmt.Sprintf("%s.%s.", db, schema))
	var result []DynamicTableInfo
	for key, dt := range m.tables {
		if strings.HasPrefix(key, prefix) {
			result = append(result, DynamicTableInfo{
				Name:        dt.Name,
				Database:    dt.Database,
				Schema:      dt.Schema,
				Query:       dt.Query,
				TargetLag:   dt.TargetLag,
				Warehouse:   dt.Warehouse,
				RefreshMode: dt.RefreshMode,
				LastRefresh: dt.LastRefresh,
				NextRefresh: dt.NextRefresh,
				RowCount:    dt.RowCount,
				CreatedAt:   dt.CreatedAt,
			})
		}
	}
	return result
}

// Start starts refresh loops for all existing dynamic tables.
func (m *Manager) Start(_ context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key := range m.tables {
		if _, running := m.stopChs[key]; !running {
			stopCh := make(chan struct{})
			m.stopChs[key] = stopCh
			go m.refreshLoop(key, stopCh)
		}
	}
}

// Stop stops all background refresh loops.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, ch := range m.stopChs {
		close(ch)
		delete(m.stopChs, key)
	}
}
