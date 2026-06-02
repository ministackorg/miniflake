package matview

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// MaterializedView represents a Snowflake materialized view.
type MaterializedView struct {
	Name        string
	Database    string
	Schema      string
	Query       string // the SELECT statement
	Columns     []string
	LastRefresh *time.Time
	IsSecure    bool
	ClusterBy   []string
	CreatedAt   time.Time
}

// MaterializedViewInfo is the read-only representation returned by Show.
type MaterializedViewInfo struct {
	Name        string
	Database    string
	Schema      string
	Query       string
	IsSecure    bool
	ClusterBy   []string
	LastRefresh *time.Time
	CreatedAt   time.Time
}

// Manager manages all materialized views.
type Manager struct {
	mu      sync.RWMutex
	views   map[string]*MaterializedView // key: DB.SCHEMA.NAME
	execFn  func(ctx context.Context, sql string) error
	queryFn func(ctx context.Context, sql string) ([]string, [][]interface{}, error)

	cancelAutoRefresh context.CancelFunc
}

func qualifiedName(db, schema, name string) string {
	return strings.ToUpper(db) + "." + strings.ToUpper(schema) + "." + strings.ToUpper(name)
}

func qualifiedTableName(db, schema, name string) string {
	return strings.ToUpper(db) + "." + strings.ToUpper(schema) + "." + strings.ToUpper(name)
}

// NewManager creates a new materialized view manager.
// execFn executes DDL/DML statements. queryFn executes SELECT statements and returns columns + rows.
func NewManager(
	execFn func(ctx context.Context, sql string) error,
	queryFn func(ctx context.Context, sql string) ([]string, [][]interface{}, error),
) *Manager {
	return &Manager{
		views:   make(map[string]*MaterializedView),
		execFn:  execFn,
		queryFn: queryFn,
	}
}

// Create creates a materialized view by executing CREATE TABLE ... AS (query).
func (m *Manager) Create(ctx context.Context, db, schema, name, query string, isSecure bool, clusterBy []string) error {
	key := qualifiedName(db, schema, name)
	tableName := qualifiedTableName(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.views[key]; exists {
		return fmt.Errorf("materialized view %s already exists", key)
	}

	sql := fmt.Sprintf("CREATE TABLE %s AS (%s)", tableName, query)
	if err := m.execFn(ctx, sql); err != nil {
		return fmt.Errorf("failed to materialize view: %w", err)
	}

	// Discover columns via queryFn.
	var columns []string
	if m.queryFn != nil {
		cols, _, err := m.queryFn(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName))
		if err == nil {
			columns = cols
		}
	}

	now := time.Now()
	m.views[key] = &MaterializedView{
		Name:        strings.ToUpper(name),
		Database:    strings.ToUpper(db),
		Schema:      strings.ToUpper(schema),
		Query:       query,
		Columns:     columns,
		LastRefresh: &now,
		IsSecure:    isSecure,
		ClusterBy:   clusterBy,
		CreatedAt:   now,
	}
	return nil
}

// Drop removes a materialized view and its backing table.
func (m *Manager) Drop(ctx context.Context, db, schema, name string) error {
	key := qualifiedName(db, schema, name)
	tableName := qualifiedTableName(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.views[key]; !exists {
		return fmt.Errorf("materialized view %s does not exist", key)
	}

	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if err := m.execFn(ctx, sql); err != nil {
		return fmt.Errorf("failed to drop backing table: %w", err)
	}

	delete(m.views, key)
	return nil
}

// Refresh performs a full refresh: drops and recreates the backing table.
func (m *Manager) Refresh(ctx context.Context, db, schema, name string) error {
	key := qualifiedName(db, schema, name)
	tableName := qualifiedTableName(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	mv, exists := m.views[key]
	if !exists {
		return fmt.Errorf("materialized view %s does not exist", key)
	}

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if err := m.execFn(ctx, dropSQL); err != nil {
		return fmt.Errorf("failed to drop table during refresh: %w", err)
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s AS (%s)", tableName, mv.Query)
	if err := m.execFn(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to recreate table during refresh: %w", err)
	}

	now := time.Now()
	mv.LastRefresh = &now
	return nil
}

// Get returns a materialized view by name.
func (m *Manager) Get(db, schema, name string) (*MaterializedView, error) {
	key := qualifiedName(db, schema, name)

	m.mu.RLock()
	defer m.mu.RUnlock()

	mv, exists := m.views[key]
	if !exists {
		return nil, fmt.Errorf("materialized view %s does not exist", key)
	}
	return mv, nil
}

// Show returns info for all materialized views in the given database and schema.
func (m *Manager) Show(db, schema string) []MaterializedViewInfo {
	prefix := strings.ToUpper(db) + "." + strings.ToUpper(schema) + "."

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []MaterializedViewInfo
	for key, mv := range m.views {
		if strings.HasPrefix(key, prefix) {
			result = append(result, MaterializedViewInfo{
				Name:        mv.Name,
				Database:    mv.Database,
				Schema:      mv.Schema,
				Query:       mv.Query,
				IsSecure:    mv.IsSecure,
				ClusterBy:   mv.ClusterBy,
				LastRefresh: mv.LastRefresh,
				CreatedAt:   mv.CreatedAt,
			})
		}
	}
	return result
}

// StartAutoRefresh starts a background goroutine that refreshes all views at the given interval.
// Call the returned cancel function or cancel the context to stop it.
func (m *Manager) StartAutoRefresh(ctx context.Context, interval time.Duration) {
	ctx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	if m.cancelAutoRefresh != nil {
		m.cancelAutoRefresh()
	}
	m.cancelAutoRefresh = cancel
	m.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.mu.RLock()
				keys := make([]struct{ db, schema, name string }, 0, len(m.views))
				for _, mv := range m.views {
					keys = append(keys, struct{ db, schema, name string }{mv.Database, mv.Schema, mv.Name})
				}
				m.mu.RUnlock()

				for _, k := range keys {
					_ = m.Refresh(ctx, k.db, k.schema, k.name)
				}
			}
		}
	}()
}
