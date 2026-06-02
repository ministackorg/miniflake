package alert

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AlertState represents whether an alert is actively running or suspended.
type AlertState string

const (
	AlertStarted   AlertState = "started"
	AlertSuspended AlertState = "suspended"
)

// Alert represents a Snowflake alert object.
type Alert struct {
	Name      string
	Database  string
	Schema    string
	Warehouse string
	Schedule  string // CRON expression or "N MINUTE"
	Condition string // SQL that returns rows when the alert should fire
	Action    string // SQL to execute when the alert fires
	State     AlertState
	CreatedAt time.Time
	LastFired *time.Time
}

// AlertInfo is the read-only representation returned by ShowAlerts.
type AlertInfo struct {
	Name      string
	Database  string
	Schema    string
	Warehouse string
	Schedule  string
	Condition string
	Action    string
	State     AlertState
	CreatedAt time.Time
	LastFired *time.Time
}

// Manager manages alert objects.
type Manager struct {
	mu      sync.RWMutex
	alerts  map[string]*Alert // key: DB.SCHEMA.NAME
	execFn  func(ctx context.Context, sql string) error
	queryFn func(ctx context.Context, sql string) ([]string, [][]interface{}, error)
	stopChs map[string]chan struct{}
	done    chan struct{} // closed when Stop() is called
}

// NewManager creates a Manager with the provided execution functions.
func NewManager(
	execFn func(ctx context.Context, sql string) error,
	queryFn func(ctx context.Context, sql string) ([]string, [][]interface{}, error),
) *Manager {
	return &Manager{
		alerts:  make(map[string]*Alert),
		execFn:  execFn,
		queryFn: queryFn,
		stopChs: make(map[string]chan struct{}),
		done:    make(chan struct{}),
	}
}

func alertKey(db, schema, name string) string {
	return strings.ToUpper(db) + "." + strings.ToUpper(schema) + "." + strings.ToUpper(name)
}

// CreateAlert creates a new alert in suspended state.
func (m *Manager) CreateAlert(db, schema, name, warehouse, schedule, condition, action string) error {
	key := alertKey(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.alerts[key]; exists {
		return fmt.Errorf("alert %q already exists", key)
	}

	m.alerts[key] = &Alert{
		Name:      name,
		Database:  db,
		Schema:    schema,
		Warehouse: warehouse,
		Schedule:  schedule,
		Condition: condition,
		Action:    action,
		State:     AlertSuspended,
		CreatedAt: time.Now(),
	}
	return nil
}

// DropAlert removes an alert, stopping it if running.
func (m *Manager) DropAlert(db, schema, name string) error {
	key := alertKey(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.alerts[key]; !exists {
		return fmt.Errorf("alert %q does not exist", key)
	}

	if ch, ok := m.stopChs[key]; ok {
		close(ch)
		delete(m.stopChs, key)
	}
	delete(m.alerts, key)
	return nil
}

// AlterAlert changes the state of an alert (started or suspended).
func (m *Manager) AlterAlert(db, schema, name string, state AlertState) error {
	key := alertKey(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	a, exists := m.alerts[key]
	if !exists {
		return fmt.Errorf("alert %q does not exist", key)
	}

	oldState := a.State
	a.State = state

	if state == AlertStarted && oldState != AlertStarted {
		m.startAlertLocked(key, a)
	} else if state == AlertSuspended && oldState == AlertStarted {
		if ch, ok := m.stopChs[key]; ok {
			close(ch)
			delete(m.stopChs, key)
		}
	}

	return nil
}

// ShowAlerts returns alerts matching the given database and schema.
// Empty strings match all.
func (m *Manager) ShowAlerts(db, schema string) []AlertInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []AlertInfo
	for _, a := range m.alerts {
		if db != "" && !strings.EqualFold(a.Database, db) {
			continue
		}
		if schema != "" && !strings.EqualFold(a.Schema, schema) {
			continue
		}
		result = append(result, AlertInfo{
			Name:      a.Name,
			Database:  a.Database,
			Schema:    a.Schema,
			Warehouse: a.Warehouse,
			Schedule:  a.Schedule,
			Condition: a.Condition,
			Action:    a.Action,
			State:     a.State,
			CreatedAt: a.CreatedAt,
			LastFired: a.LastFired,
		})
	}
	return result
}

// Start begins executing all started alerts on their schedules.
// It is a no-op for alerts in suspended state.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, a := range m.alerts {
		if a.State == AlertStarted {
			if _, running := m.stopChs[key]; !running {
				m.startAlertLocked(key, a)
			}
		}
	}
}

// Stop stops all running alert goroutines.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, ch := range m.stopChs {
		close(ch)
		delete(m.stopChs, key)
	}
}

// startAlertLocked starts the ticker goroutine for an alert.
// Must be called with m.mu held.
func (m *Manager) startAlertLocked(key string, a *Alert) {
	interval := parseSchedule(a.Schedule)
	if interval <= 0 {
		interval = time.Minute
	}

	stopCh := make(chan struct{})
	m.stopChs[key] = stopCh

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				m.checkAndFire(key)
			}
		}
	}()
}

// checkAndFire runs the condition query and fires the action if rows are returned.
func (m *Manager) checkAndFire(key string) {
	m.mu.RLock()
	a, exists := m.alerts[key]
	if !exists || a.State != AlertStarted {
		m.mu.RUnlock()
		return
	}
	condition := a.Condition
	action := a.Action
	m.mu.RUnlock()

	ctx := context.Background()
	_, rows, err := m.queryFn(ctx, condition)
	if err != nil || len(rows) == 0 {
		return
	}

	// Condition returned rows — fire the action.
	_ = m.execFn(ctx, action)

	now := time.Now()
	m.mu.Lock()
	if a, ok := m.alerts[key]; ok {
		a.LastFired = &now
	}
	m.mu.Unlock()
}

// parseSchedule parses "N MINUTE" schedules. Returns the interval duration.
// For CRON schedules, falls back to 1 minute (simplified).
func parseSchedule(schedule string) time.Duration {
	parts := strings.Fields(strings.TrimSpace(schedule))
	if len(parts) == 2 && strings.EqualFold(parts[1], "MINUTE") {
		if n, err := strconv.Atoi(parts[0]); err == nil && n > 0 {
			return time.Duration(n) * time.Minute
		}
	}
	// Fallback for CRON or unparseable — 1 minute.
	return time.Minute
}
