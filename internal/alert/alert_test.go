package alert

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestCreateAlert(t *testing.T) {
	m := NewManager(
		func(ctx context.Context, sql string) error { return nil },
		func(ctx context.Context, sql string) ([]string, [][]interface{}, error) { return nil, nil, nil },
	)

	err := m.CreateAlert("DB1", "PUBLIC", "my_alert", "WH1", "5 MINUTE",
		"SELECT 1 FROM t WHERE x > 100", "INSERT INTO log VALUES('fired')")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Duplicate.
	err = m.CreateAlert("DB1", "PUBLIC", "my_alert", "WH1", "5 MINUTE", "SELECT 1", "SELECT 1")
	if err == nil {
		t.Fatal("expected error for duplicate alert")
	}

	// Show.
	alerts := m.ShowAlerts("DB1", "PUBLIC")
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	if alerts[0].State != AlertSuspended {
		t.Fatalf("expected suspended, got %s", alerts[0].State)
	}

	// Alter to started.
	err = m.AlterAlert("DB1", "PUBLIC", "my_alert", AlertStarted)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	alerts = m.ShowAlerts("DB1", "PUBLIC")
	if alerts[0].State != AlertStarted {
		t.Fatalf("expected started, got %s", alerts[0].State)
	}

	// Stop background goroutines.
	m.Stop()

	// Drop.
	err = m.DropAlert("DB1", "PUBLIC", "my_alert")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.ShowAlerts("", "")) != 0 {
		t.Fatal("expected 0 alerts after drop")
	}
}

func TestAlertExecution(t *testing.T) {
	var conditionCalls int64
	var actionCalls int64

	// Condition returns a row on every call — alert should always fire.
	queryFn := func(ctx context.Context, sql string) ([]string, [][]interface{}, error) {
		atomic.AddInt64(&conditionCalls, 1)
		return []string{"col"}, [][]interface{}{{"val"}}, nil
	}
	execFn := func(ctx context.Context, sql string) error {
		atomic.AddInt64(&actionCalls, 1)
		return nil
	}

	mgr := NewManager(execFn, queryFn)

	err := mgr.CreateAlert("DB", "SCH", "test_exec", "WH", "1 MINUTE",
		"SELECT 1 FROM src WHERE val > 0",
		"INSERT INTO dst SELECT * FROM src")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Directly invoke checkAndFire to test the logic without waiting for a ticker.
	key := alertKey("DB", "SCH", "test_exec")

	// Alert is suspended — should not fire.
	mgr.checkAndFire(key)
	if atomic.LoadInt64(&conditionCalls) != 0 {
		t.Fatal("suspended alert should not check condition")
	}

	// Start it.
	_ = mgr.AlterAlert("DB", "SCH", "test_exec", AlertStarted)
	mgr.Stop() // stop the background goroutine so we control timing

	// Re-set to started without goroutine for manual testing.
	mgr.mu.Lock()
	mgr.alerts[key].State = AlertStarted
	mgr.mu.Unlock()

	mgr.checkAndFire(key)

	if atomic.LoadInt64(&conditionCalls) != 1 {
		t.Fatalf("expected 1 condition call, got %d", atomic.LoadInt64(&conditionCalls))
	}
	if atomic.LoadInt64(&actionCalls) != 1 {
		t.Fatalf("expected 1 action call, got %d", atomic.LoadInt64(&actionCalls))
	}

	// Verify LastFired was set.
	mgr.mu.RLock()
	a := mgr.alerts[key]
	if a.LastFired == nil {
		t.Fatal("expected LastFired to be set")
	}
	if time.Since(*a.LastFired) > 5*time.Second {
		t.Fatal("LastFired seems too old")
	}
	mgr.mu.RUnlock()

	mgr.Stop()
}
