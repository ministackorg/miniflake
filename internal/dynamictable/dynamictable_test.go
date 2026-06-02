package dynamictable

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockEngine tracks executed SQL for testing.
type mockEngine struct {
	mu       sync.Mutex
	executed []string
}

func (e *mockEngine) exec(_ context.Context, sql string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executed = append(e.executed, sql)
	return nil
}

func (e *mockEngine) query(_ context.Context, sql string) ([]string, [][]interface{}, error) {
	return []string{"col1"}, [][]interface{}{{"value1"}}, nil
}

func (e *mockEngine) getExecuted() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make([]string, len(e.executed))
	copy(cp, e.executed)
	return cp
}

func TestCreate(t *testing.T) {
	eng := &mockEngine{}
	m := NewManager(eng.exec, eng.query)
	defer m.Stop()

	err := m.Create("DB1", "PUBLIC", "SALES_DT", "SELECT * FROM SALES", "WH1", 5*time.Minute, "FULL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify CREATE TABLE was executed.
	executed := eng.getExecuted()
	if len(executed) < 1 {
		t.Fatal("expected at least one SQL execution")
	}
	if !strings.Contains(executed[0], "CREATE TABLE") {
		t.Errorf("expected CREATE TABLE, got: %s", executed[0])
	}

	// Get should return the table.
	dt, err := m.Get("DB1", "PUBLIC", "SALES_DT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dt.Name != "SALES_DT" {
		t.Errorf("expected name 'SALES_DT', got '%s'", dt.Name)
	}
	if dt.RefreshMode != "FULL" {
		t.Errorf("expected refresh mode 'FULL', got '%s'", dt.RefreshMode)
	}
	if dt.LastRefresh == nil {
		t.Error("expected LastRefresh to be set")
	}

	// Duplicate should fail.
	err = m.Create("DB1", "PUBLIC", "SALES_DT", "SELECT 1", "WH1", time.Minute, "FULL")
	if err == nil {
		t.Fatal("expected error for duplicate dynamic table")
	}

	// Show should list the table.
	tables := m.Show("DB1", "PUBLIC")
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}

	// Get non-existent should fail.
	if _, err := m.Get("DB1", "PUBLIC", "NOPE"); err == nil {
		t.Fatal("expected error for non-existent table")
	}
}

func TestRefresh(t *testing.T) {
	eng := &mockEngine{}
	m := NewManager(eng.exec, eng.query)
	defer m.Stop()

	err := m.Create("DB1", "PUBLIC", "REFRESH_DT", "SELECT * FROM SRC", "WH1", 10*time.Minute, "FULL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Manual refresh.
	err = m.Refresh(context.Background(), "DB1", "PUBLIC", "REFRESH_DT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify DELETE + INSERT were executed.
	executed := eng.getExecuted()
	foundDelete := false
	foundInsert := false
	for _, sql := range executed {
		if strings.Contains(sql, "DELETE FROM") {
			foundDelete = true
		}
		if strings.Contains(sql, "INSERT INTO") {
			foundInsert = true
		}
	}
	if !foundDelete {
		t.Error("expected DELETE FROM during refresh")
	}
	if !foundInsert {
		t.Error("expected INSERT INTO during refresh")
	}

	// Verify LastRefresh was updated.
	dt, _ := m.Get("DB1", "PUBLIC", "REFRESH_DT")
	if dt.LastRefresh == nil {
		t.Error("expected LastRefresh to be set after refresh")
	}

	// Refresh non-existent should fail.
	if err := m.Refresh(context.Background(), "DB1", "PUBLIC", "NOPE"); err == nil {
		t.Fatal("expected error for non-existent table")
	}
}

func TestDrop(t *testing.T) {
	eng := &mockEngine{}
	m := NewManager(eng.exec, eng.query)
	defer m.Stop()

	err := m.Create("DB1", "PUBLIC", "DROP_DT", "SELECT 1", "WH1", time.Minute, "FULL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Drop.
	if err := m.Drop("DB1", "PUBLIC", "DROP_DT"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify DROP TABLE was executed.
	executed := eng.getExecuted()
	foundDrop := false
	for _, sql := range executed {
		if strings.Contains(sql, "DROP TABLE") {
			foundDrop = true
		}
	}
	if !foundDrop {
		t.Error("expected DROP TABLE during drop")
	}

	// Get should fail after drop.
	if _, err := m.Get("DB1", "PUBLIC", "DROP_DT"); err == nil {
		t.Fatal("expected error for dropped table")
	}

	// Drop non-existent should fail.
	if err := m.Drop("DB1", "PUBLIC", "NOPE"); err == nil {
		t.Fatal("expected error for non-existent table")
	}
}
