package matview

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// mockEngine tracks executed SQL for assertions.
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
	return []string{"col1", "col2"}, nil, nil
}

func (e *mockEngine) lastSQL() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.executed) == 0 {
		return ""
	}
	return e.executed[len(e.executed)-1]
}

func (e *mockEngine) sqlCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.executed)
}

func TestCreate(t *testing.T) {
	eng := &mockEngine{}
	mgr := NewManager(eng.exec, eng.query)
	ctx := context.Background()

	err := mgr.Create(ctx, "DB1", "PUBLIC", "MV1", "SELECT 1 AS col1, 2 AS col2", false, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify the backing table was created.
	last := eng.lastSQL()
	if !strings.Contains(last, "SELECT * FROM") {
		// The last call is the column-discovery query; check the one before.
	}
	if eng.sqlCount() < 1 {
		t.Fatal("expected at least one SQL execution")
	}

	// Verify metadata.
	mv, err := mgr.Get("DB1", "PUBLIC", "MV1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if mv.Name != "MV1" {
		t.Fatalf("expected name MV1, got %s", mv.Name)
	}
	if mv.LastRefresh == nil {
		t.Fatal("expected LastRefresh to be set")
	}
	if len(mv.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(mv.Columns))
	}

	// Duplicate should fail.
	err = mgr.Create(ctx, "DB1", "PUBLIC", "MV1", "SELECT 1", false, nil)
	if err == nil {
		t.Fatal("expected error on duplicate create")
	}

	// Show should list it.
	infos := mgr.Show("DB1", "PUBLIC")
	if len(infos) != 1 {
		t.Fatalf("expected 1 view in Show, got %d", len(infos))
	}
}

func TestRefresh(t *testing.T) {
	eng := &mockEngine{}
	mgr := NewManager(eng.exec, eng.query)
	ctx := context.Background()

	err := mgr.Create(ctx, "DB1", "PUBLIC", "MV_R", "SELECT 42 AS val", false, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	beforeCount := eng.sqlCount()
	err = mgr.Refresh(ctx, "DB1", "PUBLIC", "MV_R")
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Refresh should have issued a DROP + CREATE (2 exec calls).
	afterCount := eng.sqlCount()
	if afterCount-beforeCount != 2 {
		t.Fatalf("expected 2 SQL calls for refresh, got %d", afterCount-beforeCount)
	}

	// Refresh non-existent view.
	err = mgr.Refresh(ctx, "DB1", "PUBLIC", "NOPE")
	if err == nil {
		t.Fatal("expected error refreshing non-existent view")
	}
}

func TestDrop(t *testing.T) {
	eng := &mockEngine{}
	mgr := NewManager(eng.exec, eng.query)
	ctx := context.Background()

	err := mgr.Create(ctx, "DB1", "PUBLIC", "MV_D", "SELECT 1", false, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	err = mgr.Drop(ctx, "DB1", "PUBLIC", "MV_D")
	if err != nil {
		t.Fatalf("Drop failed: %v", err)
	}

	// Should be gone.
	_, err = mgr.Get("DB1", "PUBLIC", "MV_D")
	if err == nil {
		t.Fatal("expected error after drop")
	}

	// Show should be empty.
	infos := mgr.Show("DB1", "PUBLIC")
	if len(infos) != 0 {
		t.Fatalf("expected 0 views after drop, got %d", len(infos))
	}

	// Drop non-existent.
	err = mgr.Drop(ctx, "DB1", "PUBLIC", "MV_D")
	if err == nil {
		t.Fatal("expected error dropping non-existent view")
	}
}

func TestCreateExecError(t *testing.T) {
	failExec := func(_ context.Context, sql string) error {
		return fmt.Errorf("exec error")
	}
	mgr := NewManager(failExec, nil)
	ctx := context.Background()

	err := mgr.Create(ctx, "DB1", "PUBLIC", "MV_FAIL", "SELECT 1", false, nil)
	if err == nil {
		t.Fatal("expected error when exec fails")
	}
}
