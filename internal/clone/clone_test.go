package clone

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// mockExec records executed SQL and optionally returns errors.
type mockExec struct {
	executed []string
	failOn   string // if non-empty, fail when SQL contains this string
}

func (m *mockExec) exec(ctx context.Context, sql string) error {
	m.executed = append(m.executed, sql)
	if m.failOn != "" && strings.Contains(sql, m.failOn) {
		return fmt.Errorf("mock error on: %s", m.failOn)
	}
	return nil
}

func (m *mockExec) query(ctx context.Context, sql string) ([]string, [][]interface{}, error) {
	m.executed = append(m.executed, sql)
	// Return mock table/schema lists based on the query.
	if strings.Contains(sql, "duckdb_tables") {
		return []string{"table_name"}, [][]interface{}{
			{"users"},
			{"orders"},
		}, nil
	}
	if strings.Contains(sql, "duckdb_schemas") {
		return []string{"schema_name"}, [][]interface{}{
			{"public"},
		}, nil
	}
	return nil, nil, nil
}

func TestCloneTable(t *testing.T) {
	m := &mockExec{}
	eng := NewEngine(m.exec, m.query)
	ctx := context.Background()

	err := eng.CloneTable(ctx, "mydb", "public", "users", "mydb", "public", "users_copy")
	if err != nil {
		t.Fatalf("CloneTable: %v", err)
	}

	if len(m.executed) != 1 {
		t.Fatalf("expected 1 SQL, got %d", len(m.executed))
	}
	if !strings.Contains(m.executed[0], "CREATE TABLE") {
		t.Errorf("expected CREATE TABLE, got: %s", m.executed[0])
	}
	if !strings.Contains(m.executed[0], "SELECT * FROM") {
		t.Errorf("expected SELECT * FROM, got: %s", m.executed[0])
	}

	clones := eng.ListClones()
	if len(clones) != 1 {
		t.Fatalf("expected 1 clone record, got %d", len(clones))
	}
	if clones[0].Type != CloneTable {
		t.Errorf("expected CloneTable type, got %s", clones[0].Type)
	}
}

func TestCloneTableError(t *testing.T) {
	m := &mockExec{failOn: "CREATE TABLE"}
	eng := NewEngine(m.exec, m.query)
	ctx := context.Background()

	err := eng.CloneTable(ctx, "mydb", "public", "users", "mydb", "public", "users_copy")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "clone table") {
		t.Errorf("expected 'clone table' in error, got: %v", err)
	}

	if len(eng.ListClones()) != 0 {
		t.Error("expected no clone records on error")
	}
}

func TestCloneSchema(t *testing.T) {
	m := &mockExec{}
	eng := NewEngine(m.exec, m.query)
	ctx := context.Background()

	err := eng.CloneSchema(ctx, "mydb", "public", "mydb", "public_copy")
	if err != nil {
		t.Fatalf("CloneSchema: %v", err)
	}

	// Should have: CREATE SCHEMA + query tables + 2x CREATE TABLE (users, orders)
	clones := eng.ListClones()
	// 2 table clones + 1 schema clone
	if len(clones) != 3 {
		t.Fatalf("expected 3 clone records, got %d", len(clones))
	}

	hasSchema := false
	for _, c := range clones {
		if c.Type == CloneSchema {
			hasSchema = true
		}
	}
	if !hasSchema {
		t.Error("expected a CloneSchema record")
	}
}

func TestCloneDatabase(t *testing.T) {
	m := &mockExec{}
	eng := NewEngine(m.exec, m.query)
	ctx := context.Background()

	err := eng.CloneDatabase(ctx, "srcdb", "dstdb")
	if err != nil {
		t.Fatalf("CloneDatabase: %v", err)
	}

	clones := eng.ListClones()
	hasDB := false
	for _, c := range clones {
		if c.Type == CloneDatabase {
			hasDB = true
		}
	}
	if !hasDB {
		t.Error("expected a CloneDatabase record")
	}
}

func TestListClonesEmpty(t *testing.T) {
	m := &mockExec{}
	eng := NewEngine(m.exec, m.query)
	clones := eng.ListClones()
	if len(clones) != 0 {
		t.Errorf("expected 0 clones, got %d", len(clones))
	}
}
