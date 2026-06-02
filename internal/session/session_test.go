package session

import (
	"strings"
	"testing"
	"time"
)

func TestCreateSession(t *testing.T) {
	m := NewManager()
	defer m.Stop()

	s := m.CreateSession("alice", "TESTDB", "PUBLIC", "COMPUTE_WH", "SYSADMIN")

	if s.User != "alice" {
		t.Fatalf("User = %q, want %q", s.User, "alice")
	}
	if s.Database != "TESTDB" {
		t.Fatalf("Database = %q, want %q", s.Database, "TESTDB")
	}
	if s.Schema != "PUBLIC" {
		t.Fatalf("Schema = %q, want %q", s.Schema, "PUBLIC")
	}
	if s.Warehouse != "COMPUTE_WH" {
		t.Fatalf("Warehouse = %q, want %q", s.Warehouse, "COMPUTE_WH")
	}
	if s.Role != "SYSADMIN" {
		t.Fatalf("Role = %q, want %q", s.Role, "SYSADMIN")
	}
	if s.Token == "" {
		t.Fatal("Token should not be empty")
	}
	if s.ID == "" {
		t.Fatal("ID should not be empty")
	}
	if s.Token == s.ID {
		t.Fatal("Token and ID should differ")
	}
}

func TestGetSession(t *testing.T) {
	m := NewManager()
	defer m.Stop()

	s := m.CreateSession("bob", "DB1", "S1", "WH1", "ROLE1")

	got, ok := m.GetSession(s.Token)
	if !ok {
		t.Fatal("expected session to be found")
	}
	if got.ID != s.ID {
		t.Fatalf("ID = %q, want %q", got.ID, s.ID)
	}

	_, ok = m.GetSession("nonexistent-token")
	if ok {
		t.Fatal("expected session not to be found for bogus token")
	}
}

func TestDeleteSession(t *testing.T) {
	m := NewManager()
	defer m.Stop()

	s := m.CreateSession("carol", "DB2", "S2", "WH2", "ROLE2")
	m.DeleteSession(s.Token)

	_, ok := m.GetSession(s.Token)
	if ok {
		t.Fatal("expected session to be deleted")
	}
}

func TestUpdateActivity(t *testing.T) {
	m := NewManager()
	defer m.Stop()

	s := m.CreateSession("dave", "DB3", "S3", "WH3", "ROLE3")
	original := s.LastActiveAt

	// Small sleep to ensure time advances.
	time.Sleep(5 * time.Millisecond)
	m.UpdateActivity(s.Token)

	got, ok := m.GetSession(s.Token)
	if !ok {
		t.Fatal("session should exist")
	}
	if !got.LastActiveAt.After(original) {
		t.Fatal("LastActiveAt should have been updated")
	}
}

func TestUpdateActivityNonexistent(t *testing.T) {
	m := NewManager()
	defer m.Stop()
	// Should not panic.
	m.UpdateActivity("does-not-exist")
}

func TestUUIDFormat(t *testing.T) {
	id := generateUUID()
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Fatalf("UUID should have 5 parts, got %d: %q", len(parts), id)
	}
	// Check version nibble is 4.
	if parts[2][0] != '4' {
		t.Fatalf("UUID version should be 4, got %q", parts[2])
	}
}

func TestMultipleSessions(t *testing.T) {
	m := NewManager()
	defer m.Stop()

	tokens := make(map[string]bool)
	for i := 0; i < 100; i++ {
		s := m.CreateSession("user", "DB", "SCH", "WH", "ROLE")
		if tokens[s.Token] {
			t.Fatalf("duplicate token generated: %s", s.Token)
		}
		tokens[s.Token] = true
	}
}

func TestDeleteSessionNonexistent(t *testing.T) {
	m := NewManager()
	defer m.Stop()
	// Should not panic.
	m.DeleteSession("nonexistent")
}
