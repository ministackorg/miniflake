package sharing

import (
	"testing"
)

func TestCreateShare(t *testing.T) {
	m := NewManager()

	// Create a share.
	if err := m.CreateShare("test_share", "test comment"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify it exists.
	s, err := m.DescribeShare("test_share")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "test_share" {
		t.Errorf("expected name 'test_share', got '%s'", s.Name)
	}
	if s.Type != ShareOutbound {
		t.Errorf("expected type OUTBOUND, got '%s'", s.Type)
	}
	if s.Comment != "test comment" {
		t.Errorf("expected comment 'test comment', got '%s'", s.Comment)
	}

	// Duplicate should fail.
	if err := m.CreateShare("test_share", ""); err == nil {
		t.Fatal("expected error for duplicate share")
	}

	// Empty name should fail.
	if err := m.CreateShare("", ""); err == nil {
		t.Fatal("expected error for empty name")
	}

	// Drop and verify.
	if err := m.DropShare("test_share"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := m.DescribeShare("test_share"); err == nil {
		t.Fatal("expected error for dropped share")
	}

	// Drop non-existent should fail.
	if err := m.DropShare("nonexistent"); err == nil {
		t.Fatal("expected error for non-existent share")
	}

	// ShowShares should be empty after drop.
	if shares := m.ShowShares(); len(shares) != 0 {
		t.Errorf("expected 0 shares, got %d", len(shares))
	}
}

func TestGrantRevoke(t *testing.T) {
	m := NewManager()
	if err := m.CreateShare("data_share", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Grant a table.
	if err := m.GrantToShare("data_share", "TABLE", "DB1.PUBLIC.CUSTOMERS"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Grant a view.
	if err := m.GrantToShare("data_share", "VIEW", "DB1.PUBLIC.ORDERS_V"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Duplicate grant should fail.
	if err := m.GrantToShare("data_share", "TABLE", "DB1.PUBLIC.CUSTOMERS"); err == nil {
		t.Fatal("expected error for duplicate grant")
	}

	// Verify objects.
	s, _ := m.DescribeShare("data_share")
	if len(s.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(s.Objects))
	}

	// Revoke the table.
	if err := m.RevokeFromShare("data_share", "TABLE", "DB1.PUBLIC.CUSTOMERS"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s, _ = m.DescribeShare("data_share")
	if len(s.Objects) != 1 {
		t.Fatalf("expected 1 object after revoke, got %d", len(s.Objects))
	}
	if s.Objects[0].Name != "DB1.PUBLIC.ORDERS_V" {
		t.Errorf("expected remaining object to be the view, got '%s'", s.Objects[0].Name)
	}

	// Revoke non-existent should fail.
	if err := m.RevokeFromShare("data_share", "TABLE", "NONEXISTENT"); err == nil {
		t.Fatal("expected error for revoking non-existent object")
	}

	// Grant/revoke on non-existent share should fail.
	if err := m.GrantToShare("nope", "TABLE", "X"); err == nil {
		t.Fatal("expected error")
	}
	if err := m.RevokeFromShare("nope", "TABLE", "X"); err == nil {
		t.Fatal("expected error")
	}
}

func TestAddRemoveAccounts(t *testing.T) {
	m := NewManager()
	if err := m.CreateShare("acct_share", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Add accounts.
	if err := m.AlterShareAddAccounts("acct_share", []string{"ACCT1", "ACCT2"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s, _ := m.DescribeShare("acct_share")
	if len(s.Accounts) != 2 {
		t.Fatalf("expected 2 accounts, got %d", len(s.Accounts))
	}

	// Adding duplicates should not create extras.
	if err := m.AlterShareAddAccounts("acct_share", []string{"ACCT2", "ACCT3"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s, _ = m.DescribeShare("acct_share")
	if len(s.Accounts) != 3 {
		t.Fatalf("expected 3 accounts after dedup add, got %d", len(s.Accounts))
	}

	// Remove one account.
	if err := m.AlterShareRemoveAccounts("acct_share", []string{"ACCT1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s, _ = m.DescribeShare("acct_share")
	if len(s.Accounts) != 2 {
		t.Fatalf("expected 2 accounts after remove, got %d", len(s.Accounts))
	}

	// Operations on non-existent share should fail.
	if err := m.AlterShareAddAccounts("nope", []string{"X"}); err == nil {
		t.Fatal("expected error")
	}
	if err := m.AlterShareRemoveAccounts("nope", []string{"X"}); err == nil {
		t.Fatal("expected error")
	}

	// Test CreateDatabaseFromShare.
	if err := m.CreateShare("provider_share", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = m.GrantToShare("provider_share", "TABLE", "SRC_DB.PUBLIC.T1")
	if err := m.CreateDatabaseFromShare("provider_share", "CONSUMER_DB"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The inbound share should exist.
	inbound, err := m.DescribeShare("CONSUMER_DB_from_provider_share")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inbound.Type != ShareInbound {
		t.Errorf("expected INBOUND type, got '%s'", inbound.Type)
	}
	if inbound.Database != "CONSUMER_DB" {
		t.Errorf("expected database 'CONSUMER_DB', got '%s'", inbound.Database)
	}

	// Duplicate should fail.
	if err := m.CreateDatabaseFromShare("provider_share", "CONSUMER_DB"); err == nil {
		t.Fatal("expected error for duplicate database from share")
	}

	// From non-existent share should fail.
	if err := m.CreateDatabaseFromShare("nope", "X"); err == nil {
		t.Fatal("expected error")
	}
}
