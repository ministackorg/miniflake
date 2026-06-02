package tag

import (
	"testing"
)

func TestCreateTag(t *testing.T) {
	m := NewManager()

	err := m.CreateTag("DB1", "PUBLIC", "ENV", nil, "environment tag")
	if err != nil {
		t.Fatalf("CreateTag failed: %v", err)
	}

	// Duplicate should fail.
	err = m.CreateTag("DB1", "PUBLIC", "ENV", nil, "")
	if err == nil {
		t.Fatal("expected error on duplicate tag creation")
	}

	// Show should list it.
	tags := m.ShowTags("DB1", "PUBLIC")
	if len(tags) != 1 {
		t.Fatalf("expected 1 tag, got %d", len(tags))
	}
	if tags[0].Comment != "environment tag" {
		t.Fatalf("expected comment 'environment tag', got %q", tags[0].Comment)
	}

	// Drop and verify.
	err = m.DropTag("DB1", "PUBLIC", "ENV")
	if err != nil {
		t.Fatalf("DropTag failed: %v", err)
	}
	tags = m.ShowTags("DB1", "PUBLIC")
	if len(tags) != 0 {
		t.Fatalf("expected 0 tags after drop, got %d", len(tags))
	}

	// Drop non-existent.
	err = m.DropTag("DB1", "PUBLIC", "ENV")
	if err == nil {
		t.Fatal("expected error dropping non-existent tag")
	}
}

func TestSetUnsetTag(t *testing.T) {
	m := NewManager()
	_ = m.CreateTag("DB1", "PUBLIC", "COST_CENTER", nil, "")

	// Set tag on a table.
	err := m.SetTag("DB1", "PUBLIC", "COST_CENTER", "TABLE", "ORDERS", "", "finance")
	if err != nil {
		t.Fatalf("SetTag failed: %v", err)
	}

	// Retrieve value.
	val, err := m.GetTagValue("TABLE", "ORDERS", "", "COST_CENTER")
	if err != nil {
		t.Fatalf("GetTagValue failed: %v", err)
	}
	if val != "finance" {
		t.Fatalf("expected 'finance', got %q", val)
	}

	// Overwrite with new value.
	err = m.SetTag("DB1", "PUBLIC", "COST_CENTER", "TABLE", "ORDERS", "", "engineering")
	if err != nil {
		t.Fatalf("SetTag overwrite failed: %v", err)
	}
	val, _ = m.GetTagValue("TABLE", "ORDERS", "", "COST_CENTER")
	if val != "engineering" {
		t.Fatalf("expected 'engineering', got %q", val)
	}

	// Unset.
	err = m.UnsetTag("DB1", "PUBLIC", "COST_CENTER", "TABLE", "ORDERS", "")
	if err != nil {
		t.Fatalf("UnsetTag failed: %v", err)
	}

	// Should be gone.
	_, err = m.GetTagValue("TABLE", "ORDERS", "", "COST_CENTER")
	if err == nil {
		t.Fatal("expected error after unset")
	}

	// Unset again should fail.
	err = m.UnsetTag("DB1", "PUBLIC", "COST_CENTER", "TABLE", "ORDERS", "")
	if err == nil {
		t.Fatal("expected error on double unset")
	}

	// Set on non-existent tag.
	err = m.SetTag("DB1", "PUBLIC", "NOPE", "TABLE", "ORDERS", "", "val")
	if err == nil {
		t.Fatal("expected error setting non-existent tag")
	}
}

func TestAllowedValues(t *testing.T) {
	m := NewManager()
	_ = m.CreateTag("DB1", "PUBLIC", "ENV", []string{"dev", "staging", "prod"}, "")

	// Valid value.
	err := m.SetTag("DB1", "PUBLIC", "ENV", "TABLE", "T1", "", "dev")
	if err != nil {
		t.Fatalf("SetTag with allowed value failed: %v", err)
	}

	// Case-insensitive match.
	err = m.SetTag("DB1", "PUBLIC", "ENV", "TABLE", "T2", "", "PROD")
	if err != nil {
		t.Fatalf("SetTag with case-insensitive allowed value failed: %v", err)
	}

	// Invalid value.
	err = m.SetTag("DB1", "PUBLIC", "ENV", "TABLE", "T3", "", "qa")
	if err == nil {
		t.Fatal("expected error for value not in allowed list")
	}
}

func TestGetAllTags(t *testing.T) {
	m := NewManager()
	_ = m.CreateTag("DB1", "PUBLIC", "ENV", nil, "")
	_ = m.CreateTag("DB1", "PUBLIC", "COST_CENTER", nil, "")
	_ = m.CreateTag("DB1", "PUBLIC", "TEAM", nil, "")

	_ = m.SetTag("DB1", "PUBLIC", "ENV", "TABLE", "ORDERS", "", "prod")
	_ = m.SetTag("DB1", "PUBLIC", "COST_CENTER", "TABLE", "ORDERS", "", "finance")
	_ = m.SetTag("DB1", "PUBLIC", "TEAM", "TABLE", "ORDERS", "", "data-eng")

	tags := m.GetAllTags("TABLE", "ORDERS")
	if len(tags) != 3 {
		t.Fatalf("expected 3 tags, got %d", len(tags))
	}

	// Verify values.
	found := map[string]string{}
	for _, ta := range tags {
		found[ta.TagName] = ta.TagValue
	}
	if found["ENV"] != "prod" {
		t.Fatalf("expected ENV=prod, got %q", found["ENV"])
	}
	if found["COST_CENTER"] != "finance" {
		t.Fatalf("expected COST_CENTER=finance, got %q", found["COST_CENTER"])
	}
	if found["TEAM"] != "data-eng" {
		t.Fatalf("expected TEAM=data-eng, got %q", found["TEAM"])
	}

	// Drop one tag and verify it's removed from assignments too.
	_ = m.DropTag("DB1", "PUBLIC", "ENV")
	tags = m.GetAllTags("TABLE", "ORDERS")
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags after drop, got %d", len(tags))
	}
}
