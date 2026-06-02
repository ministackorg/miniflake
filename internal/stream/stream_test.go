package stream

import (
	"testing"
)

func TestCreateStream(t *testing.T) {
	e := NewEngine()

	// Create a standard stream.
	if err := e.CreateStream("mydb", "public", "mystream", "orders", "STANDARD"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// Duplicate should fail.
	if err := e.CreateStream("mydb", "public", "mystream", "orders", "STANDARD"); err == nil {
		t.Fatal("expected error on duplicate stream creation")
	}

	// Get the stream back.
	s, err := e.GetStream("mydb", "public", "mystream")
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	if s.Name != "mystream" || s.TableName != "orders" || s.Type != "STANDARD" {
		t.Fatalf("unexpected stream: %+v", s)
	}

	// ShowStreams should list it.
	infos := e.ShowStreams("mydb", "public")
	if len(infos) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(infos))
	}

	// Drop it.
	if err := e.DropStream("mydb", "public", "mystream"); err != nil {
		t.Fatalf("DropStream: %v", err)
	}

	// Drop again should fail.
	if err := e.DropStream("mydb", "public", "mystream"); err == nil {
		t.Fatal("expected error dropping nonexistent stream")
	}
}

func TestRecordAndConsume(t *testing.T) {
	e := NewEngine()
	if err := e.CreateStream("db1", "public", "s1", "users", "STANDARD"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// Record several changes.
	e.RecordChange("db1", "public", "users", ChangeRecord{
		Action: ChangeInsert,
		RowID:  "row-1",
		Data:   map[string]interface{}{"id": 1, "name": "alice"},
	})
	e.RecordChange("db1", "public", "users", ChangeRecord{
		Action:   ChangeUpdate,
		IsUpdate: true,
		RowID:    "row-1",
		Data:     map[string]interface{}{"id": 1, "name": "alice2"},
	})
	e.RecordChange("db1", "public", "users", ChangeRecord{
		Action: ChangeDelete,
		RowID:  "row-1",
		Data:   map[string]interface{}{"id": 1},
	})

	// Consume should return all three.
	records, err := e.ConsumeStream("db1", "public", "s1")
	if err != nil {
		t.Fatalf("ConsumeStream: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[0].Action != ChangeInsert {
		t.Errorf("expected INSERT, got %s", records[0].Action)
	}
	if records[1].Action != ChangeUpdate || !records[1].IsUpdate {
		t.Errorf("expected UPDATE with IsUpdate=true, got %s/%v", records[1].Action, records[1].IsUpdate)
	}
	if records[2].Action != ChangeDelete {
		t.Errorf("expected DELETE, got %s", records[2].Action)
	}

	// Consume again should return nothing (offset advanced).
	records, err = e.ConsumeStream("db1", "public", "s1")
	if err != nil {
		t.Fatalf("ConsumeStream: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records after second consume, got %d", len(records))
	}
}

func TestHasData(t *testing.T) {
	e := NewEngine()
	if err := e.CreateStream("db1", "public", "s1", "events", "STANDARD"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// No data yet.
	if e.HasData("db1", "public", "s1") {
		t.Fatal("expected HasData=false before any changes")
	}

	// Record a change.
	e.RecordChange("db1", "public", "events", ChangeRecord{
		Action: ChangeInsert,
		RowID:  "r1",
		Data:   map[string]interface{}{"x": 1},
	})
	if !e.HasData("db1", "public", "s1") {
		t.Fatal("expected HasData=true after recording a change")
	}

	// Consume.
	if _, err := e.ConsumeStream("db1", "public", "s1"); err != nil {
		t.Fatalf("ConsumeStream: %v", err)
	}
	if e.HasData("db1", "public", "s1") {
		t.Fatal("expected HasData=false after consuming")
	}

	// Non-existent stream returns false.
	if e.HasData("db1", "public", "nonexistent") {
		t.Fatal("expected HasData=false for nonexistent stream")
	}
}

func TestAppendOnlyStream(t *testing.T) {
	e := NewEngine()
	if err := e.CreateStream("db1", "public", "append_s", "logs", "APPEND_ONLY"); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// Record INSERT, UPDATE, DELETE.
	e.RecordChange("db1", "public", "logs", ChangeRecord{Action: ChangeInsert, RowID: "r1"})
	e.RecordChange("db1", "public", "logs", ChangeRecord{Action: ChangeUpdate, RowID: "r1"})
	e.RecordChange("db1", "public", "logs", ChangeRecord{Action: ChangeDelete, RowID: "r1"})

	// Only INSERT should be captured.
	records, err := e.ConsumeStream("db1", "public", "append_s")
	if err != nil {
		t.Fatalf("ConsumeStream: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record (INSERT only), got %d", len(records))
	}
	if records[0].Action != ChangeInsert {
		t.Errorf("expected INSERT, got %s", records[0].Action)
	}
}
