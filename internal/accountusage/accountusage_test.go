package accountusage

import (
	"testing"
	"time"
)

func TestRecordAndGetQueryHistory(t *testing.T) {
	s := NewStore(100)

	now := time.Now()
	s.RecordQuery(QueryHistory{
		QueryID:         "q1",
		QueryText:       "SELECT 1",
		DatabaseName:    "DB1",
		SchemaName:      "PUBLIC",
		UserName:        "ADMIN",
		Status:          "SUCCESS",
		StartTime:       now,
		EndTime:         now.Add(10 * time.Millisecond),
		BytesScanned:    1024,
		RowsProduced:    1,
		ExecutionTimeMs: 10,
	})
	s.RecordQuery(QueryHistory{
		QueryID:      "q2",
		QueryText:    "SELECT 2",
		DatabaseName: "DB2",
		UserName:     "ADMIN",
		Status:       "FAIL",
		StartTime:    now,
		EndTime:      now.Add(5 * time.Millisecond),
	})
	s.RecordQuery(QueryHistory{
		QueryID:      "q3",
		QueryText:    "SELECT 3",
		DatabaseName: "DB1",
		UserName:     "USER1",
		Status:       "SUCCESS",
		StartTime:    now,
		EndTime:      now.Add(1 * time.Millisecond),
	})

	// Get all — newest first.
	all := s.GetQueryHistory(0, nil)
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}
	if all[0].QueryID != "q3" {
		t.Fatalf("expected newest first, got %s", all[0].QueryID)
	}

	// Filter by database.
	db1 := s.GetQueryHistory(0, map[string]string{"DATABASE_NAME": "DB1"})
	if len(db1) != 2 {
		t.Fatalf("expected 2 DB1 queries, got %d", len(db1))
	}

	// Filter by status.
	fails := s.GetQueryHistory(0, map[string]string{"STATUS": "FAIL"})
	if len(fails) != 1 || fails[0].QueryID != "q2" {
		t.Fatalf("expected 1 FAIL query q2, got %d", len(fails))
	}

	// Limit.
	limited := s.GetQueryHistory(1, nil)
	if len(limited) != 1 {
		t.Fatalf("expected 1, got %d", len(limited))
	}

	// AsQueryResult
	cols, rows := s.AsQueryResult("QUERY_HISTORY", 2)
	if len(cols) == 0 {
		t.Fatal("expected columns")
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestRecordLogin(t *testing.T) {
	s := NewStore(100)

	s.RecordLogin(LoginHistory{
		EventTimestamp: time.Now(),
		UserName:       "ADMIN",
		ClientIP:       "127.0.0.1",
		IsSuccess:      true,
	})
	s.RecordLogin(LoginHistory{
		EventTimestamp: time.Now(),
		UserName:       "BADUSER",
		ClientIP:       "10.0.0.1",
		IsSuccess:      false,
		ErrorCode:      "390100",
		ErrorMessage:   "Invalid credentials",
	})

	entries := s.GetLoginHistory(10)
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
	// Newest first.
	if entries[0].UserName != "BADUSER" {
		t.Fatalf("expected BADUSER first, got %s", entries[0].UserName)
	}

	cols, rows := s.AsQueryResult("LOGIN_HISTORY", 10)
	if len(cols) != 6 {
		t.Fatalf("expected 6 cols, got %d", len(cols))
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestMaxHistory(t *testing.T) {
	s := NewStore(5)

	for i := 0; i < 10; i++ {
		s.RecordQuery(QueryHistory{
			QueryID:   string(rune('A' + i)),
			QueryText: "SELECT 1",
			Status:    "SUCCESS",
			StartTime: time.Now(),
			EndTime:   time.Now(),
		})
	}

	all := s.GetQueryHistory(0, nil)
	if len(all) != 5 {
		t.Fatalf("expected 5 after eviction, got %d", len(all))
	}

	// Same for login history.
	for i := 0; i < 10; i++ {
		s.RecordLogin(LoginHistory{
			EventTimestamp: time.Now(),
			UserName:       "user",
			IsSuccess:      true,
		})
	}
	logins := s.GetLoginHistory(0)
	if len(logins) != 5 {
		t.Fatalf("expected 5 logins after eviction, got %d", len(logins))
	}

	// Same for storage.
	for i := 0; i < 10; i++ {
		s.RecordStorage(StorageUsage{
			UsageDate:    time.Now(),
			DatabaseName: "DB",
			AverageBytes: int64(i * 100),
		})
	}
	usage := s.GetStorageUsage(time.Time{}, time.Now().Add(24*time.Hour))
	if len(usage) != 5 {
		t.Fatalf("expected 5 storage entries after eviction, got %d", len(usage))
	}
}
