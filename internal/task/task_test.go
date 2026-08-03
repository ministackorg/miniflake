package task

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCreateTask(t *testing.T) {
	s := NewScheduler(func(ctx context.Context, sql string) error { return nil })

	err := s.CreateTask("mydb", "public", "t1", "INSERT INTO t SELECT 1", "5 MINUTE", "WH1", "", "")
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Duplicate should fail.
	err = s.CreateTask("mydb", "public", "t1", "SELECT 1", "1 MINUTE", "", "", "")
	if err == nil {
		t.Fatal("expected error on duplicate task")
	}

	// Get it back.
	tk, err := s.GetTask("mydb", "public", "t1")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if tk.Name != "t1" || tk.State != TaskSuspended {
		t.Fatalf("unexpected task state: %+v", tk)
	}

	// ShowTasks
	infos := s.ShowTasks("mydb", "public")
	if len(infos) != 1 || infos[0].Name != "t1" {
		t.Fatalf("ShowTasks unexpected: %+v", infos)
	}

	// Alter to started.
	if err := s.AlterTask("mydb", "public", "t1", TaskStarted); err != nil {
		t.Fatalf("AlterTask: %v", err)
	}
	tk, _ = s.GetTask("mydb", "public", "t1")
	if tk.State != TaskStarted {
		t.Fatalf("expected started, got %s", tk.State)
	}

	// Drop.
	if err := s.DropTask("mydb", "public", "t1"); err != nil {
		t.Fatalf("DropTask: %v", err)
	}
	if err := s.DropTask("mydb", "public", "t1"); err == nil {
		t.Fatal("expected error dropping nonexistent task")
	}
}

func TestScheduleParsing(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
		minutes int
		isCron  bool
	}{
		{"5 MINUTE", false, 5, false},
		{"1 MINUTE", false, 1, false},
		{"60 MINUTES", false, 60, false},
		{"USING CRON 0 9 * * * UTC", false, 0, true},
		{"USING CRON */5 * * * * America/New_York", false, 0, true},
		{"USING CRON 0 9 * * * Atlantis", true, 0, false},
		{"INVALID SCHEDULE", true, 0, false},
		{"", true, 0, false},
	}

	for _, tt := range tests {
		si, err := ParseSchedule(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseSchedule(%q): expected error, got nil", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSchedule(%q): %v", tt.input, err)
			continue
		}
		if tt.minutes > 0 && si.Minutes != tt.minutes {
			t.Errorf("ParseSchedule(%q): minutes=%d, want %d", tt.input, si.Minutes, tt.minutes)
		}
		if tt.isCron && si.Cron == "" {
			t.Errorf("ParseSchedule(%q): expected cron expression", tt.input)
		}
		if tt.isCron && si.Timezone == "" {
			t.Errorf("ParseSchedule(%q): expected timezone", tt.input)
		}
	}
}

func TestScheduleTimezone(t *testing.T) {
	if got := ScheduleTimezone("USING CRON 0 9 * * * America/Los_Angeles"); got != "America/Los_Angeles" {
		t.Errorf("got %q", got)
	}
	if got := ScheduleTimezone("USING CRON 0 * * * *"); got != "UTC" {
		t.Errorf("cron without tz: got %q want UTC", got)
	}
	if got := ScheduleTimezone("5 MINUTE"); got != "" {
		t.Errorf("minute schedule: got %q want empty", got)
	}
}

func TestShowTasksPropagatesTimezone(t *testing.T) {
	s := NewScheduler(func(ctx context.Context, sql string) error { return nil })
	if err := s.CreateTask("db", "public", "tz_task", "SELECT 1",
		"USING CRON 0 9 * * * America/Los_Angeles", "wh", "", ""); err != nil {
		t.Fatal(err)
	}
	infos := s.ShowTasks("db", "public")
	if len(infos) != 1 {
		t.Fatalf("ShowTasks: %+v", infos)
	}
	if infos[0].Timezone != "America/Los_Angeles" {
		t.Errorf("Timezone=%q, want America/Los_Angeles (got raw schedule %q)",
			infos[0].Timezone, infos[0].Schedule)
	}
}

func TestDAGOrdering(t *testing.T) {
	s := NewScheduler(func(ctx context.Context, sql string) error { return nil })

	// Create a DAG: root -> child1 -> grandchild, root -> child2
	s.CreateTask("db", "public", "root", "SELECT 1", "1 MINUTE", "", "", "")
	s.CreateTask("db", "public", "child1", "SELECT 2", "", "", "root", "")
	s.CreateTask("db", "public", "child2", "SELECT 3", "", "", "root", "")
	s.CreateTask("db", "public", "grandchild", "SELECT 4", "", "", "child1", "")

	order := s.GetDependencyOrder("db", "public")
	if len(order) != 4 {
		t.Fatalf("expected 4 tasks in order, got %d: %v", len(order), order)
	}

	// Root must come before its children.
	pos := make(map[string]int)
	for i, name := range order {
		pos[name] = i
	}

	if pos["root"] >= pos["child1"] {
		t.Errorf("root should come before child1: %v", order)
	}
	if pos["root"] >= pos["child2"] {
		t.Errorf("root should come before child2: %v", order)
	}
	if pos["child1"] >= pos["grandchild"] {
		t.Errorf("child1 should come before grandchild: %v", order)
	}
}

func TestTaskExecution(t *testing.T) {
	var mu sync.Mutex
	var executed []string

	s := NewScheduler(func(ctx context.Context, sql string) error {
		mu.Lock()
		executed = append(executed, sql)
		mu.Unlock()
		return nil
	})

	s.CreateTask("db", "public", "t1", "INSERT INTO results SELECT 1", "1 MINUTE", "", "", "")
	s.AlterTask("db", "public", "t1", TaskStarted)

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	// Set NextRunAt to now so it triggers immediately.
	tk, _ := s.GetTask("db", "public", "t1")
	tk.mu.Lock()
	past := time.Now().Add(-1 * time.Second)
	tk.NextRunAt = &past
	tk.mu.Unlock()

	// Wait for execution.
	time.Sleep(2 * time.Second)
	cancel()
	s.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(executed) == 0 {
		t.Fatal("expected task to have executed at least once")
	}
	if executed[0] != "INSERT INTO results SELECT 1" {
		t.Fatalf("unexpected SQL executed: %s", executed[0])
	}
}
