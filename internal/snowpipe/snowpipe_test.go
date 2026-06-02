package snowpipe

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

type mockExec struct {
	executed []string
	failOn   string
}

func (m *mockExec) exec(ctx context.Context, sql string) error {
	m.executed = append(m.executed, sql)
	if m.failOn != "" && strings.Contains(sql, m.failOn) {
		return fmt.Errorf("mock error on: %s", m.failOn)
	}
	return nil
}

func TestCreateDropPipe(t *testing.T) {
	m := &mockExec{}
	e := NewEngine(m.exec)

	err := e.CreatePipe("mydb", "public", "my_pipe",
		"COPY INTO mydb.public.target FROM @mystage/{FILE}", true)
	if err != nil {
		t.Fatalf("CreatePipe: %v", err)
	}

	// Duplicate.
	err = e.CreatePipe("mydb", "public", "my_pipe", "COPY INTO x", false)
	if err == nil {
		t.Fatal("expected error on duplicate pipe")
	}

	// Get.
	p, err := e.GetPipe("mydb", "public", "my_pipe")
	if err != nil {
		t.Fatalf("GetPipe: %v", err)
	}
	if p.State != PipeRunning {
		t.Errorf("expected RUNNING, got %s", p.State)
	}
	if !p.AutoIngest {
		t.Error("expected AutoIngest true")
	}

	// Drop.
	err = e.DropPipe("mydb", "public", "my_pipe")
	if err != nil {
		t.Fatalf("DropPipe: %v", err)
	}

	// Drop nonexistent.
	err = e.DropPipe("mydb", "public", "my_pipe")
	if err == nil {
		t.Fatal("expected error dropping nonexistent pipe")
	}
}

func TestGetPipeNotFound(t *testing.T) {
	m := &mockExec{}
	e := NewEngine(m.exec)

	_, err := e.GetPipe("mydb", "public", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent pipe")
	}
}

func TestAlterPipe(t *testing.T) {
	m := &mockExec{}
	e := NewEngine(m.exec)

	_ = e.CreatePipe("mydb", "public", "my_pipe", "COPY INTO x", false)

	err := e.AlterPipe("mydb", "public", "my_pipe", PipePaused)
	if err != nil {
		t.Fatalf("AlterPipe: %v", err)
	}

	p, _ := e.GetPipe("mydb", "public", "my_pipe")
	if p.State != PipePaused {
		t.Errorf("expected PAUSED, got %s", p.State)
	}

	// Alter nonexistent.
	err = e.AlterPipe("mydb", "public", "nonexistent", PipePaused)
	if err == nil {
		t.Fatal("expected error altering nonexistent pipe")
	}
}

func TestInsertFiles(t *testing.T) {
	m := &mockExec{}
	e := NewEngine(m.exec)

	_ = e.CreatePipe("mydb", "public", "loader",
		"COPY INTO mydb.public.target FROM @stage/{FILE}", false)

	ctx := context.Background()
	err := e.InsertFiles(ctx, "mydb", "public", "loader", []string{"file1.csv", "file2.csv"})
	if err != nil {
		t.Fatalf("InsertFiles: %v", err)
	}

	if len(m.executed) != 2 {
		t.Fatalf("expected 2 COPY statements, got %d", len(m.executed))
	}
	if !strings.Contains(m.executed[0], "file1.csv") {
		t.Errorf("expected file1.csv in SQL, got: %s", m.executed[0])
	}

	statuses := e.GetStatus("mydb", "public", "loader")
	if len(statuses) != 2 {
		t.Fatalf("expected 2 file statuses, got %d", len(statuses))
	}
	for _, s := range statuses {
		if s.Status != "LOADED" {
			t.Errorf("expected LOADED, got %s", s.Status)
		}
	}
}

func TestInsertFilesWithFailure(t *testing.T) {
	m := &mockExec{failOn: "file2.csv"}
	e := NewEngine(m.exec)

	_ = e.CreatePipe("mydb", "public", "loader",
		"COPY INTO mydb.public.target FROM @stage/{FILE}", false)

	ctx := context.Background()
	err := e.InsertFiles(ctx, "mydb", "public", "loader", []string{"file1.csv", "file2.csv"})
	if err != nil {
		t.Fatalf("InsertFiles should not return error for individual file failures: %v", err)
	}

	statuses := e.GetStatus("mydb", "public", "loader")
	if statuses[0].Status != "LOADED" {
		t.Errorf("file1 should be LOADED, got %s", statuses[0].Status)
	}
	if statuses[1].Status != "LOAD_FAILED" {
		t.Errorf("file2 should be LOAD_FAILED, got %s", statuses[1].Status)
	}
	if statuses[1].ErrorMessage == "" {
		t.Error("expected error message for failed file")
	}
}

func TestInsertFilesPaused(t *testing.T) {
	m := &mockExec{}
	e := NewEngine(m.exec)

	_ = e.CreatePipe("mydb", "public", "loader", "COPY INTO x", false)
	_ = e.AlterPipe("mydb", "public", "loader", PipePaused)

	ctx := context.Background()
	err := e.InsertFiles(ctx, "mydb", "public", "loader", []string{"file1.csv"})
	if err == nil {
		t.Fatal("expected error inserting into paused pipe")
	}
}

func TestInsertFilesNotFound(t *testing.T) {
	m := &mockExec{}
	e := NewEngine(m.exec)

	ctx := context.Background()
	err := e.InsertFiles(ctx, "mydb", "public", "nonexistent", []string{"file1.csv"})
	if err == nil {
		t.Fatal("expected error for nonexistent pipe")
	}
}

func TestShowPipes(t *testing.T) {
	m := &mockExec{}
	e := NewEngine(m.exec)

	_ = e.CreatePipe("mydb", "public", "pipe1", "COPY INTO x", false)
	_ = e.CreatePipe("mydb", "public", "pipe2", "COPY INTO y", true)
	_ = e.CreatePipe("otherdb", "public", "pipe3", "COPY INTO z", false)

	pipes := e.ShowPipes("mydb", "public")
	if len(pipes) != 2 {
		t.Fatalf("expected 2 pipes, got %d", len(pipes))
	}
}

func TestGetStatusEmpty(t *testing.T) {
	m := &mockExec{}
	e := NewEngine(m.exec)

	_ = e.CreatePipe("mydb", "public", "loader", "COPY INTO x", false)
	statuses := e.GetStatus("mydb", "public", "loader")
	if len(statuses) != 0 {
		t.Errorf("expected 0 statuses, got %d", len(statuses))
	}
}
