package task

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TaskState represents whether a task is running or suspended.
type TaskState string

const (
	TaskStarted   TaskState = "started"
	TaskSuspended TaskState = "suspended"
)

// Task represents a scheduled SQL task.
type Task struct {
	Name         string
	DatabaseName string
	SchemaName   string
	SQLText      string
	Schedule     string // CRON expression or "N MINUTE"
	Warehouse    string
	State        TaskState
	Predecessor  string // for DAG chains (AFTER task_name)
	When         string // WHEN condition
	CreatedAt    time.Time
	LastRunAt    *time.Time
	NextRunAt    *time.Time

	mu     sync.Mutex
	stopCh chan struct{}
}

// TaskInfo is the read-only metadata returned by ShowTasks.
type TaskInfo struct {
	Name         string
	DatabaseName string
	SchemaName   string
	Schedule     string
	State        TaskState
	Warehouse    string
	Predecessor  string
	CreatedAt    time.Time
	LastRunAt    *time.Time
	NextRunAt    *time.Time
}

// Scheduler manages tasks and their execution.
type Scheduler struct {
	mu     sync.RWMutex
	tasks  map[string]*Task // key: db.schema.task_name
	execFn func(ctx context.Context, sql string) error

	// streamHasDataFn is an optional callback for evaluating
	// SYSTEM$STREAM_HAS_DATA conditions.
	streamHasDataFn func(db, schema, streamName string) bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewScheduler creates a new Scheduler.
// execFn is called to execute a task's SQL statement.
func NewScheduler(execFn func(ctx context.Context, sql string) error) *Scheduler {
	return &Scheduler{
		tasks:  make(map[string]*Task),
		execFn: execFn,
	}
}

// SetStreamHasDataFn sets the callback used to evaluate SYSTEM$STREAM_HAS_DATA.
func (s *Scheduler) SetStreamHasDataFn(fn func(db, schema, streamName string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streamHasDataFn = fn
}

func taskKey(db, schema, name string) string {
	return strings.ToLower(fmt.Sprintf("%s.%s.%s", db, schema, name))
}

// CreateTask creates a new task in suspended state.
func (s *Scheduler) CreateTask(db, schema, name, sql, schedule, warehouse, predecessor, whenCond string) error {
	key := taskKey(db, schema, name)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[key]; exists {
		return fmt.Errorf("task: %q already exists", name)
	}

	// Validate schedule if provided.
	if schedule != "" && predecessor == "" {
		if _, err := ParseSchedule(schedule); err != nil {
			return fmt.Errorf("task: invalid schedule: %w", err)
		}
	}

	now := time.Now()
	s.tasks[key] = &Task{
		Name:         name,
		DatabaseName: db,
		SchemaName:   schema,
		SQLText:      sql,
		Schedule:     schedule,
		Warehouse:    warehouse,
		State:        TaskSuspended,
		Predecessor:  predecessor,
		When:         whenCond,
		CreatedAt:    now,
		stopCh:       make(chan struct{}),
	}
	return nil
}

// DropTask removes a task.
func (s *Scheduler) DropTask(db, schema, name string) error {
	key := taskKey(db, schema, name)

	s.mu.Lock()
	defer s.mu.Unlock()

	t, exists := s.tasks[key]
	if !exists {
		return fmt.Errorf("task: %q does not exist", name)
	}

	// Stop the task if running.
	t.mu.Lock()
	if t.State == TaskStarted {
		close(t.stopCh)
	}
	t.mu.Unlock()

	delete(s.tasks, key)
	return nil
}

// AlterTask changes a task's state (RESUME or SUSPEND).
func (s *Scheduler) AlterTask(db, schema, name string, state TaskState) error {
	key := taskKey(db, schema, name)

	s.mu.RLock()
	t, exists := s.tasks[key]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task: %q does not exist", name)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.State = state

	return nil
}

// GetTask returns the task metadata.
func (s *Scheduler) GetTask(db, schema, name string) (*Task, error) {
	key := taskKey(db, schema, name)

	s.mu.RLock()
	defer s.mu.RUnlock()

	t, exists := s.tasks[key]
	if !exists {
		return nil, fmt.Errorf("task: %q does not exist", name)
	}
	return t, nil
}

// ShowTasks returns metadata for all tasks in the given database and schema.
func (s *Scheduler) ShowTasks(db, schema string) []TaskInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dbLower := strings.ToLower(db)
	schemaLower := strings.ToLower(schema)

	var result []TaskInfo
	for _, t := range s.tasks {
		if strings.ToLower(t.DatabaseName) == dbLower && strings.ToLower(t.SchemaName) == schemaLower {
			t.mu.Lock()
			info := TaskInfo{
				Name:         t.Name,
				DatabaseName: t.DatabaseName,
				SchemaName:   t.SchemaName,
				Schedule:     t.Schedule,
				State:        t.State,
				Warehouse:    t.Warehouse,
				Predecessor:  t.Predecessor,
				CreatedAt:    t.CreatedAt,
				LastRunAt:    t.LastRunAt,
				NextRunAt:    t.NextRunAt,
			}
			t.mu.Unlock()
			result = append(result, info)
		}
	}
	return result
}

// Start begins the scheduler loop. It ticks every second and runs eligible tasks.
func (s *Scheduler) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.loop(ctx)
	}()
}

// Stop halts the scheduler and waits for the loop to exit.
func (s *Scheduler) Stop() {
	s.mu.RLock()
	cancel := s.cancel
	s.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
}

func (s *Scheduler) loop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.tick(ctx, now)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	s.mu.RLock()
	// Collect root tasks (those with schedules and no predecessors).
	var roots []*Task
	for _, t := range s.tasks {
		t.mu.Lock()
		isRoot := t.State == TaskStarted && t.Schedule != "" && t.Predecessor == ""
		t.mu.Unlock()
		if isRoot {
			roots = append(roots, t)
		}
	}
	s.mu.RUnlock()

	for _, t := range roots {
		t.mu.Lock()
		ready := false
		if t.NextRunAt == nil {
			// First run — compute next run from now.
			next := nextRunTime(t.Schedule, now)
			t.NextRunAt = &next
		} else if !now.Before(*t.NextRunAt) {
			ready = true
		}
		t.mu.Unlock()

		if ready {
			s.executeDAG(ctx, t, now)
		}
	}
}

// executeDAG runs a root task and then any successor tasks that depend on it.
func (s *Scheduler) executeDAG(ctx context.Context, root *Task, now time.Time) {
	if !s.evaluateWhen(root) {
		// Condition not met; skip this run but advance the schedule.
		root.mu.Lock()
		next := nextRunTime(root.Schedule, now)
		root.NextRunAt = &next
		root.mu.Unlock()
		return
	}

	s.runTask(ctx, root, now)

	// Find and run successors in order.
	s.runSuccessors(ctx, root.DatabaseName, root.SchemaName, root.Name, now)
}

func (s *Scheduler) runSuccessors(ctx context.Context, db, schema, predecessorName string, now time.Time) {
	s.mu.RLock()
	var successors []*Task
	predLower := strings.ToLower(predecessorName)
	for _, t := range s.tasks {
		t.mu.Lock()
		isSuccessor := t.State == TaskStarted &&
			strings.EqualFold(t.DatabaseName, db) &&
			strings.EqualFold(t.SchemaName, schema) &&
			strings.ToLower(t.Predecessor) == predLower
		t.mu.Unlock()
		if isSuccessor {
			successors = append(successors, t)
		}
	}
	s.mu.RUnlock()

	for _, t := range successors {
		if !s.evaluateWhen(t) {
			continue
		}
		s.runTask(ctx, t, now)
		// Recursively run successors of this task.
		t.mu.Lock()
		name := t.Name
		t.mu.Unlock()
		s.runSuccessors(ctx, db, schema, name, now)
	}
}

func (s *Scheduler) runTask(ctx context.Context, t *Task, now time.Time) {
	t.mu.Lock()
	sqlText := t.SQLText
	t.mu.Unlock()

	err := s.execFn(ctx, sqlText)
	nowCopy := now
	t.mu.Lock()
	t.LastRunAt = &nowCopy
	if t.Schedule != "" {
		next := nextRunTime(t.Schedule, now)
		t.NextRunAt = &next
	}
	t.mu.Unlock()

	if err != nil {
		log.Printf("task %s.%s.%s: execution error: %v", t.DatabaseName, t.SchemaName, t.Name, err)
	}
}

// evaluateWhen checks the WHEN condition for a task.
func (s *Scheduler) evaluateWhen(t *Task) bool {
	t.mu.Lock()
	when := t.When
	db := t.DatabaseName
	schema := t.SchemaName
	t.mu.Unlock()

	if when == "" {
		return true
	}

	// Support SYSTEM$STREAM_HAS_DATA('stream_name')
	re := regexp.MustCompile(`(?i)SYSTEM\$STREAM_HAS_DATA\(\s*'([^']+)'\s*\)`)
	matches := re.FindStringSubmatch(when)
	if matches != nil {
		streamName := matches[1]
		s.mu.RLock()
		fn := s.streamHasDataFn
		s.mu.RUnlock()
		if fn != nil {
			return fn(db, schema, streamName)
		}
		return false
	}

	// Unknown condition — default to true.
	return true
}

// ScheduleInterval represents a parsed schedule.
type ScheduleInterval struct {
	Minutes int    // for "N MINUTE" schedules
	Cron    string // raw cron expression
}

var minuteRe = regexp.MustCompile(`(?i)^(\d+)\s+MINUTES?$`)

// ParseSchedule parses a Snowflake schedule string.
// Supports "N MINUTE" and "USING CRON <expr> <tz>" formats.
func ParseSchedule(schedule string) (*ScheduleInterval, error) {
	schedule = strings.TrimSpace(schedule)

	// Check for "N MINUTE" format.
	if m := minuteRe.FindStringSubmatch(schedule); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid minute interval: %q", schedule)
		}
		return &ScheduleInterval{Minutes: n}, nil
	}

	// Check for CRON format: "USING CRON <expr> <timezone>"
	upper := strings.ToUpper(schedule)
	if strings.HasPrefix(upper, "USING CRON ") {
		cronPart := strings.TrimPrefix(schedule, schedule[:len("USING CRON ")])
		cronPart = strings.TrimSpace(cronPart)
		if cronPart == "" {
			return nil, fmt.Errorf("empty cron expression")
		}
		return &ScheduleInterval{Cron: cronPart}, nil
	}

	return nil, fmt.Errorf("unrecognized schedule format: %q", schedule)
}

// nextRunTime computes the next run time based on the schedule and the current
// time. Returns now+1min for unparseable schedules so a malformed expression
// doesn't stall the scheduler — the task's run will likely error, surfacing
// the misconfiguration without taking the loop down.
func nextRunTime(schedule string, now time.Time) time.Time {
	si, err := ParseSchedule(schedule)
	if err != nil {
		return now.Add(1 * time.Minute)
	}
	if si.Minutes > 0 {
		return now.Add(time.Duration(si.Minutes) * time.Minute)
	}
	if si.Cron != "" {
		next, err := nextCronTime(si.Cron, now)
		if err != nil {
			return now.Add(1 * time.Minute)
		}
		return next
	}
	return now.Add(1 * time.Minute)
}

// GetDependencyOrder returns task names topologically sorted for the given
// database and schema. Root tasks come first, followed by their successors.
func (s *Scheduler) GetDependencyOrder(db, schema string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dbLower := strings.ToLower(db)
	schemaLower := strings.ToLower(schema)

	// Build adjacency: predecessor -> []successor. inDegree doubles as the
	// node set for Kahn's algorithm, so we don't need a separate allNames.
	adj := make(map[string][]string)
	inDegree := make(map[string]int)

	for _, t := range s.tasks {
		if strings.ToLower(t.DatabaseName) != dbLower || strings.ToLower(t.SchemaName) != schemaLower {
			continue
		}
		nameLower := strings.ToLower(t.Name)
		if _, ok := inDegree[nameLower]; !ok {
			inDegree[nameLower] = 0
		}
		if t.Predecessor != "" {
			predLower := strings.ToLower(t.Predecessor)
			adj[predLower] = append(adj[predLower], nameLower)
			inDegree[nameLower]++
			if _, ok := inDegree[predLower]; !ok {
				inDegree[predLower] = 0
			}
		}
	}

	// Kahn's algorithm.
	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var order []string
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		order = append(order, curr)
		for _, succ := range adj[curr] {
			inDegree[succ]--
			if inDegree[succ] == 0 {
				queue = append(queue, succ)
			}
		}
	}

	return order
}
