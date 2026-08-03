package snowpipe

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// PipeState represents the current state of a pipe.
type PipeState string

const (
	PipeRunning PipeState = "RUNNING"
	PipePaused  PipeState = "PAUSED"
	PipeStopped PipeState = "STOPPED_BY_SNOWFLAKE"
)

// Pipe represents a Snowpipe definition.
type Pipe struct {
	Name       string
	Database   string
	Schema     string
	CopyStmt   string // the COPY INTO statement
	AutoIngest bool
	State      PipeState
	CreatedAt  time.Time
}

// FileStatus tracks the load status of an individual file.
type FileStatus struct {
	Path          string
	Status        string // LOADED, LOAD_IN_PROGRESS, LOAD_FAILED, PARTIALLY_LOADED
	RowsInserted  int64
	ErrorMessage  string
	FirstErrorRow int64
	LoadedAt      *time.Time
}

// PipeInfo is a summary for SHOW PIPES output.
type PipeInfo struct {
	Name       string
	Database   string
	Schema     string
	Definition string
	State      PipeState
	CreatedAt  time.Time
}

// Engine manages Snowpipe operations.
type Engine struct {
	mu      sync.RWMutex
	pipes   map[string]*Pipe        // key: DB.SCHEMA.PIPE_NAME
	history map[string][]FileStatus // key: pipe key
	execFn  func(ctx context.Context, sql string) error
}

// NewEngine creates a new Snowpipe engine.
func NewEngine(execFn func(ctx context.Context, sql string) error) *Engine {
	return &Engine{
		pipes:   make(map[string]*Pipe),
		history: make(map[string][]FileStatus),
		execFn:  execFn,
	}
}

// Reset clears all pipes and ingest history.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pipes = make(map[string]*Pipe)
	e.history = make(map[string][]FileStatus)
}

// pipeKey builds the map key for a pipe.
func pipeKey(db, schema, name string) string {
	return fmt.Sprintf("%s.%s.%s",
		strings.ToUpper(db),
		strings.ToUpper(schema),
		strings.ToUpper(name),
	)
}

// CreatePipe creates a new pipe.
func (e *Engine) CreatePipe(db, schema, name, copyStmt string, autoIngest bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := pipeKey(db, schema, name)
	if _, exists := e.pipes[key]; exists {
		return fmt.Errorf("snowpipe: pipe '%s' already exists", key)
	}
	e.pipes[key] = &Pipe{
		Name:       strings.ToUpper(name),
		Database:   strings.ToUpper(db),
		Schema:     strings.ToUpper(schema),
		CopyStmt:   copyStmt,
		AutoIngest: autoIngest,
		State:      PipeRunning,
		CreatedAt:  time.Now(),
	}
	e.history[key] = nil
	return nil
}

// DropPipe removes a pipe.
func (e *Engine) DropPipe(db, schema, name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := pipeKey(db, schema, name)
	if _, exists := e.pipes[key]; !exists {
		return fmt.Errorf("snowpipe: pipe '%s' does not exist", key)
	}
	delete(e.pipes, key)
	delete(e.history, key)
	return nil
}

// GetPipe retrieves a pipe by name.
func (e *Engine) GetPipe(db, schema, name string) (*Pipe, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	key := pipeKey(db, schema, name)
	p, ok := e.pipes[key]
	if !ok {
		return nil, fmt.Errorf("snowpipe: pipe '%s' does not exist", key)
	}
	return p, nil
}

// AlterPipe changes the state of a pipe.
func (e *Engine) AlterPipe(db, schema, name string, state PipeState) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := pipeKey(db, schema, name)
	p, ok := e.pipes[key]
	if !ok {
		return fmt.Errorf("snowpipe: pipe '%s' does not exist", key)
	}
	p.State = state
	return nil
}

// InsertFiles triggers the pipe's COPY INTO statement for each file.
// It records load status for each file.
func (e *Engine) InsertFiles(ctx context.Context, db, schema, pipeName string, files []string) error {
	e.mu.RLock()
	key := pipeKey(db, schema, pipeName)
	p, ok := e.pipes[key]
	if !ok {
		e.mu.RUnlock()
		return fmt.Errorf("snowpipe: pipe '%s' does not exist", key)
	}
	if p.State != PipeRunning {
		e.mu.RUnlock()
		return fmt.Errorf("snowpipe: pipe '%s' is not running (state: %s)", key, p.State)
	}
	copyStmt := p.CopyStmt
	e.mu.RUnlock()

	var statuses []FileStatus
	for _, file := range files {
		// Replace a placeholder in the COPY statement with the actual file path.
		// Convention: the COPY statement can contain {FILE} as a placeholder.
		sql := strings.ReplaceAll(copyStmt, "{FILE}", file)

		status := FileStatus{
			Path:   file,
			Status: "LOAD_IN_PROGRESS",
		}

		if err := e.execFn(ctx, sql); err != nil {
			status.Status = "LOAD_FAILED"
			status.ErrorMessage = err.Error()
		} else {
			status.Status = "LOADED"
			now := time.Now()
			status.LoadedAt = &now
			// RowsInserted would be set by actual execution; we set 0 as placeholder.
		}
		statuses = append(statuses, status)
	}

	e.mu.Lock()
	e.history[key] = append(e.history[key], statuses...)
	e.mu.Unlock()
	return nil
}

// GetStatus returns the file load history for a pipe.
func (e *Engine) GetStatus(db, schema, pipeName string) []FileStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()
	key := pipeKey(db, schema, pipeName)
	h := e.history[key]
	out := make([]FileStatus, len(h))
	copy(out, h)
	return out
}

// ShowPipes returns summary info for all pipes in a schema.
func (e *Engine) ShowPipes(db, schema string) []PipeInfo {
	prefix := fmt.Sprintf("%s.%s.", strings.ToUpper(db), strings.ToUpper(schema))
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []PipeInfo
	for key, p := range e.pipes {
		if strings.HasPrefix(key, prefix) {
			result = append(result, PipeInfo{
				Name:       p.Name,
				Database:   p.Database,
				Schema:     p.Schema,
				Definition: p.CopyStmt,
				State:      p.State,
				CreatedAt:  p.CreatedAt,
			})
		}
	}
	return result
}
