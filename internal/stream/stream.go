package stream

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ChangeType represents the type of DML change captured by a stream.
type ChangeType string

const (
	ChangeInsert ChangeType = "INSERT"
	ChangeDelete ChangeType = "DELETE"
	ChangeUpdate ChangeType = "UPDATE"
)

// ChangeRecord represents a single change captured by a stream.
type ChangeRecord struct {
	Action    ChangeType             // METADATA$ACTION
	IsUpdate  bool                   // METADATA$ISUPDATE
	RowID     string                 // METADATA$ROW_ID
	Data      map[string]interface{} // actual row data
	Timestamp time.Time
}

// Stream tracks DML changes on a source table.
type Stream struct {
	Name         string
	DatabaseName string
	SchemaName   string
	TableName    string // source table
	Type         string // STANDARD, APPEND_ONLY
	Stale        bool
	StaleAfter   time.Time
	Offset       int64 // current consumption offset
	CreatedAt    time.Time

	mu      sync.Mutex
	changes []ChangeRecord
}

// StreamInfo is the read-only metadata returned by ShowStreams.
type StreamInfo struct {
	Name         string
	DatabaseName string
	SchemaName   string
	TableName    string
	Type         string
	Stale        bool
	CreatedAt    time.Time
}

// Engine manages streams and their change records.
type Engine struct {
	mu      sync.RWMutex
	streams map[string]*Stream // key: db.schema.stream_name
}

// NewEngine creates a new stream Engine.
func NewEngine() *Engine {
	return &Engine{
		streams: make(map[string]*Stream),
	}
}

// Reset clears all stream state.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.streams = make(map[string]*Stream)
}

func streamKey(db, schema, name string) string {
	return strings.ToLower(fmt.Sprintf("%s.%s.%s", db, schema, name))
}

func tableKey(db, schema, table string) string {
	return strings.ToLower(fmt.Sprintf("%s.%s.%s", db, schema, table))
}

// CreateStream creates a new stream on the given source table.
func (e *Engine) CreateStream(db, schema, name, tableName, streamType string) error {
	key := streamKey(db, schema, name)

	if streamType == "" {
		streamType = "STANDARD"
	}
	streamType = strings.ToUpper(streamType)
	if streamType != "STANDARD" && streamType != "APPEND_ONLY" {
		return fmt.Errorf("stream: unsupported type %q", streamType)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.streams[key]; exists {
		return fmt.Errorf("stream: %q already exists", name)
	}

	now := time.Now()
	e.streams[key] = &Stream{
		Name:         name,
		DatabaseName: db,
		SchemaName:   schema,
		TableName:    tableName,
		Type:         streamType,
		Stale:        false,
		StaleAfter:   now.Add(14 * 24 * time.Hour), // 14-day default
		Offset:       0,
		CreatedAt:    now,
		changes:      nil,
	}
	return nil
}

// DropStream removes a stream.
func (e *Engine) DropStream(db, schema, name string) error {
	key := streamKey(db, schema, name)

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.streams[key]; !exists {
		return fmt.Errorf("stream: %q does not exist", name)
	}
	delete(e.streams, key)
	return nil
}

// GetStream returns the stream metadata.
func (e *Engine) GetStream(db, schema, name string) (*Stream, error) {
	key := streamKey(db, schema, name)

	e.mu.RLock()
	defer e.mu.RUnlock()

	s, exists := e.streams[key]
	if !exists {
		return nil, fmt.Errorf("stream: %q does not exist", name)
	}
	return s, nil
}

// RecordChange appends a change record to every stream watching the given table.
func (e *Engine) RecordChange(db, schema, tableName string, change ChangeRecord) {
	tKey := tableKey(db, schema, tableName)

	if change.Timestamp.IsZero() {
		change.Timestamp = time.Now()
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, s := range e.streams {
		sTableKey := tableKey(s.DatabaseName, s.SchemaName, s.TableName)
		if sTableKey != tKey {
			continue
		}
		// APPEND_ONLY streams only capture inserts.
		if s.Type == "APPEND_ONLY" && change.Action != ChangeInsert {
			continue
		}
		s.mu.Lock()
		s.changes = append(s.changes, change)
		s.mu.Unlock()
	}
}

// ConsumeStream returns all unconsumed change records and advances the offset.
func (e *Engine) ConsumeStream(db, schema, name string) ([]ChangeRecord, error) {
	key := streamKey(db, schema, name)

	e.mu.RLock()
	s, exists := e.streams[key]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("stream: %q does not exist", name)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.Offset
	if offset >= int64(len(s.changes)) {
		return nil, nil
	}

	records := make([]ChangeRecord, len(s.changes[offset:]))
	copy(records, s.changes[offset:])
	s.Offset = int64(len(s.changes))

	return records, nil
}

// HasData returns true if the stream has unconsumed change records.
func (e *Engine) HasData(db, schema, name string) bool {
	key := streamKey(db, schema, name)

	e.mu.RLock()
	s, exists := e.streams[key]
	e.mu.RUnlock()

	if !exists {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Offset < int64(len(s.changes))
}

// ShowStreams returns metadata for all streams in the given database and schema.
func (e *Engine) ShowStreams(db, schema string) []StreamInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []StreamInfo
	dbLower := strings.ToLower(db)
	schemaLower := strings.ToLower(schema)

	for _, s := range e.streams {
		if strings.ToLower(s.DatabaseName) == dbLower && strings.ToLower(s.SchemaName) == schemaLower {
			result = append(result, StreamInfo{
				Name:         s.Name,
				DatabaseName: s.DatabaseName,
				SchemaName:   s.SchemaName,
				TableName:    s.TableName,
				Type:         s.Type,
				Stale:        s.Stale,
				CreatedAt:    s.CreatedAt,
			})
		}
	}
	return result
}
