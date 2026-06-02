package sequence

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Sequence represents a Snowflake sequence object.
type Sequence struct {
	Name      string
	Database  string
	Schema    string
	Start     int64
	Increment int64
	Current   int64
	CreatedAt time.Time
	mu        sync.Mutex
}

// SequenceInfo is the read-only representation returned by Show.
type SequenceInfo struct {
	Name      string
	Database  string
	Schema    string
	Start     int64
	Increment int64
	Current   int64
	CreatedAt time.Time
}

// Manager manages all sequences in the emulator.
type Manager struct {
	mu        sync.RWMutex
	sequences map[string]*Sequence // key: DB.SCHEMA.NAME
}

func qualifiedName(db, schema, name string) string {
	return strings.ToUpper(db) + "." + strings.ToUpper(schema) + "." + strings.ToUpper(name)
}

// NewManager creates a new sequence manager.
func NewManager() *Manager {
	return &Manager{
		sequences: make(map[string]*Sequence),
	}
}

// CreateSequence creates a new sequence. Returns an error if it already exists.
func (m *Manager) CreateSequence(db, schema, name string, start, increment int64) error {
	key := qualifiedName(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sequences[key]; exists {
		return fmt.Errorf("sequence %s already exists", key)
	}

	m.sequences[key] = &Sequence{
		Name:      strings.ToUpper(name),
		Database:  strings.ToUpper(db),
		Schema:    strings.ToUpper(schema),
		Start:     start,
		Increment: increment,
		Current:   start,
		CreatedAt: time.Now(),
	}
	return nil
}

// DropSequence removes a sequence. Returns an error if it does not exist.
func (m *Manager) DropSequence(db, schema, name string) error {
	key := qualifiedName(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sequences[key]; !exists {
		return fmt.Errorf("sequence %s does not exist", key)
	}

	delete(m.sequences, key)
	return nil
}

// NextVal atomically increments the sequence and returns the new value.
// The first call returns Start, subsequent calls return Current + Increment.
func (m *Manager) NextVal(db, schema, name string) (int64, error) {
	key := qualifiedName(db, schema, name)

	m.mu.RLock()
	seq, exists := m.sequences[key]
	m.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("sequence %s does not exist", key)
	}

	seq.mu.Lock()
	defer seq.mu.Unlock()

	val := seq.Current
	seq.Current += seq.Increment
	return val, nil
}

// CurrentVal returns the current value without incrementing.
func (m *Manager) CurrentVal(db, schema, name string) (int64, error) {
	key := qualifiedName(db, schema, name)

	m.mu.RLock()
	seq, exists := m.sequences[key]
	m.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("sequence %s does not exist", key)
	}

	seq.mu.Lock()
	defer seq.mu.Unlock()

	return seq.Current, nil
}

// ShowSequences returns info for all sequences in the given database and schema.
func (m *Manager) ShowSequences(db, schema string) []SequenceInfo {
	prefix := strings.ToUpper(db) + "." + strings.ToUpper(schema) + "."

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []SequenceInfo
	for key, seq := range m.sequences {
		if strings.HasPrefix(key, prefix) {
			seq.mu.Lock()
			result = append(result, SequenceInfo{
				Name:      seq.Name,
				Database:  seq.Database,
				Schema:    seq.Schema,
				Start:     seq.Start,
				Increment: seq.Increment,
				Current:   seq.Current,
				CreatedAt: seq.CreatedAt,
			})
			seq.mu.Unlock()
		}
	}
	return result
}
