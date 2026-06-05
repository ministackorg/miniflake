package timetravel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Querier is the interface for executing queries against DuckDB.
// This avoids a hard dependency on the engine package.
type Querier interface {
	Execute(ctx context.Context, query string, args ...interface{}) ([]string, [][]interface{}, error)
}

// Snapshot represents a point-in-time capture of a table's data.
type Snapshot struct {
	ID        int64
	TableName string // fully qualified: db.schema.table
	Timestamp time.Time
	DataFile  string // path to Parquet export
}

// Engine manages time-travel snapshots.
type Engine struct {
	mu        sync.RWMutex
	snapshots map[string][]Snapshot // key: fully qualified table name
	dataDir   string
	maxAge    time.Duration // retention period
	nextID    int64
}

// NewEngine creates a new time-travel Engine.
// dataDir is the root directory for snapshot storage.
// maxAge is the retention period (snapshots older than this are cleaned up).
func NewEngine(dataDir string, maxAge time.Duration) *Engine {
	if maxAge <= 0 {
		maxAge = 24 * time.Hour
	}
	return &Engine{
		snapshots: make(map[string][]Snapshot),
		dataDir:   dataDir,
		maxAge:    maxAge,
		nextID:    1,
	}
}

func fqTableName(db, schema, table string) string {
	return strings.ToLower(fmt.Sprintf("%s.%s.%s", db, schema, table))
}

// snapshotDir returns the directory for a table's snapshots.
func (e *Engine) snapshotDir(db, schema, table string) string {
	return filepath.Join(e.dataDir, "snapshots", db, schema, table)
}

// CaptureSnapshot exports the current state of a table to a Parquet file.
func (e *Engine) CaptureSnapshot(db, schema, table string, querier Querier) error {
	fqn := fqTableName(db, schema, table)
	now := time.Now()

	dir := e.snapshotDir(db, schema, table)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("timetravel: create snapshot dir: %w", err)
	}

	filename := fmt.Sprintf("%d.parquet", now.UnixNano())
	filePath := filepath.Join(dir, filename)

	// Export table to Parquet using DuckDB's COPY. DuckDB resolves the
	// table via the engine's current schema (set by SetCurrentSchema after
	// USE SCHEMA), so an unqualified name is what works here — the
	// Snowflake-style db/schema only key the snapshot directory.
	query := fmt.Sprintf("COPY \"%s\" TO '%s' (FORMAT PARQUET)", table, filePath)
	_, _, err := querier.Execute(context.Background(), query)
	if err != nil {
		return fmt.Errorf("timetravel: export snapshot: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	snap := Snapshot{
		ID:        e.nextID,
		TableName: fqn,
		Timestamp: now,
		DataFile:  filePath,
	}
	e.nextID++
	e.snapshots[fqn] = append(e.snapshots[fqn], snap)

	return nil
}

// QueryAtTimestamp returns the snapshot file closest to (but not after) the given timestamp.
func (e *Engine) QueryAtTimestamp(db, schema, table string, ts time.Time) (string, error) {
	fqn := fqTableName(db, schema, table)

	e.mu.RLock()
	snaps := e.snapshots[fqn]
	e.mu.RUnlock()

	if len(snaps) == 0 {
		return "", fmt.Errorf("timetravel: no snapshots for table %q", fqn)
	}

	// Find the latest snapshot at or before ts.
	var best *Snapshot
	for i := len(snaps) - 1; i >= 0; i-- {
		if !snaps[i].Timestamp.After(ts) {
			best = &snaps[i]
			break
		}
	}

	if best == nil {
		return "", fmt.Errorf("timetravel: no snapshot at or before %v for table %q", ts, fqn)
	}

	return best.DataFile, nil
}

// QueryAtOffset returns the snapshot file for the table state N seconds ago.
func (e *Engine) QueryAtOffset(db, schema, table string, seconds int64) (string, error) {
	ts := time.Now().Add(-time.Duration(seconds) * time.Second)
	return e.QueryAtTimestamp(db, schema, table, ts)
}

// Undrop returns the most recent snapshot file for a table, enabling restore.
func (e *Engine) Undrop(db, schema, table string) (string, error) {
	fqn := fqTableName(db, schema, table)

	e.mu.RLock()
	snaps := e.snapshots[fqn]
	e.mu.RUnlock()

	if len(snaps) == 0 {
		return "", fmt.Errorf("timetravel: no snapshots for table %q (cannot undrop)", fqn)
	}

	latest := snaps[len(snaps)-1]
	return latest.DataFile, nil
}

// Cleanup removes snapshots older than maxAge and deletes their Parquet files.
func (e *Engine) Cleanup() {
	cutoff := time.Now().Add(-e.maxAge)

	e.mu.Lock()
	defer e.mu.Unlock()

	for fqn, snaps := range e.snapshots {
		var kept []Snapshot
		for _, s := range snaps {
			if s.Timestamp.Before(cutoff) {
				// Best-effort delete of the Parquet file.
				os.Remove(s.DataFile)
			} else {
				kept = append(kept, s)
			}
		}
		if len(kept) == 0 {
			delete(e.snapshots, fqn)
		} else {
			e.snapshots[fqn] = kept
		}
	}
}

// StartCleanupLoop runs Cleanup periodically in a background goroutine.
func (e *Engine) StartCleanupLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.Cleanup()
			}
		}
	}()
}

// ListSnapshots returns all snapshots for a table, sorted by timestamp ascending.
func (e *Engine) ListSnapshots(db, schema, table string) []Snapshot {
	fqn := fqTableName(db, schema, table)

	e.mu.RLock()
	defer e.mu.RUnlock()

	snaps := e.snapshots[fqn]
	result := make([]Snapshot, len(snaps))
	copy(result, snaps)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.Before(result[j].Timestamp)
	})
	return result
}
