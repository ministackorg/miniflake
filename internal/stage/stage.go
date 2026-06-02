package stage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// StageType identifies the kind of Snowflake stage.
type StageType string

const (
	StageInternal StageType = "INTERNAL"
	StageExternal StageType = "EXTERNAL"
	StageUser     StageType = "USER"
	StageTable    StageType = "TABLE"
)

// StageMeta holds metadata for a stage.
type StageMeta struct {
	Name      string
	Type      StageType
	URL       string // for external: s3://bucket/path
	LocalPath string // actual filesystem path
	CreatedAt time.Time
}

// FileInfo describes a file within a stage.
type FileInfo struct {
	Name string
	Size int64
}

// Manager manages Snowflake stages on the local filesystem.
type Manager struct {
	baseDir string
	mu      sync.RWMutex
	stages  map[string]*StageMeta // key: DB.SCHEMA.STAGE_NAME
}

// NewManager creates a new stage manager rooted at baseDir.
func NewManager(baseDir string) *Manager {
	return &Manager{
		baseDir: baseDir,
		stages:  make(map[string]*StageMeta),
	}
}

func stageKey(db, schema, name string) string {
	return strings.ToUpper(fmt.Sprintf("%s.%s.%s", db, schema, name))
}

// CreateStage creates a named stage and its backing directory.
func (m *Manager) CreateStage(db, schema, name string, stageType StageType, url string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := stageKey(db, schema, name)
	if _, exists := m.stages[key]; exists {
		return fmt.Errorf("stage '%s' already exists", key)
	}

	localPath := filepath.Join(m.baseDir, "stages", strings.ToUpper(db), strings.ToUpper(schema), strings.ToUpper(name))
	if err := os.MkdirAll(localPath, 0o755); err != nil {
		return fmt.Errorf("failed to create stage directory: %w", err)
	}

	m.stages[key] = &StageMeta{
		Name:      strings.ToUpper(name),
		Type:      stageType,
		URL:       url,
		LocalPath: localPath,
		CreatedAt: time.Now(),
	}
	return nil
}

// DropStage removes a stage and its backing directory.
func (m *Manager) DropStage(db, schema, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := stageKey(db, schema, name)
	meta, exists := m.stages[key]
	if !exists {
		return fmt.Errorf("stage '%s' does not exist", key)
	}
	_ = os.RemoveAll(meta.LocalPath)
	delete(m.stages, key)
	return nil
}

// GetStage returns metadata for a stage.
func (m *Manager) GetStage(db, schema, name string) (*StageMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := stageKey(db, schema, name)
	meta, ok := m.stages[key]
	if !ok {
		return nil, fmt.Errorf("stage '%s' does not exist", key)
	}
	return meta, nil
}

// PutFile copies a local file into the stage directory.
func (m *Manager) PutFile(db, schema, stageName, localPath, destPath string) error {
	meta, err := m.GetStage(db, schema, stageName)
	if err != nil {
		return err
	}

	dest := filepath.Join(meta.LocalPath, destPath)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	return copyFile(localPath, dest)
}

// GetFile copies a file from the stage to a local path.
func (m *Manager) GetFile(db, schema, stageName, srcPath, localPath string) error {
	meta, err := m.GetStage(db, schema, stageName)
	if err != nil {
		return err
	}

	src := filepath.Join(meta.LocalPath, srcPath)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	return copyFile(src, localPath)
}

// ListFiles lists files in a stage, optionally filtered by a glob pattern.
func (m *Manager) ListFiles(db, schema, stageName, pattern string) ([]FileInfo, error) {
	meta, err := m.GetStage(db, schema, stageName)
	if err != nil {
		return nil, err
	}

	var files []FileInfo
	err = filepath.Walk(meta.LocalPath, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(meta.LocalPath, path)
		if pattern != "" {
			matched, matchErr := filepath.Match(pattern, filepath.Base(rel))
			if matchErr != nil {
				return matchErr
			}
			if !matched {
				return nil
			}
		}
		files = append(files, FileInfo{Name: rel, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}
	return files, nil
}

// RemoveFile removes a file from a stage.
func (m *Manager) RemoveFile(db, schema, stageName, path string) error {
	meta, err := m.GetStage(db, schema, stageName)
	if err != nil {
		return err
	}
	target := filepath.Join(meta.LocalPath, path)
	if err := os.Remove(target); err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}
	return nil
}

// GetUserStage returns (or creates) the @~ stage for a user.
func (m *Manager) GetUserStage(user string) *StageMeta {
	key := stageKey("~", "~", strings.ToUpper(user))

	m.mu.RLock()
	if meta, ok := m.stages[key]; ok {
		m.mu.RUnlock()
		return meta
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	// Double-check after acquiring write lock.
	if meta, ok := m.stages[key]; ok {
		return meta
	}

	localPath := filepath.Join(m.baseDir, "user_stages", strings.ToUpper(user))
	_ = os.MkdirAll(localPath, 0o755)
	meta := &StageMeta{
		Name:      "@~" + strings.ToUpper(user),
		Type:      StageUser,
		LocalPath: localPath,
		CreatedAt: time.Now(),
	}
	m.stages[key] = meta
	return meta
}

// GetTableStage returns (or creates) the @%table stage.
func (m *Manager) GetTableStage(db, schema, table string) *StageMeta {
	key := stageKey(db, schema, "%"+strings.ToUpper(table))

	m.mu.RLock()
	if meta, ok := m.stages[key]; ok {
		m.mu.RUnlock()
		return meta
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if meta, ok := m.stages[key]; ok {
		return meta
	}

	localPath := filepath.Join(m.baseDir, "table_stages", strings.ToUpper(db), strings.ToUpper(schema), strings.ToUpper(table))
	_ = os.MkdirAll(localPath, 0o755)
	meta := &StageMeta{
		Name:      "@%" + strings.ToUpper(table),
		Type:      StageTable,
		LocalPath: localPath,
		CreatedAt: time.Now(),
	}
	m.stages[key] = meta
	return meta
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	return out.Close()
}
