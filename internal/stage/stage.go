package stage

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
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
	Name    string
	Size    int64
	ModTime time.Time
	MD5     string
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

// Reset drops every registered stage and wipes the stage directory on disk.
func (m *Manager) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stages = make(map[string]*StageMeta)
	if err := os.RemoveAll(m.baseDir); err != nil {
		return fmt.Errorf("stage: reset remove %s: %w", m.baseDir, err)
	}
	if err := os.MkdirAll(m.baseDir, 0o755); err != nil {
		return fmt.Errorf("stage: reset mkdir %s: %w", m.baseDir, err)
	}
	return nil
}

// ErrStageExists reports that a stage of that name is already registered, so
// callers can implement CREATE STAGE IF NOT EXISTS without matching on the
// error text.
var ErrStageExists = errors.New("stage already exists")

func stageKey(db, schema, name string) string {
	return strings.ToUpper(fmt.Sprintf("%s.%s.%s", db, schema, name))
}

// CreateStage creates a named stage and its backing directory.
func (m *Manager) CreateStage(db, schema, name string, stageType StageType, url string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := stageKey(db, schema, name)
	if _, exists := m.stages[key]; exists {
		return fmt.Errorf("stage '%s': %w", key, ErrStageExists)
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

// PutFile copies a local file into a named stage.
func (m *Manager) PutFile(db, schema, stageName, localPath, destPath string) error {
	meta, err := m.GetStage(db, schema, stageName)
	if err != nil {
		return err
	}
	return m.PutMetaFile(meta, localPath, destPath)
}

// PutMetaFile copies a local file into an already-resolved stage (named, user,
// or table), mirroring ListMetaFiles.
func (m *Manager) PutMetaFile(meta *StageMeta, localPath, destPath string) error {
	dest, err := ResolveInStage(meta, destPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}
	return copyFile(localPath, dest)
}

// GetFile copies a file out of a named stage to a local path.
func (m *Manager) GetFile(db, schema, stageName, srcPath, localPath string) error {
	meta, err := m.GetStage(db, schema, stageName)
	if err != nil {
		return err
	}
	return m.GetMetaFile(meta, srcPath, localPath)
}

// GetMetaFile copies a file out of an already-resolved stage (named, user, or
// table), mirroring ListMetaFiles.
func (m *Manager) GetMetaFile(meta *StageMeta, srcPath, localPath string) error {
	src, err := ResolveInStage(meta, srcPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}
	return copyFile(src, localPath)
}

// ListOptions controls optional filtering and checksum behaviour for listings.
type ListOptions struct {
	// Prefix keeps files whose relative path begins with prefix, matching
	// Snowflake's literal-prefix `@stage/path` semantics: a raw string prefix
	// on the file path, not restricted to whole path components (so `data`
	// also matches `database.csv`). Empty means no filter.
	Prefix string
	// Pattern is a filepath glob matched against the file basename. Used by
	// COPY INTO / GET. Empty means no filter.
	Pattern string
	// Regex is a RE2 pattern matched against the slash-normalized relative
	// path (Snowflake `LIST … PATTERN = '…'`). Empty means no filter.
	Regex string
	// Checksum, when true, populates FileInfo.MD5. Left false for hot paths
	// (COPY INTO, GET) that only need names and sizes.
	Checksum bool
}

// ListFiles lists files in a named stage, optionally filtered by a basename glob.
func (m *Manager) ListFiles(db, schema, stageName, pattern string) ([]FileInfo, error) {
	return m.ListFilesWithOptions(db, schema, stageName, ListOptions{Pattern: pattern})
}

// ListFilesWithOptions lists files in a named stage with optional filters.
func (m *Manager) ListFilesWithOptions(db, schema, stageName string, opts ListOptions) ([]FileInfo, error) {
	meta, err := m.GetStage(db, schema, stageName)
	if err != nil {
		return nil, err
	}
	return m.ListMetaFiles(meta, opts)
}

// ListMetaFiles lists files under an already-resolved stage (named, user, or table).
func (m *Manager) ListMetaFiles(meta *StageMeta, opts ListOptions) ([]FileInfo, error) {
	// Trim only a leading slash (left over from the `@stage/…` split); keep any
	// trailing slash so `@stage/data/` still scopes to that folder while
	// `@stage/data` is the broader literal prefix Snowflake documents.
	prefix := strings.TrimPrefix(filepath.ToSlash(opts.Prefix), "/")

	var re *regexp.Regexp
	if opts.Regex != "" {
		// Snowflake's PATTERN has to match the whole path, not merely occur
		// somewhere in it (Java Matcher.matches semantics), which is why its
		// documented examples all lead with ".*". Anchor what the caller gave
		// us so `PATTERN = 'a[.]csv'` does not quietly match `dir/a.csv`.
		compiled, err := regexp.Compile(`^(?:` + opts.Regex + `)$`)
		if err != nil {
			return nil, fmt.Errorf("invalid PATTERN regexp: %w", err)
		}
		re = compiled
	}

	var files []FileInfo
	err := filepath.WalkDir(meta.LocalPath, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// The stage directory may not exist yet, and individual files can
			// vanish mid-walk under a concurrent REMOVE or DROP STAGE. Neither
			// should fail the listing: report what is actually there.
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(meta.LocalPath, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)

		if prefix != "" && !strings.HasPrefix(rel, prefix) {
			return nil
		}
		if opts.Pattern != "" {
			matched, matchErr := filepath.Match(opts.Pattern, filepath.Base(rel))
			if matchErr != nil {
				return matchErr
			}
			if !matched {
				return nil
			}
		}
		if re != nil && !re.MatchString(rel) {
			return nil
		}

		// Stat only what survives the filters, and tolerate the file going
		// away between readdir and here.
		info, infoErr := d.Info()
		if infoErr != nil {
			if errors.Is(infoErr, fs.ErrNotExist) {
				return nil
			}
			return infoErr
		}

		fi := FileInfo{
			Name:    rel,
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}
		if opts.Checksum {
			fi.MD5 = fileMD5(path)
		}
		files = append(files, fi)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}
	return files, nil
}

// fileMD5 returns the hex-encoded MD5 checksum of the file at path, matching
// the checksum Snowflake's LIST command reports for staged files. Returns an
// empty string if the file can't be read, rather than failing the whole
// listing over one unreadable entry.
func fileMD5(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RemoveFile removes a file from a named stage.
func (m *Manager) RemoveFile(db, schema, stageName, path string) error {
	meta, err := m.GetStage(db, schema, stageName)
	if err != nil {
		return err
	}
	return m.RemoveMetaFile(meta, path)
}

// RemoveMetaFile removes a file under an already-resolved stage (named, user,
// or table), mirroring ListMetaFiles.
func (m *Manager) RemoveMetaFile(meta *StageMeta, path string) error {
	target, err := ResolveInStage(meta, path)
	if err != nil {
		return err
	}
	if err := os.Remove(target); err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}
	return nil
}

// ResolveInStage joins a stage-relative path onto the stage root and refuses
// anything that climbs back out of it. filepath.Join cleans "../" segments
// rather than rejecting them, so without this a reference like
// "@s/../../secret" would address files outside the stage entirely.
//
// Exported so every subsystem that turns a stage reference into a filesystem
// path (PUT, GET, REMOVE and COPY INTO) shares one containment check and they
// cannot drift apart on it.
func ResolveInStage(meta *StageMeta, path string) (string, error) {
	target := filepath.Join(meta.LocalPath, filepath.FromSlash(path))
	rel, err := filepath.Rel(meta.LocalPath, target)
	if err != nil {
		return "", fmt.Errorf("invalid stage path %q: %w", path, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("stage path %q escapes the stage", path)
	}
	return target, nil
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
