// Package copyinto implements Snowflake's COPY INTO command for loading data
// from staged files into tables and unloading table data to staged files.
package copyinto

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/miniflakedb/miniflake/internal/stage"
)

// Direction indicates whether a COPY INTO loads or unloads data.
type Direction string

const (
	LoadIntoTable Direction = "LOAD"
	UnloadToStage Direction = "UNLOAD"
)

// FileFormat describes the file format options for COPY INTO.
type FileFormat struct {
	Type                       string // CSV, JSON, PARQUET, AVRO, ORC
	Compression                string // AUTO, GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE, NONE
	FieldDelimiter             string // for CSV, default ','
	RecordDelimiter            string // for CSV, default '\n'
	SkipHeader                 int    // for CSV
	DateFormat                 string
	TimestampFormat            string
	NullIf                     []string
	TrimSpace                  bool
	ErrorOnColumnCountMismatch bool
	StripOuterArray            bool // for JSON
}

// CopyOptions holds the runtime options for a COPY INTO operation.
type CopyOptions struct {
	OnError          string // CONTINUE, SKIP_FILE, SKIP_FILE_num, ABORT_STATEMENT
	SizeLimit        int64
	Purge            bool // delete files after load
	ReturnFailedOnly bool
	ForceLoad        bool  // reload even if already loaded
	MaxFileSize      int64 // for unload
	SingleFile       bool  // for unload
	Overwrite        bool  // for unload
}

// CopyResult describes the outcome of loading a single file.
type CopyResult struct {
	File           string
	Status         string // LOADED, LOAD_FAILED, PARTIALLY_LOADED, LOAD_SKIPPED
	RowsParsed     int64
	RowsLoaded     int64
	ErrorsSeen     int64
	FirstError     string
	FirstErrorLine int64
	FirstErrorCol  string
}

// Executor runs COPY INTO operations.
type Executor struct {
	engineExec  func(ctx context.Context, sql string, args ...interface{}) (int64, error)
	engineQuery func(ctx context.Context, sql string, args ...interface{}) ([]string, [][]interface{}, error)
	stageMgr    *stage.Manager
}

// NewExecutor creates a new COPY INTO executor.
func NewExecutor(
	engineExec func(ctx context.Context, sql string, args ...interface{}) (int64, error),
	engineQuery func(ctx context.Context, sql string, args ...interface{}) ([]string, [][]interface{}, error),
	stageMgr *stage.Manager,
) *Executor {
	return &Executor{
		engineExec:  engineExec,
		engineQuery: engineQuery,
		stageMgr:    stageMgr,
	}
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

// reCopyLoad matches: COPY INTO <table> FROM @<stage>[/<path>] ...
var reCopyLoad = regexp.MustCompile(
	`(?i)^\s*COPY\s+INTO\s+(\S+)\s+FROM\s+@(\S+)`,
)

// reCopyUnload matches: COPY INTO @<stage>[/<path>] FROM <table_or_query> ...
var reCopyUnload = regexp.MustCompile(
	`(?i)^\s*COPY\s+INTO\s+@(\S+)\s+FROM\s+(\S+)`,
)

// reFileFormat captures FILE_FORMAT = ( ... )
var reFileFormat = regexp.MustCompile(
	`(?i)FILE_FORMAT\s*=\s*\(([^)]+)\)`,
)

// rePurge captures PURGE = TRUE/FALSE
var rePurge = regexp.MustCompile(`(?i)\bPURGE\s*=\s*(TRUE|FALSE)\b`)

// reOnError captures ON_ERROR = <value>
var reOnError = regexp.MustCompile(`(?i)\bON_ERROR\s*=\s*(\S+)`)

// reOverwrite captures OVERWRITE = TRUE/FALSE
var reOverwrite = regexp.MustCompile(`(?i)\bOVERWRITE\s*=\s*(TRUE|FALSE)\b`)

// reSingleFile captures SINGLE = TRUE/FALSE
var reSingleFile = regexp.MustCompile(`(?i)\bSINGLE\s*=\s*(TRUE|FALSE)\b`)

// ParseCopyStatement parses a Snowflake COPY INTO statement and returns its components.
func ParseCopyStatement(sql string) (direction Direction, tableName string, stagePath string, format FileFormat, options CopyOptions, err error) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimRight(sql, ";")

	// Try LOAD first: COPY INTO table FROM @stage
	if m := reCopyLoad.FindStringSubmatch(sql); m != nil {
		direction = LoadIntoTable
		tableName = m[1]
		stagePath = m[2]
	} else if m := reCopyUnload.FindStringSubmatch(sql); m != nil {
		direction = UnloadToStage
		stagePath = m[1]
		tableName = m[2]
	} else {
		err = fmt.Errorf("copyinto: unable to parse COPY INTO statement: %s", sql)
		return
	}

	// Parse FILE_FORMAT
	format = DefaultCSV() // default
	if m := reFileFormat.FindStringSubmatch(sql); m != nil {
		opts := parseInlineOptions(m[1])
		format = ParseFileFormatOptions(opts)
	}

	// Parse copy options
	if m := rePurge.FindStringSubmatch(sql); m != nil {
		options.Purge = strings.EqualFold(m[1], "TRUE")
	}
	if m := reOnError.FindStringSubmatch(sql); m != nil {
		options.OnError = strings.ToUpper(m[1])
	} else {
		options.OnError = "ABORT_STATEMENT"
	}
	if m := reOverwrite.FindStringSubmatch(sql); m != nil {
		options.Overwrite = strings.EqualFold(m[1], "TRUE")
	}
	if m := reSingleFile.FindStringSubmatch(sql); m != nil {
		options.SingleFile = strings.EqualFold(m[1], "TRUE")
	}

	return
}

// parseInlineOptions parses "KEY1 = 'VAL1' KEY2 = VAL2" into a map.
func parseInlineOptions(raw string) map[string]string {
	result := make(map[string]string)
	// Match KEY = 'value' or KEY = value patterns
	re := regexp.MustCompile(`(?i)(\w+)\s*=\s*(?:'([^']*)'|(\S+))`)
	matches := re.FindAllStringSubmatch(raw, -1)
	for _, m := range matches {
		key := strings.ToUpper(m[1])
		val := m[2]
		if val == "" {
			val = m[3]
		}
		result[key] = val
	}
	return result
}

// ---------------------------------------------------------------------------
// Load: stage files -> table
// ---------------------------------------------------------------------------

// ExecuteLoad reads files from an already-resolved stage and loads them into a
// table. The caller resolves the stage reference (see the orchestrator's
// stageref.go) so that COPY INTO, LIST, PUT and GET all agree on which stage a
// given reference names.
func (e *Executor) ExecuteLoad(ctx context.Context, tableName string, meta *stage.StageMeta, subPath string, format FileFormat, options CopyOptions) ([]CopyResult, error) {
	files, err := e.stageMgr.ListMetaFiles(meta, stage.ListOptions{Prefix: subPath})
	if err != nil {
		return nil, fmt.Errorf("copyinto: list stage files: %w", err)
	}

	if len(files) == 0 {
		return nil, nil
	}

	var results []CopyResult
	for _, f := range files {
		absPath := filepath.Join(meta.LocalPath, f.Name)
		result := CopyResult{
			File: f.Name,
		}

		loadSQL, loadErr := buildLoadSQL(tableName, absPath, format)
		if loadErr != nil {
			result.Status = "LOAD_FAILED"
			result.ErrorsSeen = 1
			result.FirstError = loadErr.Error()
			results = append(results, result)
			if options.OnError == "ABORT_STATEMENT" {
				return results, loadErr
			}
			continue
		}

		rowsAffected, execErr := e.engineExec(ctx, loadSQL)
		if execErr != nil {
			result.Status = "LOAD_FAILED"
			result.ErrorsSeen = 1
			result.FirstError = execErr.Error()
			results = append(results, result)
			if options.OnError == "ABORT_STATEMENT" {
				return results, execErr
			}
			continue
		}

		result.Status = "LOADED"
		result.RowsParsed = rowsAffected
		result.RowsLoaded = rowsAffected
		results = append(results, result)

		// Purge: delete the file after successful load.
		if options.Purge {
			_ = e.stageMgr.RemoveMetaFile(meta, f.Name)
		}
	}

	return results, nil
}

// buildLoadSQL produces a DuckDB INSERT ... SELECT statement to load a file.
func buildLoadSQL(tableName string, filePath string, format FileFormat) (string, error) {
	switch strings.ToUpper(format.Type) {
	case "PARQUET":
		return fmt.Sprintf("INSERT INTO %s SELECT * FROM read_parquet('%s')", tableName, escapePath(filePath)), nil
	case "CSV":
		opts := buildCSVReadOptions(format)
		return fmt.Sprintf("INSERT INTO %s SELECT * FROM read_csv('%s'%s)", tableName, escapePath(filePath), opts), nil
	case "JSON":
		return fmt.Sprintf("INSERT INTO %s SELECT * FROM read_json_auto('%s')", tableName, escapePath(filePath)), nil
	default:
		return "", fmt.Errorf("unsupported file format: %s", format.Type)
	}
}

// buildCSVReadOptions builds the DuckDB read_csv option string.
func buildCSVReadOptions(f FileFormat) string {
	var parts []string
	if f.FieldDelimiter != "" {
		parts = append(parts, fmt.Sprintf("delim = '%s'", f.FieldDelimiter))
	}
	if f.SkipHeader > 0 {
		parts = append(parts, "header = true")
	} else {
		parts = append(parts, "header = false")
	}
	if len(parts) == 0 {
		return ""
	}
	return ", " + strings.Join(parts, ", ")
}

// escapePath escapes single quotes in file paths for SQL.
func escapePath(p string) string {
	return strings.ReplaceAll(p, "'", "''")
}

// ---------------------------------------------------------------------------
// Unload: table -> stage files
// ---------------------------------------------------------------------------

// ExecuteUnload writes table data to files in a stage.
func (e *Executor) ExecuteUnload(ctx context.Context, tableName string, meta *stage.StageMeta, subPath string, format FileFormat, options CopyOptions) ([]CopyResult, error) {
	destDir := meta.LocalPath
	if subPath != "" {
		// Contain the subpath inside the stage. filepath.Join alone cleans
		// "../" segments rather than rejecting them, so COPY INTO
		// @stage/../../x FROM t would otherwise write outside the stage
		// directory (issue #3). ResolveInStage is the same containment check
		// PUT, GET and REMOVE use.
		resolved, resErr := stage.ResolveInStage(meta, subPath)
		if resErr != nil {
			return nil, fmt.Errorf("copyinto: %w", resErr)
		}
		destDir = resolved
		if mkErr := os.MkdirAll(destDir, 0o755); mkErr != nil {
			return nil, fmt.Errorf("copyinto: create dest dir: %w", mkErr)
		}
	}

	ext := formatExtension(format.Type)
	destFile := filepath.Join(destDir, "data_0_0_0"+ext)

	if options.Overwrite {
		_ = os.Remove(destFile)
	}

	unloadSQL := buildUnloadSQL(tableName, destFile, format)

	_, execErr := e.engineExec(ctx, unloadSQL)
	if execErr != nil {
		return []CopyResult{{
			File:       filepath.Base(destFile),
			Status:     "LOAD_FAILED",
			ErrorsSeen: 1,
			FirstError: execErr.Error(),
		}}, execErr
	}

	// Get file info for the result.
	info, _ := os.Stat(destFile)
	var rowCount int64
	if info != nil {
		rowCount = info.Size() // approximate; real Snowflake returns row count
	}

	return []CopyResult{{
		File:       filepath.Base(destFile),
		Status:     "LOADED",
		RowsParsed: rowCount,
		RowsLoaded: rowCount,
	}}, nil
}

// buildUnloadSQL produces a DuckDB COPY ... TO statement.
func buildUnloadSQL(tableName, filePath string, format FileFormat) string {
	fmtStr := strings.ToUpper(format.Type)
	switch fmtStr {
	case "PARQUET":
		return fmt.Sprintf("COPY (SELECT * FROM %s) TO '%s' (FORMAT PARQUET)", tableName, escapePath(filePath))
	case "CSV":
		return fmt.Sprintf("COPY (SELECT * FROM %s) TO '%s' (FORMAT CSV, DELIMITER '%s', HEADER)", tableName, escapePath(filePath), format.FieldDelimiter)
	case "JSON":
		return fmt.Sprintf("COPY (SELECT * FROM %s) TO '%s' (FORMAT JSON)", tableName, escapePath(filePath))
	default:
		return fmt.Sprintf("COPY (SELECT * FROM %s) TO '%s' (FORMAT %s)", tableName, escapePath(filePath), fmtStr)
	}
}

// formatExtension returns the file extension for a given format type.
func formatExtension(fmtType string) string {
	switch strings.ToUpper(fmtType) {
	case "PARQUET":
		return ".parquet"
	case "CSV":
		return ".csv"
	case "JSON":
		return ".json"
	default:
		return ".dat"
	}
}

// ---------------------------------------------------------------------------
// Stage path parsing
// ---------------------------------------------------------------------------
