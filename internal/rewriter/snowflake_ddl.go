package rewriter

import (
	"regexp"
	"strings"
)

// Rewrite rules for Snowflake-specific DDL/DML that DuckDB doesn't recognize
// natively. Each rule emits a marker comment carrying the original SQL so
// the orchestrator can route to the correct in-process subsystem.

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

var reCreateStream = regexp.MustCompile(`(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?STREAM\b`)
var reDropStream = regexp.MustCompile(`(?is)^\s*DROP\s+STREAM\s+(IF\s+EXISTS\s+)?(\S+)\s*;?\s*$`)
var reShowStreams = regexp.MustCompile(`(?is)^\s*SHOW\s+STREAMS(\s+IN\s+SCHEMA\s+(\S+))?\s*;?\s*$`)

func rewriteStreams(sql string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(sql), ";")
	if reCreateStream.MatchString(trimmed) {
		return "/* MINIFLAKE_CREATE_STREAM " + trimmed + " */"
	}
	if m := reDropStream.FindStringSubmatch(trimmed); m != nil {
		ifExists := "0"
		if m[1] != "" {
			ifExists = "1"
		}
		return "/* MINIFLAKE_DROP_STREAM " + m[2] + " " + ifExists + " */"
	}
	if m := reShowStreams.FindStringSubmatch(trimmed); m != nil {
		scope := ""
		if m[2] != "" {
			scope = m[2]
		}
		return "/* MINIFLAKE_SHOW_STREAMS " + scope + " */"
	}
	return sql
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

var reCreateTask = regexp.MustCompile(`(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?TASK\b`)
var reDropTask = regexp.MustCompile(`(?is)^\s*DROP\s+TASK\s+(IF\s+EXISTS\s+)?(\S+)\s*;?\s*$`)
var reAlterTask = regexp.MustCompile(`(?is)^\s*ALTER\s+TASK\s+(\S+)\s+(RESUME|SUSPEND)\s*;?\s*$`)
var reShowTasks = regexp.MustCompile(`(?is)^\s*SHOW\s+TASKS(\s+IN\s+SCHEMA\s+(\S+))?\s*;?\s*$`)
var reExecTask = regexp.MustCompile(`(?is)^\s*EXECUTE\s+TASK\s+(\S+)\s*;?\s*$`)

func rewriteTasks(sql string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(sql), ";")
	if reCreateTask.MatchString(trimmed) {
		return "/* MINIFLAKE_CREATE_TASK " + trimmed + " */"
	}
	if m := reDropTask.FindStringSubmatch(trimmed); m != nil {
		ifExists := "0"
		if m[1] != "" {
			ifExists = "1"
		}
		return "/* MINIFLAKE_DROP_TASK " + m[2] + " " + ifExists + " */"
	}
	if m := reAlterTask.FindStringSubmatch(trimmed); m != nil {
		return "/* MINIFLAKE_ALTER_TASK " + m[1] + " " + strings.ToUpper(m[2]) + " */"
	}
	if m := reShowTasks.FindStringSubmatch(trimmed); m != nil {
		scope := ""
		if m[2] != "" {
			scope = m[2]
		}
		return "/* MINIFLAKE_SHOW_TASKS " + scope + " */"
	}
	if m := reExecTask.FindStringSubmatch(trimmed); m != nil {
		return "/* MINIFLAKE_EXECUTE_TASK " + m[1] + " */"
	}
	return sql
}

// ---------------------------------------------------------------------------
// Pipes (Snowpipe)
// ---------------------------------------------------------------------------

var reCreatePipe = regexp.MustCompile(`(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?PIPE\b`)
var reDropPipe = regexp.MustCompile(`(?is)^\s*DROP\s+PIPE\s+(IF\s+EXISTS\s+)?(\S+)\s*;?\s*$`)
var reShowPipes = regexp.MustCompile(`(?is)^\s*SHOW\s+PIPES(\s+IN\s+SCHEMA\s+(\S+))?\s*;?\s*$`)

func rewritePipes(sql string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(sql), ";")
	if reCreatePipe.MatchString(trimmed) {
		return "/* MINIFLAKE_CREATE_PIPE " + trimmed + " */"
	}
	if m := reDropPipe.FindStringSubmatch(trimmed); m != nil {
		ifExists := "0"
		if m[1] != "" {
			ifExists = "1"
		}
		return "/* MINIFLAKE_DROP_PIPE " + m[2] + " " + ifExists + " */"
	}
	if m := reShowPipes.FindStringSubmatch(trimmed); m != nil {
		scope := ""
		if m[2] != "" {
			scope = m[2]
		}
		return "/* MINIFLAKE_SHOW_PIPES " + scope + " */"
	}
	return sql
}

// ---------------------------------------------------------------------------
// Stage file commands: LIST/LS and REMOVE/RM against @stage
// ---------------------------------------------------------------------------
//
// DuckDB has no @stage concept, so these can't be executed directly. Wrap them
// in markers carrying the original statement; the orchestrator answers them
// from the stage manager (see handleListStage / handleRemoveStage in
// snowflake_handlers.go). PUT and GET follow the same pattern in rewritePutGet.

var reListCmd = regexp.MustCompile(`(?is)^\s*(?:LIST|LS)\s+@`)
var reRemoveCmd = regexp.MustCompile(`(?is)^\s*(?:REMOVE|RM)\s+@`)

func rewriteStageCommands(sql string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(sql), ";")
	if reListCmd.MatchString(trimmed) {
		return "/* MINIFLAKE_LIST " + trimmed + " */"
	}
	if reRemoveCmd.MatchString(trimmed) {
		return "/* MINIFLAKE_REMOVE " + trimmed + " */"
	}
	return sql
}

// ---------------------------------------------------------------------------
// Time travel / UNDROP
// ---------------------------------------------------------------------------

var reUndrop = regexp.MustCompile(`(?is)^\s*UNDROP\s+TABLE\s+(\S+)\s*;?\s*$`)

func rewriteUndrop(sql string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(sql), ";")
	if m := reUndrop.FindStringSubmatch(trimmed); m != nil {
		return "/* MINIFLAKE_UNDROP_TABLE " + m[1] + " */"
	}
	return sql
}

// AT/BEFORE time-travel — Snowflake form:
//
//	FROM t AT(OFFSET => -<seconds>)
//	FROM t AT(TIMESTAMP => '<iso-utc>')
//	FROM t BEFORE(STATEMENT => '<id>')
//
// We translate to a marker that the orchestrator resolves to the right
// snapshot file (via the timetravel engine) and rewrites to read_parquet.
var reAtBeforeOffset = regexp.MustCompile(`(?is)\bFROM\s+(\S+)\s+AT\s*\(\s*OFFSET\s*=>\s*(-?\d+)\s*\)`)
var reAtBeforeTimestamp = regexp.MustCompile(`(?is)\bFROM\s+(\S+)\s+(AT|BEFORE)\s*\(\s*TIMESTAMP\s*=>\s*'([^']+)'\s*\)`)

func rewriteTimeTravel(sql string) string {
	sql = reAtBeforeOffset.ReplaceAllStringFunc(sql, func(match string) string {
		m := reAtBeforeOffset.FindStringSubmatch(match)
		return "FROM /* MINIFLAKE_AT_OFFSET " + m[1] + " " + m[2] + " */ " + m[1]
	})
	sql = reAtBeforeTimestamp.ReplaceAllStringFunc(sql, func(match string) string {
		m := reAtBeforeTimestamp.FindStringSubmatch(match)
		return "FROM /* MINIFLAKE_AT_TIMESTAMP " + m[1] + " " + m[3] + " */ " + m[1]
	})
	return sql
}

// ---------------------------------------------------------------------------
// MERGE INTO — translate the simple upsert pattern to DuckDB
// INSERT ... ON CONFLICT DO UPDATE. Anything more complex falls through and
// will fail at DuckDB (intentional — better than silently wrong semantics).
//
//	MERGE INTO t USING src ON t.k = src.k
//	  WHEN MATCHED THEN UPDATE SET t.v = src.v
//	  WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.v)
//
// Mapped to:
//
//	INSERT INTO t (k, v) SELECT src.k, src.v FROM src
//	ON CONFLICT (k) DO UPDATE SET v = excluded.v
// ---------------------------------------------------------------------------

var reMergeUpsert = regexp.MustCompile(`(?is)^\s*MERGE\s+INTO\s+(\S+)(?:\s+(?:AS\s+)?(\w+))?\s+USING\s+(\S+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.+?)\s+WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+(.+?)\s+WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*\(([^)]+)\)\s+VALUES\s*\(([^)]+)\)\s*;?\s*$`)

func rewriteMerge(sql string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(sql), ";")
	m := reMergeUpsert.FindStringSubmatch(trimmed)
	if m == nil {
		return sql
	}
	targetTable := m[1]
	targetAlias := m[2]
	if targetAlias == "" {
		targetAlias = targetTable
	}
	sourceTable := m[3]
	sourceAlias := m[4]
	if sourceAlias == "" {
		sourceAlias = sourceTable
	}
	onClause := m[5]
	updateSet := m[6]
	insertCols := strings.TrimSpace(m[7])
	insertVals := strings.TrimSpace(m[8])

	// Extract the conflict key from the ON clause. We only support a single
	// equality predicate of the form "target.<col> = source.<col>"; anything
	// else returns the original SQL so DuckDB can attempt its own parse.
	keyMatch := regexp.MustCompile(`(?is)^\s*(?:[A-Za-z_][\w]*\.)?([A-Za-z_]\w*)\s*=\s*(?:[A-Za-z_][\w]*\.)?([A-Za-z_]\w*)\s*$`).FindStringSubmatch(onClause)
	if keyMatch == nil {
		return sql
	}
	conflictCol := keyMatch[1]

	// Drop target-alias prefixes from the UPDATE SET clause and translate
	// "src.col" → "excluded.col" so DuckDB's ON CONFLICT semantics resolve.
	cleanSet := updateSet
	if targetAlias != "" {
		cleanSet = regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(targetAlias)+`\.`).ReplaceAllString(cleanSet, "")
	}
	if sourceAlias != "" {
		cleanSet = regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(sourceAlias)+`\.`).ReplaceAllString(cleanSet, "excluded.")
	}

	// Keep the source alias in the FROM clause so the SELECT list's
	// `s.col` references resolve. DuckDB rejects `SELECT s.id FROM source`
	// when no alias is bound; binding it here keeps the rewrite faithful.
	fromClause := sourceTable
	if sourceAlias != "" && sourceAlias != sourceTable {
		fromClause = sourceTable + " AS " + sourceAlias
	}
	return "INSERT INTO " + targetTable + " (" + insertCols + ") SELECT " + insertVals +
		" FROM " + fromClause + " ON CONFLICT (" + conflictCol + ") DO UPDATE SET " + cleanSet
}
