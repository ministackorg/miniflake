package orchestrator

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/stage"
	"github.com/miniflakedb/miniflake/internal/task"
)

// Each handler returns the (result, handled=true, error) triple required by
// handleSpecialMarkers. They parse the captured original SQL and dispatch to
// the in-process engine.

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

var reParseCreateStream = regexp.MustCompile(`(?is)CREATE\s+(OR\s+REPLACE\s+)?STREAM\s+(\S+)\s+ON\s+TABLE\s+(\S+)(?:\s+APPEND_ONLY\s*=\s*TRUE)?`)

func (o *Orchestrator) handleCreateStream(sess *session.Session, originalSQL string) (*QueryResult, bool, error) {
	m := reParseCreateStream.FindStringSubmatch(originalSQL)
	if m == nil {
		return nil, true, fmt.Errorf("CREATE STREAM: unable to parse %q", originalSQL)
	}
	streamName := strings.Trim(m[2], `"'`)
	tableName := strings.Trim(m[3], `"'`)
	streamType := "STANDARD"
	if strings.Contains(strings.ToUpper(originalSQL), "APPEND_ONLY") {
		streamType = "APPEND_ONLY"
	}
	if err := o.streamEng.CreateStream(sess.Database, sess.Schema, streamName, tableName, streamType); err != nil {
		return nil, true, err
	}
	return statusResult("Stream " + streamName + " successfully created."), true, nil
}

func (o *Orchestrator) handleDropStream(sess *session.Session, name string, ifExists bool) (*QueryResult, bool, error) {
	if err := o.streamEng.DropStream(sess.Database, sess.Schema, name); err != nil {
		if ifExists {
			return statusResult("Stream " + name + " does not exist (IF EXISTS)."), true, nil
		}
		return nil, true, err
	}
	return statusResult("Stream " + name + " successfully dropped."), true, nil
}

func (o *Orchestrator) handleShowStreams(sess *session.Session, scope string) (*QueryResult, bool, error) {
	db, schema := scopeOrSession(scope, sess)
	streams := o.streamEng.ShowStreams(db, schema)
	rows := make([][]interface{}, 0, len(streams))
	for _, s := range streams {
		rows = append(rows, []interface{}{
			s.CreatedAt.UTC(), s.Name, s.DatabaseName, s.SchemaName,
			s.TableName, s.Type, s.Stale,
		})
	}
	return &QueryResult{
		Columns:       []string{"created_on", "name", "database_name", "schema_name", "table_name", "type", "stale"},
		Rows:          rows,
		StatementType: "SHOW",
	}, true, nil
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

var reParseCreateTask = regexp.MustCompile(`(?is)CREATE\s+(OR\s+REPLACE\s+)?TASK\s+(\S+)`)
var reTaskWarehouse = regexp.MustCompile(`(?is)WAREHOUSE\s*=\s*(\S+)`)
var reTaskSchedule = regexp.MustCompile(`(?is)SCHEDULE\s*=\s*'([^']+)'`)
var reTaskAfter = regexp.MustCompile(`(?is)\bAFTER\s+(\S+?)(?:\s|$)`)
var reTaskWhen = regexp.MustCompile(`(?is)\bWHEN\s+(.+?)\s+AS\s+`)
var reTaskAs = regexp.MustCompile(`(?is)\bAS\s+(.+)$`)

func (o *Orchestrator) handleCreateTask(sess *session.Session, originalSQL string) (*QueryResult, bool, error) {
	m := reParseCreateTask.FindStringSubmatch(originalSQL)
	if m == nil {
		return nil, true, fmt.Errorf("CREATE TASK: unable to parse name from %q", originalSQL)
	}
	name := strings.Trim(m[2], `"'`)

	warehouse := ""
	if w := reTaskWarehouse.FindStringSubmatch(originalSQL); w != nil {
		warehouse = strings.Trim(w[1], `"'`)
	}
	schedule := ""
	if s := reTaskSchedule.FindStringSubmatch(originalSQL); s != nil {
		schedule = s[1]
	}
	predecessor := ""
	if a := reTaskAfter.FindStringSubmatch(originalSQL); a != nil {
		predecessor = strings.Trim(a[1], `"'`)
	}
	whenCond := ""
	if w := reTaskWhen.FindStringSubmatch(originalSQL); w != nil {
		whenCond = strings.TrimSpace(w[1])
	}
	body := ""
	if as := reTaskAs.FindStringSubmatch(originalSQL); as != nil {
		body = strings.TrimSpace(as[1])
	}
	if body == "" {
		return nil, true, fmt.Errorf("CREATE TASK %s: missing AS <body>", name)
	}

	if err := o.taskSched.CreateTask(sess.Database, sess.Schema, name, body, schedule, warehouse, predecessor, whenCond); err != nil {
		return nil, true, err
	}
	return statusResult("Task " + name + " successfully created."), true, nil
}

func (o *Orchestrator) handleDropTask(sess *session.Session, name string, ifExists bool) (*QueryResult, bool, error) {
	if err := o.taskSched.DropTask(sess.Database, sess.Schema, name); err != nil {
		if ifExists {
			return statusResult("Task " + name + " does not exist (IF EXISTS)."), true, nil
		}
		return nil, true, err
	}
	return statusResult("Task " + name + " successfully dropped."), true, nil
}

func (o *Orchestrator) handleAlterTask(sess *session.Session, name, action string) (*QueryResult, bool, error) {
	var state task.TaskState
	switch strings.ToUpper(action) {
	case "RESUME":
		state = task.TaskStarted
	case "SUSPEND":
		state = task.TaskSuspended
	default:
		return nil, true, fmt.Errorf("ALTER TASK %s: unknown action %q", name, action)
	}
	if err := o.taskSched.AlterTask(sess.Database, sess.Schema, name, state); err != nil {
		return nil, true, err
	}
	return statusResult("Task " + name + " successfully " + strings.ToLower(action) + "ed."), true, nil
}

func (o *Orchestrator) handleShowTasks(sess *session.Session, scope string) (*QueryResult, bool, error) {
	db, schema := scopeOrSession(scope, sess)
	tasks := o.taskSched.ShowTasks(db, schema)
	rows := make([][]interface{}, 0, len(tasks))
	for _, t := range tasks {
		var lastRun, nextRun interface{}
		if t.LastRunAt != nil {
			lastRun = t.LastRunAt.UTC()
		}
		if t.NextRunAt != nil {
			nextRun = t.NextRunAt.UTC()
		}
		rows = append(rows, []interface{}{
			t.CreatedAt.UTC(), t.Name, t.DatabaseName, t.SchemaName,
			t.Warehouse, t.Schedule, string(t.State), t.Predecessor, lastRun, nextRun,
		})
	}
	return &QueryResult{
		Columns: []string{
			"created_on", "name", "database_name", "schema_name",
			"warehouse", "schedule", "state", "predecessor", "last_run_at", "next_run_at",
		},
		Rows:          rows,
		StatementType: "SHOW",
	}, true, nil
}

func (o *Orchestrator) handleExecuteTask(ctx context.Context, sess *session.Session, name string) (*QueryResult, bool, error) {
	t, err := o.taskSched.GetTask(sess.Database, sess.Schema, name)
	if err != nil {
		return nil, true, err
	}
	if _, execErr := o.engine.ExecNoResult(ctx, t.SQLText); execErr != nil {
		return nil, true, fmt.Errorf("EXECUTE TASK %s: %w", name, execErr)
	}
	return statusResult("Task " + name + " executed."), true, nil
}

// ---------------------------------------------------------------------------
// Pipes
// ---------------------------------------------------------------------------

var reParseCreatePipe = regexp.MustCompile(`(?is)CREATE\s+(OR\s+REPLACE\s+)?PIPE\s+(\S+)`)
var rePipeAutoIngest = regexp.MustCompile(`(?is)AUTO_INGEST\s*=\s*TRUE`)
var rePipeAs = regexp.MustCompile(`(?is)\bAS\s+(.+)$`)

func (o *Orchestrator) handleCreatePipe(sess *session.Session, originalSQL string) (*QueryResult, bool, error) {
	m := reParseCreatePipe.FindStringSubmatch(originalSQL)
	if m == nil {
		return nil, true, fmt.Errorf("CREATE PIPE: unable to parse name from %q", originalSQL)
	}
	name := strings.Trim(m[2], `"'`)
	autoIngest := rePipeAutoIngest.MatchString(originalSQL)
	copyStmt := ""
	if as := rePipeAs.FindStringSubmatch(originalSQL); as != nil {
		copyStmt = strings.TrimSpace(as[1])
	}
	if copyStmt == "" {
		return nil, true, fmt.Errorf("CREATE PIPE %s: missing AS <copy_statement>", name)
	}
	if err := o.snowpipeEng.CreatePipe(sess.Database, sess.Schema, name, copyStmt, autoIngest); err != nil {
		return nil, true, err
	}
	return statusResult("Pipe " + name + " successfully created."), true, nil
}

func (o *Orchestrator) handleDropPipe(sess *session.Session, name string, ifExists bool) (*QueryResult, bool, error) {
	if err := o.snowpipeEng.DropPipe(sess.Database, sess.Schema, name); err != nil {
		if ifExists {
			return statusResult("Pipe " + name + " does not exist (IF EXISTS)."), true, nil
		}
		return nil, true, err
	}
	return statusResult("Pipe " + name + " successfully dropped."), true, nil
}

func (o *Orchestrator) handleShowPipes(sess *session.Session, scope string) (*QueryResult, bool, error) {
	db, schema := scopeOrSession(scope, sess)
	pipes := o.snowpipeEng.ShowPipes(db, schema)
	rows := make([][]interface{}, 0, len(pipes))
	for _, p := range pipes {
		rows = append(rows, []interface{}{
			p.CreatedAt.UTC(), p.Name, p.Database, p.Schema, p.Definition, string(p.State),
		})
	}
	return &QueryResult{
		Columns:       []string{"created_on", "name", "database_name", "schema_name", "definition", "state"},
		Rows:          rows,
		StatementType: "SHOW",
	}, true, nil
}

// ---------------------------------------------------------------------------
// Time travel: UNDROP
// ---------------------------------------------------------------------------

func (o *Orchestrator) handleUndropTable(ctx context.Context, sess *session.Session, tableName string) (*QueryResult, bool, error) {
	tableName = strings.Trim(tableName, `"'`)
	snapshotKey, err := o.timeTravelEng.Undrop(sess.Database, sess.Schema, tableName)
	if err != nil {
		return nil, true, fmt.Errorf("UNDROP TABLE %s: %w", tableName, err)
	}
	// Restore from the captured snapshot. The timetravel engine has already
	// written a parquet file at snapshotKey; CTAS reads it back into the
	// recreated table.
	createSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')",
		tableName, escapeStringLiteral(snapshotKey))
	if _, err := o.engine.ExecNoResult(ctx, createSQL); err != nil {
		return nil, true, fmt.Errorf("UNDROP TABLE %s: restore failed: %w", tableName, err)
	}
	return statusResult("Table " + tableName + " successfully restored."), true, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// scopeOrSession parses the optional `IN SCHEMA` qualifier from a SHOW
// command. Empty scope falls back to the session's current db/schema.
func scopeOrSession(scope string, sess *session.Session) (db, schema string) {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return sess.Database, sess.Schema
	}
	parts := strings.SplitN(scope, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	// Just a schema name — keep the session's database.
	return sess.Database, parts[0]
}

func statusResult(status string) *QueryResult {
	return &QueryResult{
		Columns:       []string{"status"},
		Rows:          [][]interface{}{{status}},
		StatementType: "DDL",
	}
}

func escapeStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// InsertPipeFiles is the entry point the HTTP server calls when a client
// POSTs to /v1/data/pipes/{db}/{schema}/{pipe}/insertFiles. It's exposed
// here (rather than directly on snowpipe.Engine) so the server doesn't have
// to depend on every subsystem.
func (o *Orchestrator) InsertPipeFiles(ctx context.Context, db, schema, pipeName string, files []string) error {
	return o.snowpipeEng.InsertFiles(ctx, db, schema, pipeName, files)
}

// ---------------------------------------------------------------------------
// Time travel — AT / BEFORE marker expansion
// ---------------------------------------------------------------------------

var reMarkerAtOffset = regexp.MustCompile(`(?s)/\*\s*MINIFLAKE_AT_OFFSET\s+(\S+)\s+(-?\d+)\s*\*/`)
var reMarkerAtTimestamp = regexp.MustCompile(`(?s)/\*\s*MINIFLAKE_AT_TIMESTAMP\s+(\S+)\s+(\S+)\s*\*/`)

func (o *Orchestrator) expandTimeTravelMarkers(sess *session.Session, sql string) (string, error) {
	if o.timeTravelEng == nil {
		return sql, nil
	}
	var firstErr error
	sql = reMarkerAtOffset.ReplaceAllStringFunc(sql, func(match string) string {
		if firstErr != nil {
			return match
		}
		m := reMarkerAtOffset.FindStringSubmatch(match)
		tableName := strings.Trim(m[1], `"'`)
		var seconds int64
		fmt.Sscanf(m[2], "%d", &seconds)
		if seconds < 0 {
			seconds = -seconds
		}
		file, err := o.timeTravelEng.QueryAtOffset(sess.Database, sess.Schema, tableName, seconds)
		if err != nil {
			firstErr = fmt.Errorf("AT(OFFSET => -%d): %w", seconds, err)
			return match
		}
		return "read_parquet('" + escapeStringLiteral(file) + "')"
	})
	if firstErr != nil {
		return sql, firstErr
	}
	sql = reMarkerAtTimestamp.ReplaceAllStringFunc(sql, func(match string) string {
		if firstErr != nil {
			return match
		}
		m := reMarkerAtTimestamp.FindStringSubmatch(match)
		tableName := strings.Trim(m[1], `"'`)
		ts, err := time.Parse(time.RFC3339, m[2])
		if err != nil {
			firstErr = fmt.Errorf("AT(TIMESTAMP => '%s'): %w", m[2], err)
			return match
		}
		file, err := o.timeTravelEng.QueryAtTimestamp(sess.Database, sess.Schema, tableName, ts)
		if err != nil {
			firstErr = fmt.Errorf("AT(TIMESTAMP => '%s'): %w", m[2], err)
			return match
		}
		return "read_parquet('" + escapeStringLiteral(file) + "')"
	})
	return sql, firstErr
}

// ---------------------------------------------------------------------------
// Stage file commands: LIST/LS and REMOVE/RM
// ---------------------------------------------------------------------------
//
// The rewriter wraps `LIST @stage` / `REMOVE @stage` in markers (see
// rewriteStageCommands) because DuckDB has no @stage concept. Both resolve
// their reference through the shared resolver in stageref.go, so a given
// reference names the same files for LIST, REMOVE, PUT, GET and COPY INTO.
//
// PATTERN = '<regex>' is optional and accepted on both statements (RE2;
// Snowflake documents Java Pattern semantics — see ListMetaFiles for the
// whole-path anchoring).
var (
	reListStage   = regexp.MustCompile(`(?i)^(?:LIST|LS)\s+@(\S+)`)
	reRemoveStage = regexp.MustCompile(`(?i)^(?:REMOVE|RM)\s+@(\S+)`)
	reListPattern = regexp.MustCompile(`(?i)\bPATTERN\s*=\s*'([^']*)'`)
)

// listTimeFormat matches the last_modified rendering of real Snowflake's LIST
// output. "GMT" is a literal here, not a layout element; net/http relies on
// the same trick for http.TimeFormat.
const listTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

func (o *Orchestrator) handleListStage(sess *session.Session, originalSQL string) (*QueryResult, bool, error) {
	m := reListStage.FindStringSubmatch(originalSQL)
	if m == nil {
		return nil, true, fmt.Errorf("LIST: unable to parse %q", originalSQL)
	}

	meta, subpath, namePrefix, err := o.resolveStage(sess, m[1])
	if err != nil {
		return nil, true, fmt.Errorf("LIST: %w", err)
	}

	pattern := ""
	if pm := reListPattern.FindStringSubmatch(originalSQL); pm != nil {
		pattern = pm[1]
	}

	files, err := o.stageMgr.ListMetaFiles(meta, stage.ListOptions{
		Prefix:   subpath,
		Regex:    pattern,
		Checksum: true,
	})
	if err != nil {
		return nil, true, err
	}

	rows := make([][]interface{}, 0, len(files))
	for _, f := range files {
		rows = append(rows, []interface{}{
			namePrefix + "/" + f.Name,
			f.Size,
			f.MD5,
			f.ModTime.UTC().Format(listTimeFormat),
		})
	}
	return &QueryResult{
		Columns:       []string{"name", "size", "md5", "last_modified"},
		Rows:          rows,
		StatementType: "LIST",
	}, true, nil
}

func (o *Orchestrator) handleRemoveStage(sess *session.Session, originalSQL string) (*QueryResult, bool, error) {
	m := reRemoveStage.FindStringSubmatch(originalSQL)
	if m == nil {
		return nil, true, fmt.Errorf("REMOVE: unable to parse %q", originalSQL)
	}

	meta, subpath, namePrefix, err := o.resolveStage(sess, m[1])
	if err != nil {
		return nil, true, fmt.Errorf("REMOVE: %w", err)
	}

	pattern := ""
	if pm := reListPattern.FindStringSubmatch(originalSQL); pm != nil {
		pattern = pm[1]
	}

	files, err := o.stageMgr.ListMetaFiles(meta, stage.ListOptions{
		Prefix: subpath,
		Regex:  pattern,
	})
	if err != nil {
		return nil, true, fmt.Errorf("REMOVE: %w", err)
	}

	rows := make([][]interface{}, 0, len(files))
	for _, f := range files {
		result := "removed"
		if rmErr := o.stageMgr.RemoveMetaFile(meta, f.Name); rmErr != nil {
			result = "failed: " + rmErr.Error()
		}
		rows = append(rows, []interface{}{namePrefix + "/" + f.Name, result})
	}
	return &QueryResult{
		Columns:       []string{"name", "result"},
		Rows:          rows,
		StatementType: "REMOVE",
	}, true, nil
}

// ---------------------------------------------------------------------------
// SHOW PARAMETERS
// ---------------------------------------------------------------------------

// snowflakeParameter is one row of SHOW PARAMETERS output.
// Columns match Snowflake: key, value, default, level, description, type.
type snowflakeParameter struct {
	Key         string
	Value       string
	Default     string
	Level       string
	Description string
	Type        string
}

// defaultSessionParameters are the session-level defaults real Snowflake
// exposes. Drivers (JDBC, .NET, Python, gosnowflake) probe these at connect
// time; an empty result set makes some of them fail.
//
// Values follow Snowflake's documented defaults. MiniFlake does not honour
// ALTER SESSION yet, so value always equals default and level is empty.
var defaultSessionParameters = []snowflakeParameter{
	{"ABORT_DETACHED_QUERY", "false", "false", "", "abort queries when the client disconnects", "BOOLEAN"},
	{"AUTOCOMMIT", "true", "true", "", "autocommit mode", "BOOLEAN"},
	{"BINARY_OUTPUT_FORMAT", "HEX", "HEX", "", "display format for binary", "STRING"},
	{"CLIENT_SESSION_KEEP_ALIVE", "false", "false", "", "keep session alive between requests", "BOOLEAN"},
	{"CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "3600", "3600", "", "heartbeat frequency in seconds", "NUMBER"},
	{"DATE_OUTPUT_FORMAT", "YYYY-MM-DD", "YYYY-MM-DD", "", "display format for date", "STRING"},
	{"ERROR_ON_NONDETERMINISTIC_MERGE", "true", "true", "", "error on nondeterministic MERGE", "BOOLEAN"},
	{"ERROR_ON_NONDETERMINISTIC_UPDATE", "false", "false", "", "error on nondeterministic UPDATE", "BOOLEAN"},
	{"GEOGRAPHY_OUTPUT_FORMAT", "GeoJSON", "GeoJSON", "", "display format for GEOGRAPHY", "STRING"},
	{"JSON_INDENT", "2", "2", "", "indent for JSON output", "NUMBER"},
	{"LOCK_TIMEOUT", "43200", "43200", "", "lock timeout in seconds", "NUMBER"},
	{"QUERY_TAG", "", "", "", "query tag", "STRING"},
	{"QUOTED_IDENTIFIERS_IGNORE_CASE", "false", "false", "", "case-insensitive quoted identifiers", "BOOLEAN"},
	{"ROWS_PER_RESULTSET", "0", "0", "", "max rows per result set (0 = unlimited)", "NUMBER"},
	{"SIMULATED_DATA_SHARING_CONSUMER", "", "", "", "simulated data sharing consumer", "STRING"},
	{"STATEMENT_TIMEOUT_IN_SECONDS", "0", "0", "", "statement timeout in seconds (0 = none)", "NUMBER"},
	{"TIMESTAMP_LTZ_OUTPUT_FORMAT", "", "", "", "display format for TIMESTAMP_LTZ", "STRING"},
	{"TIMESTAMP_NTZ_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF3", "YYYY-MM-DD HH24:MI:SS.FF3", "", "display format for TIMESTAMP_NTZ", "STRING"},
	{"TIMESTAMP_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM", "", "display format for timestamps", "STRING"},
	{"TIMESTAMP_TZ_OUTPUT_FORMAT", "", "", "", "display format for TIMESTAMP_TZ", "STRING"},
	{"TIMEZONE", "America/Los_Angeles", "America/Los_Angeles", "", "time zone", "STRING"},
	{"TIME_INPUT_FORMAT", "AUTO", "AUTO", "", "input format for time", "STRING"},
	{"TIME_OUTPUT_FORMAT", "HH24:MI:SS", "HH24:MI:SS", "", "display format for time", "STRING"},
	{"TRANSACTION_ABORT_ON_ERROR", "false", "false", "", "abort transaction on statement error", "BOOLEAN"},
	{"TWO_DIGIT_CENTURY_START", "1970", "1970", "", "century start for two-digit years", "NUMBER"},
	{"UNSUPPORTED_DDL_ACTION", "ignore", "ignore", "", "action on unsupported DDL", "STRING"},
	{"USE_CACHED_RESULT", "true", "true", "", "reuse cached query results", "BOOLEAN"},
}

// defaultAccountParameters are account-level extras returned by
// SHOW PARAMETERS IN ACCOUNT. Session parameters are included too, matching
// Snowflake's IN ACCOUNT behaviour.
var defaultAccountParameters = []snowflakeParameter{
	{"MAX_CONCURRENCY_LEVEL", "8", "8", "", "max concurrency level", "NUMBER"},
	{"PERIODIC_DATA_REKEYING", "true", "true", "", "periodic data rekeying", "BOOLEAN"},
}

func matchLike(pattern, value string) bool {
	pattern = strings.ToUpper(pattern)
	value = strings.ToUpper(value)
	// SQL LIKE: % → .*  _ → .  ; escape is not required for the probe patterns
	// drivers send.
	var b strings.Builder
	b.WriteString("(?s)^")
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '.', '+', '*', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\':
			b.WriteByte('\\')
			b.WriteByte(pattern[i])
		default:
			b.WriteByte(pattern[i])
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(value)
}

func listParameters(likePattern, scope string) []snowflakeParameter {
	params := make([]snowflakeParameter, len(defaultSessionParameters))
	copy(params, defaultSessionParameters)
	if strings.EqualFold(scope, "ACCOUNT") {
		params = append(params, defaultAccountParameters...)
	}

	if likePattern == "" {
		return params
	}
	out := make([]snowflakeParameter, 0, len(params))
	for _, p := range params {
		if matchLike(likePattern, p.Key) {
			out = append(out, p)
		}
	}
	return out
}

func (o *Orchestrator) handleShowParameters(likePattern, scope string) (*QueryResult, bool, error) {
	scope = strings.ToUpper(strings.TrimSpace(scope))
	if scope == "" {
		scope = "SESSION"
	}
	switch scope {
	case "SESSION", "ACCOUNT":
		// Supported.
	case "USER", "WAREHOUSE", "DATABASE", "SCHEMA", "TASK", "TABLE":
		// Object scopes are accepted for syntax parity but MiniFlake has no
		// per-object parameter overrides yet, so return the session defaults.
	default:
		return nil, true, fmt.Errorf("SHOW PARAMETERS: unsupported scope %q", scope)
	}

	params := listParameters(likePattern, scope)
	rows := make([][]interface{}, 0, len(params))
	for _, p := range params {
		rows = append(rows, []interface{}{p.Key, p.Value, p.Default, p.Level, p.Description, p.Type})
	}
	return &QueryResult{
		Columns:       []string{"key", "value", "default", "level", "description", "type"},
		Rows:          rows,
		StatementType: "SHOW",
	}, true, nil
}
