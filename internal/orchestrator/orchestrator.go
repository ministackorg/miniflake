// Package orchestrator is the central routing layer that ties all MiniFlake
// subsystems together. It receives SQL from the server layer, rewrites it,
// detects the statement type, and dispatches to the correct subsystem(s).
package orchestrator

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/miniflakedb/miniflake/internal/catalog"
	"github.com/miniflakedb/miniflake/internal/clone"
	"github.com/miniflakedb/miniflake/internal/copyinto"
	"github.com/miniflakedb/miniflake/internal/engine"
	"github.com/miniflakedb/miniflake/internal/rbac"
	"github.com/miniflakedb/miniflake/internal/rewriter"
	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/snowpipe"
	"github.com/miniflakedb/miniflake/internal/stage"
	"github.com/miniflakedb/miniflake/internal/stream"
	"github.com/miniflakedb/miniflake/internal/task"
	"github.com/miniflakedb/miniflake/internal/timetravel"
	"github.com/miniflakedb/miniflake/internal/udf"
)

// QueryResult holds the output of an executed SQL statement.
type QueryResult struct {
	Columns       []string
	Rows          [][]interface{}
	RowsAffected  int64
	StatementType string // SELECT, DML, DDL, USE, etc.
}

// Orchestrator is the brain of MiniFlake. It holds every subsystem and routes
// SQL statements to the right place.
type Orchestrator struct {
	engine        *engine.Engine
	catalog       *catalog.Catalog
	stageMgr      *stage.Manager
	streamEng     *stream.Engine
	taskSched     *task.Scheduler
	timeTravelEng *timetravel.Engine
	cloneEng      *clone.Engine
	udfReg        *udf.Registry
	rbacEng       *rbac.Engine
	snowpipeEng   *snowpipe.Engine
}

// New creates a fully wired Orchestrator.
func New(
	eng *engine.Engine,
	cat *catalog.Catalog,
	stageMgr *stage.Manager,
	streamEng *stream.Engine,
	taskSched *task.Scheduler,
	timeTravelEng *timetravel.Engine,
	cloneEng *clone.Engine,
	udfReg *udf.Registry,
	rbacEng *rbac.Engine,
	snowpipeEng *snowpipe.Engine,
) *Orchestrator {
	return &Orchestrator{
		engine:        eng,
		catalog:       cat,
		stageMgr:      stageMgr,
		streamEng:     streamEng,
		taskSched:     taskSched,
		timeTravelEng: timeTravelEng,
		cloneEng:      cloneEng,
		udfReg:        udfReg,
		rbacEng:       rbacEng,
		snowpipeEng:   snowpipeEng,
	}
}

// ExecuteSQL is the main entry point. It rewrites the SQL, detects the
// statement type, and routes to the correct subsystem(s).
func (o *Orchestrator) ExecuteSQL(ctx context.Context, sess *session.Session, sql string) (*QueryResult, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return &QueryResult{StatementType: "EMPTY"}, nil
	}

	// Step 1: rewrite Snowflake SQL to DuckDB-compatible SQL.
	rewritten, err := rewriter.Rewrite(sql)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: rewrite: %w", err)
	}

	// Step 2: check for special markers left by the rewriter.
	if result, handled, err := o.handleSpecialMarkers(ctx, sess, rewritten); handled {
		return result, err
	}

	upper := strings.ToUpper(strings.TrimSpace(rewritten))

	// Step 3: route DDL.
	if result, handled, err := o.handleDDL(ctx, sess, rewritten, upper); handled {
		return result, err
	}

	// Step 4: route DML.
	if result, handled, err := o.handleDML(ctx, sess, rewritten, upper); handled {
		return result, err
	}

	// Step 5: route transactions.
	if result, handled, err := o.handleTransaction(ctx, rewritten, upper); handled {
		return result, err
	}

	// Step 6: route queries (SELECT, SHOW, DESCRIBE, WITH, EXPLAIN).
	if result, handled, err := o.handleQuery(ctx, rewritten, upper); handled {
		return result, err
	}

	// Fallback: pass through to engine as a DML/DDL.
	affected, err := o.engine.ExecNoResult(ctx, rewritten)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: exec: %w", err)
	}
	return &QueryResult{
		RowsAffected:  affected,
		StatementType: "UNKNOWN",
	}, nil
}

// Execute implements the server.QueryEngine interface for query execution.
func (o *Orchestrator) Execute(ctx context.Context, sql string, args ...interface{}) ([]string, [][]interface{}, error) {
	return o.engine.Execute(ctx, sql, args...)
}

// ExecNoResult implements the server.QueryEngine interface for DML/DDL.
func (o *Orchestrator) ExecNoResult(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	return o.engine.ExecNoResult(ctx, sql, args...)
}

// ---------------------------------------------------------------------------
// Special markers
// ---------------------------------------------------------------------------

var (
	reMarkerUse      = regexp.MustCompile(`(?s)/\*\s*MINIFLAKE_USE_(DATABASE|SCHEMA|WAREHOUSE|ROLE)\s+(\S+)\s*\*/`)
	reMarkerCopyInto = regexp.MustCompile(`(?s)/\*\s*MINIFLAKE_COPY_INTO\s+(.*?)\s*\*/`)
	reMarkerPut      = regexp.MustCompile(`(?s)/\*\s*MINIFLAKE_PUT\s+(.*?)\s*\*/`)
	reMarkerGet      = regexp.MustCompile(`(?s)/\*\s*MINIFLAKE_GET\s+(.*?)\s*\*/`)
)

func (o *Orchestrator) handleSpecialMarkers(ctx context.Context, sess *session.Session, sql string) (*QueryResult, bool, error) {
	// USE DATABASE/SCHEMA/WAREHOUSE/ROLE
	if m := reMarkerUse.FindStringSubmatch(sql); m != nil {
		kind := strings.ToUpper(m[1])
		name := strings.Trim(m[2], "\"'` ")
		return o.handleUse(ctx, sess, kind, name)
	}

	// COPY INTO — route to the copyinto executor. The marker captures the
	// original Snowflake-shape statement (the rewriter wraps it in
	// MINIFLAKE_COPY_INTO so DuckDB doesn't see it); we parse + execute here.
	if m := reMarkerCopyInto.FindStringSubmatch(sql); m != nil {
		originalSQL := strings.TrimSpace(m[1])
		direction, tableName, stagePath, format, options, perr := copyinto.ParseCopyStatement(originalSQL)
		if perr != nil {
			return nil, true, fmt.Errorf("COPY INTO: %w", perr)
		}
		exec := copyinto.NewExecutor(
			o.engine.ExecNoResult,
			o.engine.Execute,
			o.stageMgr,
		)
		var results []copyinto.CopyResult
		var execErr error
		switch direction {
		case copyinto.LoadIntoTable:
			results, execErr = exec.ExecuteLoad(ctx, tableName, stagePath, format, options)
		case copyinto.UnloadToStage:
			results, execErr = exec.ExecuteUnload(ctx, tableName, stagePath, format, options)
		default:
			return nil, true, fmt.Errorf("COPY INTO: unknown direction")
		}
		if execErr != nil {
			return nil, true, execErr
		}
		// Snowflake's COPY INTO returns one row per file with status columns.
		rows := make([][]interface{}, 0, len(results))
		for _, r := range results {
			rows = append(rows, []interface{}{
				r.File, r.Status, r.RowsLoaded, r.RowsParsed,
				r.ErrorsSeen, r.FirstError,
			})
		}
		return &QueryResult{
			Columns: []string{
				"file", "status", "rows_loaded", "rows_parsed",
				"errors_seen", "first_error",
			},
			Rows:          rows,
			StatementType: "COPY",
		}, true, nil
	}

	// PUT
	if m := reMarkerPut.FindStringSubmatch(sql); m != nil {
		return &QueryResult{
			Columns:       []string{"status"},
			Rows:          [][]interface{}{{"PUT statement acknowledged (stub)"}},
			StatementType: "PUT",
		}, true, nil
	}

	// GET
	if m := reMarkerGet.FindStringSubmatch(sql); m != nil {
		return &QueryResult{
			Columns:       []string{"status"},
			Rows:          [][]interface{}{{"GET statement acknowledged (stub)"}},
			StatementType: "GET",
		}, true, nil
	}

	return nil, false, nil
}

func (o *Orchestrator) handleUse(_ context.Context, sess *session.Session, kind, name string) (*QueryResult, bool, error) {
	switch kind {
	case "DATABASE":
		sess.Database = name
		o.engine.SetCurrentDatabase(name)
	case "SCHEMA":
		sess.Schema = name
		o.engine.SetCurrentSchema(name)
	case "WAREHOUSE":
		sess.Warehouse = name
	case "ROLE":
		sess.Role = name
	default:
		return nil, false, fmt.Errorf("orchestrator: unknown USE kind %q", kind)
	}
	return &QueryResult{
		Columns:       []string{"status"},
		Rows:          [][]interface{}{{"Statement executed successfully."}},
		StatementType: "USE",
	}, true, nil
}

// ---------------------------------------------------------------------------
// DDL routing
// ---------------------------------------------------------------------------

// Regex patterns for DDL detection.
var (
	reCreateDatabase  = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?DATABASE\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)`)
	reDropDatabase    = regexp.MustCompile(`(?i)^DROP\s+DATABASE\s+(IF\s+EXISTS\s+)?(\S+)`)
	reCreateSchema    = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?SCHEMA\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)`)
	reDropSchema      = regexp.MustCompile(`(?i)^DROP\s+SCHEMA\s+(IF\s+EXISTS\s+)?(\S+)`)
	reCreateTable     = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?(TEMPORARY\s+|TRANSIENT\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)`)
	reDropTable       = regexp.MustCompile(`(?i)^DROP\s+TABLE\s+(IF\s+EXISTS\s+)?(\S+)`)
	reCreateView      = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+`)
	reCreateStream    = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?STREAM\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)\s+ON\s+TABLE\s+(\S+)`)
	reDropStream      = regexp.MustCompile(`(?i)^DROP\s+STREAM\s+(IF\s+EXISTS\s+)?(\S+)`)
	reCreateTask      = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?TASK\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)`)
	reDropTask        = regexp.MustCompile(`(?i)^DROP\s+TASK\s+(IF\s+EXISTS\s+)?(\S+)`)
	reAlterTaskResume = regexp.MustCompile(`(?i)^ALTER\s+TASK\s+(\S+)\s+RESUME`)
	reAlterTaskSusp   = regexp.MustCompile(`(?i)^ALTER\s+TASK\s+(\S+)\s+SUSPEND`)
	reCreatePipe      = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?PIPE\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)`)
	reDropPipe        = regexp.MustCompile(`(?i)^DROP\s+PIPE\s+(IF\s+EXISTS\s+)?(\S+)`)
	reCreateStage     = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?STAGE\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)`)
	reDropStage       = regexp.MustCompile(`(?i)^DROP\s+STAGE\s+(IF\s+EXISTS\s+)?(\S+)`)
	reCreateFunc      = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?(FUNCTION|PROCEDURE)\s+(\S+)`)
	reGrant           = regexp.MustCompile(`(?i)^GRANT\s+(\S+)\s+ON\s+(\S+)\s+(\S+)\s+TO\s+ROLE\s+(\S+)`)
	reRevoke          = regexp.MustCompile(`(?i)^REVOKE\s+(\S+)\s+ON\s+(\S+)\s+(\S+)\s+FROM\s+ROLE\s+(\S+)`)
	reCreateRole      = regexp.MustCompile(`(?i)^CREATE\s+ROLE\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)`)
	reCloneTable      = regexp.MustCompile(`(?i)^CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+(\S+)\s+CLONE\s+(\S+)`)
)

func (o *Orchestrator) handleDDL(ctx context.Context, sess *session.Session, sql, upper string) (*QueryResult, bool, error) {
	// CLONE (must check before generic CREATE TABLE)
	if m := reCloneTable.FindStringSubmatch(sql); m != nil {
		dst := cleanIdent(m[2])
		src := cleanIdent(m[3])
		db := sess.Database
		schema := sess.Schema
		err := o.cloneEng.CloneTable(ctx, db, schema, src, db, schema, dst)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// CREATE DATABASE
	if m := reCreateDatabase.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[3])
		if err := o.catalog.CreateDatabase(name, sess.Role); err != nil {
			// if IF NOT EXISTS, ignore already-exists errors
			if m[2] == "" && !strings.Contains(err.Error(), "already exists") {
				return nil, true, err
			}
		}
		// Also create in DuckDB via ATTACH
		_ = o.engine.AttachDatabase(name)
		return ddlResult(), true, nil
	}

	// DROP DATABASE
	if m := reDropDatabase.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[2])
		_ = o.catalog.DropDatabase(name)
		return ddlResult(), true, nil
	}

	// CREATE SCHEMA
	if m := reCreateSchema.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[3])
		db := sess.Database
		// Check if it's qualified: db.schema
		if parts := strings.SplitN(name, ".", 2); len(parts) == 2 {
			db = parts[0]
			name = parts[1]
		}
		_, execErr := o.engine.ExecNoResult(ctx, sql)
		if execErr != nil && m[2] == "" {
			return nil, true, execErr
		}
		_ = o.catalog.CreateSchema(db, name, sess.Role)
		return ddlResult(), true, nil
	}

	// DROP SCHEMA
	if m := reDropSchema.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[2])
		db := sess.Database
		if parts := strings.SplitN(name, ".", 2); len(parts) == 2 {
			db = parts[0]
			name = parts[1]
		}
		_, _ = o.engine.ExecNoResult(ctx, sql)
		_ = o.catalog.DropSchema(db, name)
		return ddlResult(), true, nil
	}

	// CREATE TABLE (non-clone)
	if m := reCreateTable.FindStringSubmatch(sql); m != nil {
		tableName := cleanIdent(m[4])
		_, execErr := o.engine.ExecNoResult(ctx, sql)
		if execErr != nil {
			return nil, true, execErr
		}
		db := sess.Database
		schema := sess.Schema
		if parts := splitQualifiedName(tableName); len(parts) == 3 {
			db = parts[0]
			schema = parts[1]
			tableName = parts[2]
		} else if parts := splitQualifiedName(tableName); len(parts) == 2 {
			schema = parts[0]
			tableName = parts[1]
		}
		// Register in catalog (best-effort; ignore errors for IF NOT EXISTS)
		_ = o.catalog.RegisterTable(db, schema, &catalog.TableMeta{
			Name: tableName,
		})
		return ddlResult(), true, nil
	}

	// DROP TABLE
	if m := reDropTable.FindStringSubmatch(sql); m != nil {
		tableName := cleanIdent(m[2])
		_, execErr := o.engine.ExecNoResult(ctx, sql)
		if execErr != nil && m[1] == "" {
			return nil, true, execErr
		}
		db := sess.Database
		schema := sess.Schema
		if parts := splitQualifiedName(tableName); len(parts) == 3 {
			db = parts[0]
			schema = parts[1]
			tableName = parts[2]
		} else if parts := splitQualifiedName(tableName); len(parts) == 2 {
			schema = parts[0]
			tableName = parts[1]
		}
		_ = o.catalog.DropTable(db, schema, tableName)
		return ddlResult(), true, nil
	}

	// CREATE VIEW
	if reCreateView.MatchString(sql) {
		_, execErr := o.engine.ExecNoResult(ctx, sql)
		if execErr != nil {
			return nil, true, execErr
		}
		return ddlResult(), true, nil
	}

	// CREATE STREAM
	if m := reCreateStream.FindStringSubmatch(sql); m != nil {
		streamName := cleanIdent(m[3])
		tableName := cleanIdent(m[4])
		db := sess.Database
		schema := sess.Schema
		err := o.streamEng.CreateStream(db, schema, streamName, tableName, "STANDARD")
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// DROP STREAM
	if m := reDropStream.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[2])
		err := o.streamEng.DropStream(sess.Database, sess.Schema, name)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// CREATE TASK
	if m := reCreateTask.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[3])
		err := o.taskSched.CreateTask(sess.Database, sess.Schema, name, "", "", "", "", "")
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// DROP TASK
	if m := reDropTask.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[2])
		err := o.taskSched.DropTask(sess.Database, sess.Schema, name)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// ALTER TASK RESUME
	if m := reAlterTaskResume.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[1])
		err := o.taskSched.AlterTask(sess.Database, sess.Schema, name, task.TaskStarted)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// ALTER TASK SUSPEND
	if m := reAlterTaskSusp.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[1])
		err := o.taskSched.AlterTask(sess.Database, sess.Schema, name, task.TaskSuspended)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// CREATE PIPE
	if m := reCreatePipe.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[3])
		err := o.snowpipeEng.CreatePipe(sess.Database, sess.Schema, name, "", false)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// DROP PIPE
	if m := reDropPipe.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[2])
		err := o.snowpipeEng.DropPipe(sess.Database, sess.Schema, name)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// CREATE STAGE
	if m := reCreateStage.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[3])
		err := o.stageMgr.CreateStage(sess.Database, sess.Schema, name, stage.StageInternal, "")
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// DROP STAGE
	if m := reDropStage.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[2])
		err := o.stageMgr.DropStage(sess.Database, sess.Schema, name)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// CREATE FUNCTION / PROCEDURE
	if m := reCreateFunc.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[3])
		err := o.udfReg.Register(&udf.UDF{
			Name:     name,
			Database: sess.Database,
			Schema:   sess.Schema,
			Language: udf.LangSQL,
			Body:     sql,
		})
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// GRANT
	if m := reGrant.FindStringSubmatch(sql); m != nil {
		priv := rbac.Privilege(strings.ToUpper(m[1]))
		objType := rbac.ObjectType(strings.ToUpper(m[2]))
		objName := cleanIdent(m[3])
		roleName := cleanIdent(m[4])
		err := o.rbacEng.GrantPrivilege(priv, objType, objName, roleName, false)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// REVOKE
	if m := reRevoke.FindStringSubmatch(sql); m != nil {
		priv := rbac.Privilege(strings.ToUpper(m[1]))
		objType := rbac.ObjectType(strings.ToUpper(m[2]))
		objName := cleanIdent(m[3])
		roleName := cleanIdent(m[4])
		err := o.rbacEng.RevokePrivilege(priv, objType, objName, roleName)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	// CREATE ROLE
	if m := reCreateRole.FindStringSubmatch(sql); m != nil {
		name := cleanIdent(m[2])
		err := o.rbacEng.CreateRole(name)
		if err != nil {
			return nil, true, err
		}
		return ddlResult(), true, nil
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// DML routing
// ---------------------------------------------------------------------------

func (o *Orchestrator) handleDML(ctx context.Context, sess *session.Session, sql, upper string) (*QueryResult, bool, error) {
	isDML := strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "UPDATE") ||
		strings.HasPrefix(upper, "DELETE") ||
		strings.HasPrefix(upper, "MERGE")

	if !isDML {
		return nil, false, nil
	}

	affected, err := o.engine.ExecNoResult(ctx, sql)
	if err != nil {
		return nil, true, err
	}

	// After successful DML, record changes to streams watching affected tables.
	tableName := extractTableFromDML(upper)
	if tableName != "" {
		db := sess.Database
		schema := sess.Schema
		action := stream.ChangeInsert
		if strings.HasPrefix(upper, "UPDATE") {
			action = stream.ChangeUpdate
		} else if strings.HasPrefix(upper, "DELETE") {
			action = stream.ChangeDelete
		}
		o.streamEng.RecordChange(db, schema, tableName, stream.ChangeRecord{
			Action: action,
		})
	}

	return &QueryResult{
		Columns:       []string{"number of rows inserted", "number of rows updated", "number of rows deleted"},
		Rows:          [][]interface{}{{affected, int64(0), int64(0)}},
		RowsAffected:  affected,
		StatementType: "DML",
	}, true, nil
}

// ---------------------------------------------------------------------------
// Transaction routing
// ---------------------------------------------------------------------------

func (o *Orchestrator) handleTransaction(ctx context.Context, sql, upper string) (*QueryResult, bool, error) {
	isTxn := strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "COMMIT") ||
		strings.HasPrefix(upper, "ROLLBACK")

	if !isTxn {
		return nil, false, nil
	}

	_, err := o.engine.ExecNoResult(ctx, sql)
	if err != nil {
		return nil, true, err
	}
	return &QueryResult{
		Columns:       []string{"status"},
		Rows:          [][]interface{}{{"Statement executed successfully."}},
		StatementType: "TRANSACTION",
	}, true, nil
}

// ---------------------------------------------------------------------------
// Query routing (SELECT, SHOW, DESCRIBE, WITH, EXPLAIN)
// ---------------------------------------------------------------------------

func (o *Orchestrator) handleQuery(ctx context.Context, sql, upper string) (*QueryResult, bool, error) {
	isQuery := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "DESCRIBE") ||
		strings.HasPrefix(upper, "DESC ") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "LIST") ||
		strings.HasPrefix(upper, "WITH")

	if !isQuery {
		return nil, false, nil
	}

	cols, rows, err := o.engine.Execute(ctx, sql)
	if err != nil {
		return nil, true, err
	}
	return &QueryResult{
		Columns:       cols,
		Rows:          rows,
		StatementType: "SELECT",
	}, true, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func ddlResult() *QueryResult {
	return &QueryResult{
		Columns:       []string{"status"},
		Rows:          [][]interface{}{{"Statement executed successfully."}},
		StatementType: "DDL",
	}
}

// cleanIdent strips quoting characters from an identifier.
func cleanIdent(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "\"'`")
	s = strings.TrimRight(s, ";")
	return s
}

// splitQualifiedName splits "db.schema.table" or "schema.table" into parts.
func splitQualifiedName(name string) []string {
	return strings.Split(name, ".")
}

// extractTableFromDML extracts the target table name from an INSERT/UPDATE/DELETE.
var (
	reInsertInto = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+(\S+)`)
	reUpdateTbl  = regexp.MustCompile(`(?i)^UPDATE\s+(\S+)`)
	reDeleteFrom = regexp.MustCompile(`(?i)^DELETE\s+FROM\s+(\S+)`)
	reMergeInto  = regexp.MustCompile(`(?i)^MERGE\s+INTO\s+(\S+)`)
)

func extractTableFromDML(upper string) string {
	for _, re := range []*regexp.Regexp{reInsertInto, reUpdateTbl, reDeleteFrom, reMergeInto} {
		if m := re.FindStringSubmatch(upper); m != nil {
			name := m[1]
			// Return just the table name (last part of qualified name)
			parts := strings.Split(name, ".")
			return parts[len(parts)-1]
		}
	}
	return ""
}
