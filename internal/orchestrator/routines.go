package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/miniflakedb/miniflake/internal/engine"
	"github.com/miniflakedb/miniflake/internal/rewriter"
	"github.com/miniflakedb/miniflake/internal/scripting"
	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/udf"
)

// User-defined functions (#36) and stored procedures (#37).
//
// SQL UDFs become DuckDB macros in the routine's database and schema, so
// DuckDB calls them natively (scalar macros in expressions, table macros in
// FROM). LANGUAGE SQL procedures are kept in the udf registry and run by the
// scripting interpreter, statement by statement, through ExecuteSQL in the
// caller's session. The registry also backs SHOW and DROP. Other languages
// are rejected at CREATE time instead of failing later at call time.

const maxCallDepth = 32

type callDepthKey struct{}

func (o *Orchestrator) handleRoutine(ctx context.Context, sess *session.Session, stmt string) (*QueryResult, bool, error) {
	w := strings.Fields(strings.ToUpper(stmt))
	var res *QueryResult
	var err error
	switch {
	case len(w) > 0 && w[0] == "CREATE":
		res, err = o.createRoutine(ctx, sess, stmt)
	case len(w) > 0 && w[0] == "DROP":
		res, err = o.dropRoutine(ctx, sess, stmt)
	case len(w) > 0 && w[0] == "CALL":
		res, err = o.callProcedure(ctx, sess, stmt)
	default:
		res, err = o.showRoutines(ctx, sess, stmt)
	}
	return res, true, err
}

func compilationError(format string, args ...interface{}) error {
	return &SnowflakeError{Code: "001003", SQLState: "42000", Message: "SQL compilation error:\n" + fmt.Sprintf(format, args...)}
}

// resolveRoutineName turns the written name parts into the database, schema
// and stored name. The database and schema must exist.
func (o *Orchestrator) resolveRoutineName(ctx context.Context, sess *session.Session, parts []string, quoted bool) (db, schema, name string, err error) {
	db, schema = sess.Database, sess.Schema
	switch len(parts) {
	case 1:
	case 2:
		schema = parts[0]
	case 3:
		db, schema = parts[0], parts[1]
	default:
		return "", "", "", compilationError("invalid name '%s'", strings.Join(parts, "."))
	}
	name = parts[len(parts)-1]
	if !quoted {
		name = strings.ToUpper(name)
	}
	rdb, rschema, ok := o.engine.ResolveSchema(ctx, db, schema)
	if !ok && len(parts) == 1 {
		// An unqualified name in a session whose current schema is gone
		// resolves where the engine runs the session's statements: the
		// database's main schema, else miniflake.main (see engine.usePath).
		if d, found := o.engine.ResolveDatabase(ctx, db); found {
			rdb, rschema, ok = o.engine.ResolveSchema(ctx, d, engine.DefaultSchema)
		}
		if !ok {
			rdb, rschema, ok = o.engine.ResolveSchema(ctx, engine.DefaultDatabase, engine.DefaultSchema)
		}
	}
	if !ok {
		return "", "", "", &SnowflakeError{Code: "002003", SQLState: "02000",
			Message: fmt.Sprintf("SQL compilation error:\nSchema '%s.%s' does not exist or not authorized.", strings.ToUpper(db), strings.ToUpper(schema))}
	}
	return rdb, rschema, name, nil
}

func sameSignature(a, b []udf.UDFArg) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if rewriter.SnowflakeTypeName(a[i].Type) != rewriter.SnowflakeTypeName(b[i].Type) {
			return false
		}
	}
	return true
}

func kindTitle(k udf.Kind) string {
	if k == udf.KindProcedure {
		return "Procedure"
	}
	return "Function"
}

// ---------------------------------------------------------------------------
// CREATE
// ---------------------------------------------------------------------------

func (o *Orchestrator) createRoutine(ctx context.Context, sess *session.Session, stmt string) (*QueryResult, error) {
	d, err := udf.ParseCreate(stmt)
	if err != nil {
		return nil, compilationError("%v", err)
	}
	if d.Language != udf.LangSQL {
		return nil, errUnsupported(fmt.Sprintf("CREATE %s ... LANGUAGE %s", d.Kind, d.Language))
	}
	if !d.HasBody {
		return nil, compilationError("%s body is missing (expected AS $$ ... $$ or AS '...')", strings.ToLower(string(d.Kind)))
	}
	db, schema, name, err := o.resolveRoutineName(ctx, sess, d.NameParts, d.QuotedName)
	if err != nil {
		return nil, err
	}

	existing := o.udfReg.LookupRoutines(d.Kind, db, schema, name)
	for _, r := range existing {
		if !sameSignature(r.Args, d.Args) {
			continue
		}
		if d.IfNotExists {
			return statusResult(fmt.Sprintf("%s already exists, statement succeeded.", name)), nil
		}
		if !d.OrReplace {
			return nil, &SnowflakeError{Code: "002002", SQLState: "42710",
				Message: fmt.Sprintf("SQL compilation error:\nObject '%s' already exists.", name)}
		}
	}

	r := &udf.Routine{
		Kind: d.Kind, Database: db, Schema: schema, Name: name,
		Args: d.Args, ReturnType: d.ReturnType, ReturnTable: d.ReturnTable,
		Language: d.Language, Body: d.Body, Secure: d.Secure,
	}

	switch d.Kind {
	case udf.KindFunction:
		for _, e := range existing {
			if !sameSignature(e.Args, d.Args) {
				// DuckDB macros are resolved by name, not by argument types.
				return nil, errUnsupported("overloading a SQL function with different argument types")
			}
		}
		macro, err := macroDDL(r)
		if err != nil {
			return nil, err
		}
		if _, err := o.engine.ExecNoResult(ctx, macro); err != nil {
			return nil, err
		}
	case udf.KindProcedure:
		if d.ReturnTable != nil {
			return nil, errUnsupported("stored procedures that RETURNS TABLE")
		}
		if _, err := scripting.Parse(d.Body); err != nil {
			return nil, scriptingError(err)
		}
	}

	o.udfReg.PutRoutine(r, rewriter.SnowflakeTypeName)
	return statusResult(fmt.Sprintf("%s %s successfully created.", kindTitle(d.Kind), name)), nil
}

func scriptingError(err error) error {
	var u *scripting.UnsupportedError
	if errors.As(err, &u) {
		return errUnsupported(u.Feature)
	}
	return compilationError("%v", err)
}

func qualifiedRoutine(r *udf.Routine) string {
	return quoteIdent(r.Database) + "." + quoteIdent(r.Schema) + "." + quoteIdent(r.Name)
}

// macroDDL renders a SQL UDF as a DuckDB macro:
//
//	scalar: CREATE OR REPLACE MACRO db.schema.f(a, b := <default>) AS (CAST((<body>) AS <type>))
//	table:  CREATE OR REPLACE MACRO db.schema.f(a) AS TABLE SELECT * FROM (<body>) AS t(<columns>)
func macroDDL(r *udf.Routine) (string, error) {
	params := make([]string, len(r.Args))
	for i, a := range r.Args {
		params[i] = quoteIdent(a.Name)
		if a.Default != "" {
			def, err := rewriter.Rewrite(a.Default)
			if err != nil {
				return "", err
			}
			params[i] += " := " + def
		}
	}
	body, err := rewriter.Rewrite(strings.TrimRight(strings.TrimSpace(r.Body), ";"))
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(body) == "" {
		return "", compilationError("function body is empty")
	}
	head := "CREATE OR REPLACE MACRO " + qualifiedRoutine(r) + "(" + strings.Join(params, ", ") + ")"
	if r.ReturnTable != nil {
		cols := make([]string, len(r.ReturnTable))
		for i, c := range r.ReturnTable {
			cols[i] = quoteIdent(c.Name)
		}
		return head + " AS TABLE SELECT * FROM (" + body + ") AS \"_udtf\"(" + strings.Join(cols, ", ") + ")", nil
	}
	return head + " AS (CAST((" + body + ") AS " + rewriter.MapType(r.ReturnType) + "))", nil
}

// ---------------------------------------------------------------------------
// DROP
// ---------------------------------------------------------------------------

var reDropRoutine = regexp.MustCompile(`(?is)^\s*DROP\s+(FUNCTION|PROCEDURE)\s+(IF\s+EXISTS\s+)?([^(]+?)\s*\((.*)\)\s*;?\s*$`)

func (o *Orchestrator) dropRoutine(ctx context.Context, sess *session.Session, stmt string) (*QueryResult, error) {
	m := reDropRoutine.FindStringSubmatch(stmt)
	if m == nil {
		return nil, compilationError("DROP FUNCTION / DROP PROCEDURE needs the argument types: DROP %s <name>(<arg types>)", "FUNCTION|PROCEDURE")
	}
	kind := udf.Kind(strings.ToUpper(m[1]))
	ifExists := m[2] != ""
	parts, quoted := udf.SplitName(m[3])
	var types []udf.UDFArg
	for _, t := range udf.SplitTopLevel(m[4], ',') {
		if t = strings.TrimSpace(t); t != "" {
			types = append(types, udf.UDFArg{Type: t})
		}
	}
	signature := func(name string) string {
		names := make([]string, len(types))
		for i, t := range types {
			names[i] = rewriter.SnowflakeTypeName(t.Type)
		}
		return name + "(" + strings.Join(names, ", ") + ")"
	}

	db, schema, name, err := o.resolveRoutineName(ctx, sess, parts, quoted)
	var target *udf.Routine
	if err == nil {
		for _, r := range o.udfReg.LookupRoutines(kind, db, schema, name) {
			if sameSignature(r.Args, types) {
				target = r
				break
			}
		}
	}
	if target == nil {
		if ifExists {
			return statusResult("Drop statement executed successfully (" + strings.ToUpper(parts[len(parts)-1]) + " already dropped)."), nil
		}
		return nil, &SnowflakeError{Code: "002003", SQLState: "42S02",
			Message: fmt.Sprintf("SQL compilation error:\n%s '%s' does not exist or not authorized.", kindTitle(kind), signature(strings.ToUpper(parts[len(parts)-1])))}
	}
	if kind == udf.KindFunction {
		dropMacro := "DROP MACRO IF EXISTS "
		if target.ReturnTable != nil {
			dropMacro = "DROP MACRO TABLE IF EXISTS "
		}
		if _, err := o.engine.ExecNoResult(ctx, dropMacro+qualifiedRoutine(target)); err != nil {
			return nil, err
		}
	}
	o.udfReg.DeleteRoutine(target, rewriter.SnowflakeTypeName)
	return statusResult(target.Name + " successfully dropped."), nil
}

// ---------------------------------------------------------------------------
// CALL
// ---------------------------------------------------------------------------

var reCall = regexp.MustCompile(`(?is)^\s*CALL\s+([^(]+?)\s*\(`)

func (o *Orchestrator) callProcedure(ctx context.Context, sess *session.Session, stmt string) (*QueryResult, error) {
	stmt = strings.TrimRight(strings.TrimSpace(stmt), ";")
	m := reCall.FindStringSubmatchIndex(stmt)
	if m == nil {
		return nil, compilationError("invalid CALL statement")
	}
	open := m[1] - 1
	closeIdx := matchingParen(stmt, open)
	if closeIdx < 0 {
		return nil, compilationError("unbalanced argument list in CALL")
	}
	if rest := strings.TrimSpace(stmt[closeIdx+1:]); rest != "" {
		if strings.HasPrefix(strings.ToUpper(rest), "INTO") {
			return nil, errUnsupported("CALL ... INTO outside Snowflake Scripting")
		}
		return nil, compilationError("unexpected '%s' after CALL arguments", rest)
	}
	var callArgs []string
	for _, a := range udf.SplitTopLevel(stmt[open+1:closeIdx], ',') {
		if a = strings.TrimSpace(a); a != "" {
			callArgs = append(callArgs, a)
		}
	}

	parts, quoted := udf.SplitName(stmt[m[2]:m[3]])
	db, schema, name, err := o.resolveRoutineName(ctx, sess, parts, quoted)
	if err != nil {
		return nil, err
	}
	var proc *udf.Routine
	for _, r := range o.udfReg.LookupRoutines(udf.KindProcedure, db, schema, name) {
		if r.AcceptsArity(len(callArgs)) {
			proc = r
			break
		}
	}
	if proc == nil {
		return nil, &SnowflakeError{Code: "002140", SQLState: "42601",
			Message: fmt.Sprintf("SQL compilation error:\nUnknown user-defined function %s.%s.%s.", strings.ToUpper(db), strings.ToUpper(schema), name)}
	}

	depth, _ := ctx.Value(callDepthKey{}).(int)
	if depth >= maxCallDepth {
		return nil, &SnowflakeError{Code: "100320", SQLState: "P0001", Message: fmt.Sprintf("Maximum call depth of %d exceeded.", maxCallDepth)}
	}
	ctx = context.WithValue(ctx, callDepthKey{}, depth+1)

	exprs, err := bindCallArgs(proc, callArgs)
	if err != nil {
		return nil, err
	}

	exec := func(ctx context.Context, sql string) (*scripting.Result, error) {
		res, err := o.ExecuteSQL(ctx, sess, sql)
		if err != nil {
			return nil, err
		}
		return &scripting.Result{
			Columns: res.Columns, Types: res.ColumnTypes, Rows: res.Rows,
			RowsAffected: res.RowsAffected, IsDML: res.StatementType == "DML",
		}, nil
	}

	// Evaluate every argument, cast to its declared type, in one SELECT.
	var args []scripting.Arg
	if len(exprs) > 0 {
		sel := make([]string, len(exprs))
		for i, e := range exprs {
			sel[i] = "CAST((" + e + ") AS " + rewriter.MapType(proc.Args[i].Type) + ")"
		}
		res, err := exec(ctx, "SELECT "+strings.Join(sel, ", "))
		if err != nil {
			return nil, err
		}
		for i, a := range proc.Args {
			v := scripting.Value{Type: rewriter.MapType(a.Type)}
			if len(res.Rows) > 0 && i < len(res.Rows[0]) {
				v.V = res.Rows[0][i]
				if i < len(res.Types) && res.Types[i] != "" {
					v.Type = res.Types[i]
				}
			}
			args = append(args, scripting.Arg{Name: a.Name, Value: v})
		}
	}

	prog, err := scripting.Parse(proc.Body)
	if err != nil {
		return nil, scriptingError(err)
	}
	ret, err := prog.Run(ctx, exec, rewriter.MapType, args)
	if err != nil {
		var u *scripting.UnsupportedError
		if errors.As(err, &u) {
			return nil, errUnsupported(u.Feature)
		}
		return nil, err
	}

	// One row, one column named after the procedure, of its return type.
	q := "SELECT CAST(" + scripting.Literal(ret) + " AS " + rewriter.MapType(proc.ReturnType) + ") AS " + quoteIdent(proc.Name)
	cols, types, rows, err := o.engine.ExecuteTyped(ctx, q)
	if err != nil {
		return nil, err
	}
	return &QueryResult{Columns: cols, ColumnTypes: types, Rows: rows, StatementType: "CALL"}, nil
}

var reNamedArg = regexp.MustCompile(`(?s)^([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")\s*=>\s*(.+)$`)

// bindCallArgs matches the call's arguments (positional or name => value)
// to the procedure's parameters, filling in DEFAULTs.
func bindCallArgs(proc *udf.Routine, callArgs []string) ([]string, error) {
	exprs := make([]string, len(proc.Args))
	named := false
	for i, a := range callArgs {
		if m := reNamedArg.FindStringSubmatch(a); m != nil {
			named = true
			argName := strings.Trim(m[1], `"`)
			found := false
			for j, p := range proc.Args {
				if strings.EqualFold(p.Name, argName) {
					exprs[j], found = m[2], true
				}
			}
			if !found {
				return nil, compilationError("invalid argument name '%s' for procedure %s", argName, proc.Name)
			}
			continue
		}
		if named {
			return nil, compilationError("positional arguments cannot follow named arguments")
		}
		exprs[i] = a
	}
	for i, p := range proc.Args {
		if exprs[i] != "" {
			continue
		}
		if p.Default == "" {
			return nil, compilationError("missing argument '%s' for procedure %s", p.Name, proc.Name)
		}
		exprs[i] = p.Default
	}
	return exprs, nil
}

// matchingParen returns the index of the ')' closing the '(' at open,
// skipping quoted text, or -1.
func matchingParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'', '"':
			j := i + 1
			for j < len(s) {
				if s[j] == c {
					if j+1 < len(s) && s[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// SHOW USER FUNCTIONS / SHOW PROCEDURES
// ---------------------------------------------------------------------------

var reShowRoutines = regexp.MustCompile(`(?is)^\s*SHOW\s+(USER\s+FUNCTIONS|PROCEDURES)` +
	`(?:\s+LIKE\s+'((?:[^']|'')*)')?` +
	`(?:\s+IN\s+(ACCOUNT|DATABASE|SCHEMA)?\s*([^\s;]*))?\s*;?\s*$`)

var routineShowColumns = []string{
	"created_on", "name", "schema_name", "is_builtin", "is_aggregate", "is_ansi",
	"min_num_arguments", "max_num_arguments", "arguments", "description",
	"catalog_name", "is_table_function", "valid_for_clustering", "is_secure",
}

var functionShowExtraColumns = []string{
	"secrets", "external_access_integrations", "is_external_function",
	"language", "is_memoizable", "is_data_metric",
}

func (o *Orchestrator) showRoutines(ctx context.Context, sess *session.Session, stmt string) (*QueryResult, error) {
	m := reShowRoutines.FindStringSubmatch(stmt)
	if m == nil {
		return nil, compilationError("invalid SHOW statement")
	}
	kind := udf.KindProcedure
	if strings.HasPrefix(strings.ToUpper(m[1]), "USER") {
		kind = udf.KindFunction
	}
	like := strings.ReplaceAll(m[2], "''", "'")

	// Scope: IN ACCOUNT, IN DATABASE [db], IN SCHEMA [db.]schema, IN db,
	// IN db.schema; without IN, the current database.
	db, schema := sess.Database, ""
	scopeKind, scopeName := strings.ToUpper(m[3]), m[4]
	parts, _ := udf.SplitName(scopeName)
	switch {
	case scopeKind == "ACCOUNT":
		db = ""
	case scopeKind == "DATABASE":
		if scopeName != "" {
			db = parts[0]
		}
	case scopeKind == "SCHEMA":
		schema = sess.Schema
		if scopeName != "" {
			schema = parts[len(parts)-1]
			if len(parts) == 2 {
				db = parts[0]
			}
		}
	case scopeName != "":
		if len(parts) == 2 {
			db, schema = parts[0], parts[1]
		} else {
			db = parts[0]
		}
	}

	cols := append([]string{}, routineShowColumns...)
	if kind == udf.KindFunction {
		cols = append(cols, functionShowExtraColumns...)
	}
	types := make([]string, len(cols))
	types[0] = "TIMESTAMP WITH TIME ZONE"

	var rows [][]interface{}
	for _, r := range o.udfReg.ListRoutines(kind, db, schema) {
		if like != "" && !matchLike(like, r.Name) {
			continue
		}
		isTable, secure := "N", "N"
		if r.ReturnTable != nil {
			isTable = "Y"
		}
		if r.Secure {
			secure = "Y"
		}
		description := "user-defined function"
		if kind == udf.KindProcedure {
			description = "user-defined procedure"
		}
		row := []interface{}{
			r.CreatedAt, r.Name, r.Schema, "N", "N", "N",
			int64(r.MinArgs()), int64(len(r.Args)), routineSignature(r), description,
			r.Database, isTable, "N", secure,
		}
		if kind == udf.KindFunction {
			row = append(row, nil, nil, "N", string(r.Language), "N", "N")
		}
		rows = append(rows, row)
	}
	return &QueryResult{Columns: cols, ColumnTypes: types, Rows: rows, StatementType: "SHOW"}, nil
}

// routineSignature is the `arguments` column of SHOW: NAME(NUMBER, DEFAULT
// VARCHAR) RETURN VARCHAR.
func routineSignature(r *udf.Routine) string {
	args := make([]string, len(r.Args))
	for i, a := range r.Args {
		args[i] = rewriter.SnowflakeTypeName(a.Type)
		if a.Default != "" {
			args[i] = "DEFAULT " + args[i]
		}
	}
	ret := rewriter.SnowflakeTypeName(r.ReturnType)
	if r.ReturnTable != nil {
		cols := make([]string, len(r.ReturnTable))
		for i, c := range r.ReturnTable {
			cols[i] = c.Name + " " + rewriter.SnowflakeTypeName(c.Type)
		}
		ret = "TABLE (" + strings.Join(cols, ", ") + ")"
	}
	return r.Name + "(" + strings.Join(args, ", ") + ") RETURN " + ret
}
