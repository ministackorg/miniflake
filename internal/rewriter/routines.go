package rewriter

import (
	"encoding/hex"
	"regexp"
	"strings"
)

// ---------------------------------------------------------------------------
// User-defined functions and stored procedures (#36, #37)
// ---------------------------------------------------------------------------

// Routine statements are handed to the orchestrator whole. Their bodies are
// SQL or Snowflake Scripting that the other rules must not touch (a
// procedure body's `:var` binds look like VARIANT paths, its `;` and `*/`
// would break a plain marker comment), so the original statement travels
// hex-encoded: /* MINIFLAKE_ROUTINE <hex> */.
var (
	reRoutineCreate = regexp.MustCompile(`(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?(SECURE\s+)?((TEMP|TEMPORARY|VOLATILE)\s+)?(SECURE\s+)?(FUNCTION|PROCEDURE)\b`)
	reRoutineDrop   = regexp.MustCompile(`(?is)^\s*DROP\s+(FUNCTION|PROCEDURE)\b`)
	reRoutineCall   = regexp.MustCompile(`(?is)^\s*CALL\b`)
	reRoutineShow   = regexp.MustCompile(`(?is)^\s*SHOW\s+(USER\s+FUNCTIONS|PROCEDURES)\b`)
)

// RoutineMarker is the marker prefix the orchestrator matches.
const RoutineMarker = "MINIFLAKE_ROUTINE"

func rewriteRoutines(sql string) string {
	if reRoutineCreate.MatchString(sql) || reRoutineDrop.MatchString(sql) ||
		reRoutineCall.MatchString(sql) || reRoutineShow.MatchString(sql) {
		return "/* " + RoutineMarker + " " + hex.EncodeToString([]byte(strings.TrimSpace(sql))) + " */"
	}
	return sql
}

// DecodeRoutineMarker returns the original statement of a routine marker.
func DecodeRoutineMarker(sql string) (string, bool) {
	s := strings.TrimSpace(sql)
	prefix := "/* " + RoutineMarker + " "
	if !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, " */") {
		return "", false
	}
	b, err := hex.DecodeString(s[len(prefix) : len(s)-3])
	if err != nil {
		return "", false
	}
	return string(b), true
}

// TABLE(f(args)) calls a table function in Snowflake. DuckDB calls table
// macros directly in FROM: f(args). FLATTEN has its own rule, which runs
// first.
var reTableCall = regexp.MustCompile(`(?i)\bTABLE\s*\(\s*([A-Za-z_"][\w$."]*)\s*\(`)

func rewriteTableFunctionCall(sql string) string {
	for {
		loc := reTableCall.FindStringSubmatchIndex(sql)
		if loc == nil {
			return sql
		}
		outerOpen := strings.Index(sql[loc[0]:], "(") + loc[0]
		args, outerClose := extractFuncArgs(sql, outerOpen)
		if outerClose == -1 || len(args) != 1 {
			return sql
		}
		inner := strings.TrimSpace(sql[outerOpen+1 : outerClose])
		sql = sql[:loc[0]] + inner + sql[outerClose+1:]
	}
}

// ---------------------------------------------------------------------------
// Type names
// ---------------------------------------------------------------------------

var reTypeParams = regexp.MustCompile(`^([A-Z_ ]+?)\s*(\(.*\))?$`)

// MapType translates a Snowflake data type name to the DuckDB type used for
// it (casts of routine arguments and return values). Unknown names are
// returned unchanged, so DuckDB reports them.
func MapType(t string) string {
	u := strings.ToUpper(strings.Join(strings.Fields(t), " "))
	m := reTypeParams.FindStringSubmatch(u)
	if m == nil {
		return t
	}
	base, params := strings.TrimSpace(m[1]), m[2]
	switch base {
	case "NUMBER", "NUMERIC", "DECIMAL", "DEC":
		if params == "" {
			return "BIGINT" // NUMBER is NUMBER(38,0)
		}
		inner := strings.Trim(params, "()")
		parts := strings.Split(inner, ",")
		if len(parts) == 2 && strings.TrimSpace(parts[1]) == "0" {
			return "BIGINT"
		}
		if len(parts) == 1 {
			return "BIGINT"
		}
		return "DECIMAL" + params
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT":
		return "BIGINT"
	case "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL":
		return "DOUBLE"
	case "VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER", "NCHAR", "NVARCHAR", "NVARCHAR2", "CHAR VARYING", "NCHAR VARYING":
		return "VARCHAR"
	case "BOOLEAN":
		return "BOOLEAN"
	case "DATE":
		return "DATE"
	case "TIME":
		return "TIME"
	case "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ":
		return "TIMESTAMP"
	case "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		return "TIMESTAMPTZ"
	case "VARIANT", "OBJECT", "ARRAY":
		return "JSON"
	case "BINARY", "VARBINARY":
		return "BLOB"
	}
	return t
}

// SnowflakeTypeName is the name Snowflake shows for a type in SHOW output
// and signatures: INT is NUMBER, STRING is VARCHAR, DOUBLE is FLOAT, and
// so on. Parameters are dropped.
func SnowflakeTypeName(t string) string {
	u := strings.ToUpper(strings.Join(strings.Fields(t), " "))
	if i := strings.Index(u, "("); i >= 0 {
		u = strings.TrimSpace(u[:i])
	}
	switch u {
	case "NUMBER", "NUMERIC", "DECIMAL", "DEC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT":
		return "NUMBER"
	case "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL":
		return "FLOAT"
	case "VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER", "NCHAR", "NVARCHAR", "NVARCHAR2", "CHAR VARYING", "NCHAR VARYING":
		return "VARCHAR"
	case "DATETIME", "TIMESTAMP":
		return "TIMESTAMP_NTZ"
	case "VARBINARY":
		return "BINARY"
	}
	return u
}
