// Package rewriter transforms Snowflake SQL into DuckDB-compatible SQL.
// It applies a chain of regex-based rewrite rules in order.
package rewriter

import (
	"fmt"
	"regexp"
	"strings"
)

// RewriteRule is a single transformation applied to a SQL string.
type RewriteRule func(sql string) string

// rules is the ordered list of rewrite rules. Order matters —
// some rules must run before others (e.g., VARIANT access before type casting).
var rules []RewriteRule

func init() {
	rules = []RewriteRule{
		rewriteUseStatements,
		rewritePutGet,
		rewriteCopyInto,
		rewriteStageCommands,
		rewriteStreams,
		rewriteTasks,
		rewritePipes,
		rewriteShowParameters,
		rewriteUndrop,
		rewriteTimeTravel,
		rewriteMerge,
		rewriteShowCommands,
		rewriteDescribe,
		rewriteIdentifierFunc,
		rewriteVariantAccess,
		rewriteFlatten,
		rewriteIFF,
		rewriteNVL2,
		rewriteNVL,
		rewriteDIV0NULL,
		rewriteDIV0,
		rewriteSquare,
		rewriteArrayConstruct,
		rewriteArraySize,
		rewriteObjectConstruct,
		rewriteParseJSON,
		rewriteRegexpLike,
		rewriteRLike,
		rewriteListAgg,
		rewriteStrtokToArray,
		rewriteDateAdd,
		rewriteDateDiff,
		rewriteToVarchar,
		rewriteToNumber,
		rewriteToDate,
		rewriteToTimestamp,
		rewriteDataTypes,
	}
}

// Rewrite transforms a single Snowflake SQL statement to DuckDB-compatible SQL.
func Rewrite(sql string) (string, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return "", nil
	}
	for _, rule := range rules {
		sql = rule(sql)
	}
	return strings.TrimSpace(sql), nil
}

// RewriteMulti splits a multi-statement string on semicolons and rewrites each.
func RewriteMulti(sql string) ([]string, error) {
	stmts := splitStatements(sql)
	var results []string
	for _, s := range stmts {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		r, err := Rewrite(s)
		if err != nil {
			return nil, err
		}
		if r != "" {
			results = append(results, r)
		}
	}
	return results, nil
}

// splitStatements splits on semicolons that are not inside single-quoted strings.
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' && !inQuote {
			inQuote = true
			current.WriteByte(ch)
		} else if ch == '\'' && inQuote {
			// handle escaped quote ''
			if i+1 < len(sql) && sql[i+1] == '\'' {
				current.WriteByte(ch)
				current.WriteByte(sql[i+1])
				i++
			} else {
				inQuote = false
				current.WriteByte(ch)
			}
		} else if ch == ';' && !inQuote {
			stmts = append(stmts, current.String())
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		stmts = append(stmts, current.String())
	}
	return stmts
}

// ---------------------------------------------------------------------------
// Helper: balanced parenthesis argument extraction
// ---------------------------------------------------------------------------

// extractFuncArgs extracts the arguments of a function call starting at the
// opening paren position. Returns the list of top-level comma-separated args
// and the index of the closing paren. Returns nil, -1 on failure.
func extractFuncArgs(sql string, openParen int) ([]string, int) {
	if openParen >= len(sql) || sql[openParen] != '(' {
		return nil, -1
	}
	depth := 0
	start := openParen + 1
	var args []string
	inQuote := false
	for i := openParen; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' && !inQuote {
			inQuote = true
			continue
		}
		if ch == '\'' && inQuote {
			if i+1 < len(sql) && sql[i+1] == '\'' {
				i++
				continue
			}
			inQuote = false
			continue
		}
		if inQuote {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				arg := strings.TrimSpace(sql[start:i])
				if arg != "" {
					args = append(args, arg)
				}
				return args, i
			}
		case ',':
			if depth == 1 {
				args = append(args, strings.TrimSpace(sql[start:i]))
				start = i + 1
			}
		}
	}
	return nil, -1
}

// findFuncCall finds a case-insensitive function call by name and returns
// the start index, open-paren index, args, and close-paren index.
// Returns -1 for start if not found.
func findFuncCall(sql, funcName string) (startIdx, openParen int, args []string, closeParen int) {
	pat := regexp.MustCompile(`(?i)\b` + funcName + `\s*\(`)
	loc := pat.FindStringIndex(sql)
	if loc == nil {
		return -1, -1, nil, -1
	}
	// find the actual '(' position
	op := strings.Index(sql[loc[0]:], "(")
	if op == -1 {
		return -1, -1, nil, -1
	}
	op += loc[0]
	args, cp := extractFuncArgs(sql, op)
	if cp == -1 {
		return -1, -1, nil, -1
	}
	return loc[0], op, args, cp
}

// replaceFuncCall replaces the first occurrence of funcName(...) with replacement text.
// The replacer receives the extracted args and returns the replacement string.
func replaceFuncCall(sql, funcName string, replacer func(args []string) string) string {
	for {
		start, _, args, cp := findFuncCall(sql, funcName)
		if start == -1 {
			break
		}
		replacement := replacer(args)
		sql = sql[:start] + replacement + sql[cp+1:]
	}
	return sql
}

// ---------------------------------------------------------------------------
// Rewrite rules
// ---------------------------------------------------------------------------

// 18. USE DATABASE/SCHEMA/WAREHOUSE/ROLE → special markers
var reUse = regexp.MustCompile(`(?i)^\s*USE\s+(DATABASE|SCHEMA|WAREHOUSE|ROLE)\s+(.+)$`)

func rewriteUseStatements(sql string) string {
	m := reUse.FindStringSubmatch(sql)
	if m != nil {
		kind := strings.ToUpper(m[1])
		name := strings.TrimSpace(strings.TrimRight(m[2], ";"))
		return fmt.Sprintf("/* MINIFLAKE_USE_%s %s */", kind, name)
	}
	return sql
}

// 17. PUT/GET → special markers
var rePut = regexp.MustCompile(`(?i)^\s*PUT\b`)
var reGet = regexp.MustCompile(`(?i)^\s*GET\b`)

func rewritePutGet(sql string) string {
	if rePut.MatchString(sql) {
		return "/* MINIFLAKE_PUT " + sql + " */"
	}
	if reGet.MatchString(sql) {
		return "/* MINIFLAKE_GET " + sql + " */"
	}
	return sql
}

// 16. COPY INTO → special markers
var reCopyInto = regexp.MustCompile(`(?i)^\s*COPY\s+INTO\b`)

func rewriteCopyInto(sql string) string {
	if reCopyInto.MatchString(sql) {
		return "/* MINIFLAKE_COPY_INTO " + sql + " */"
	}
	return sql
}

// 13. SHOW commands
var reShowDatabases = regexp.MustCompile(`(?i)^\s*SHOW\s+DATABASES\s*$`)
var reShowSchemas = regexp.MustCompile(`(?i)^\s*SHOW\s+SCHEMAS(\s+IN\s+(DATABASE\s+)?(\S+))?\s*$`)
var reShowTables = regexp.MustCompile(`(?i)^\s*SHOW\s+TABLES(\s+IN\s+(SCHEMA\s+|DATABASE\s+)?(\S+))?\s*$`)
var reShowColumns = regexp.MustCompile(`(?i)^\s*SHOW\s+COLUMNS\s+IN\s+(\S+)\s*$`)
var reShowWarehouses = regexp.MustCompile(`(?i)^\s*SHOW\s+WAREHOUSES\s*$`)

func rewriteShowCommands(sql string) string {
	if reShowDatabases.MatchString(sql) {
		return "SELECT DISTINCT catalog_name AS \"name\" FROM information_schema.schemata"
	}
	if m := reShowSchemas.FindStringSubmatch(sql); m != nil {
		if m[3] != "" {
			return fmt.Sprintf("SELECT schema_name AS \"name\" FROM information_schema.schemata WHERE catalog_name = '%s'", m[3])
		}
		return "SELECT schema_name AS \"name\" FROM information_schema.schemata"
	}
	if m := reShowTables.FindStringSubmatch(sql); m != nil {
		if m[3] != "" {
			return fmt.Sprintf("SELECT table_name AS \"name\" FROM information_schema.tables WHERE table_schema = '%s'", m[3])
		}
		return "SELECT table_name AS \"name\" FROM information_schema.tables"
	}
	if m := reShowColumns.FindStringSubmatch(sql); m != nil {
		return fmt.Sprintf("SELECT column_name AS \"name\", data_type AS \"type\" FROM information_schema.columns WHERE table_name = '%s'", m[1])
	}
	if reShowWarehouses.MatchString(sql) {
		return "SELECT 'MINIFLAKE_WH' AS \"name\", 'STARTED' AS \"state\", 'X-SMALL' AS \"size\""
	}
	return sql
}

// 14. DESCRIBE/DESC TABLE
var reDescribe = regexp.MustCompile(`(?i)^\s*(DESCRIBE|DESC)\s+TABLE\s+(\S+)\s*$`)

func rewriteDescribe(sql string) string {
	m := reDescribe.FindStringSubmatch(sql)
	if m != nil {
		return "DESCRIBE " + m[2]
	}
	return sql
}

// 19. IDENTIFIER('name') → name
var reIdentifier = regexp.MustCompile(`(?i)IDENTIFIER\s*\(\s*'([^']+)'\s*\)`)

func rewriteIdentifierFunc(sql string) string {
	return reIdentifier.ReplaceAllString(sql, "$1")
}

// 1. VARIANT / semi-structured access
// col:key::type → CAST(col->>'key' AS type)
// col:key       → col->>'key'
// col[0]        → col->>0
// Nested: col:key1.key2 → col->>'key1'->>'key2'
var reVariantCastNested = regexp.MustCompile(`(?i)\b([a-zA-Z_][a-zA-Z0-9_]*):([a-zA-Z_][a-zA-Z0-9_.]*)::([\w]+(?:\([^)]*\))?)`)
var reVariantAccessNested = regexp.MustCompile(`\b([a-zA-Z_][a-zA-Z0-9_]*):([a-zA-Z_][a-zA-Z0-9_.]*)`)
var reVariantArrayAccess = regexp.MustCompile(`\b([a-zA-Z_][a-zA-Z0-9_]*)\[(\d+)\]`)

func expandNestedKeys(col, keys string) string {
	parts := strings.Split(keys, ".")
	result := col + "->>" + "'" + parts[0] + "'"
	for _, p := range parts[1:] {
		result += "->>" + "'" + p + "'"
	}
	return result
}

func rewriteVariantAccess(sql string) string {
	// col:key::type → CAST(col->>'key' AS type) (must run before plain access)
	sql = reVariantCastNested.ReplaceAllStringFunc(sql, func(match string) string {
		m := reVariantCastNested.FindStringSubmatch(match)
		expanded := expandNestedKeys(m[1], m[2])
		return "CAST(" + expanded + " AS " + m[3] + ")"
	})
	// col:key → col->>'key'
	sql = reVariantAccessNested.ReplaceAllStringFunc(sql, func(match string) string {
		m := reVariantAccessNested.FindStringSubmatch(match)
		return expandNestedKeys(m[1], m[2])
	})
	// col[0] → col->>0
	sql = reVariantArrayAccess.ReplaceAllString(sql, "${1}->>${2}")
	return sql
}

// 2. FLATTEN
var reFlattenTable = regexp.MustCompile(`(?i)TABLE\s*\(\s*FLATTEN\s*\(\s*(?:input\s*=>\s*)?([^)]+)\)\s*\)`)
var reLateralFlatten = regexp.MustCompile(`(?i),?\s*LATERAL\s+FLATTEN\s*\(\s*(?:input\s*=>\s*)?([^)]+)\)\s+(?:AS\s+)?(\w+)`)

func rewriteFlatten(sql string) string {
	// Snowflake's FLATTEN returns the columns (seq, key, path, index, value, this).
	// DuckDB's unnest returns a single column named "unnest". Alias the unnest
	// result as <alias>(value) so the common `<alias>.value` projection works.
	sql = reFlattenTable.ReplaceAllString(sql, "unnest($1) AS flat(value)")
	sql = reLateralFlatten.ReplaceAllString(sql, ", LATERAL (SELECT unnest AS value FROM unnest($1)) AS $2")
	return sql
}

// 5. IFF(cond, true_val, false_val) → CASE WHEN cond THEN true_val ELSE false_val END
func rewriteIFF(sql string) string {
	return replaceFuncCall(sql, "IFF", func(args []string) string {
		if len(args) < 3 {
			return "IFF(" + strings.Join(args, ", ") + ")"
		}
		return "CASE WHEN " + args[0] + " THEN " + args[1] + " ELSE " + args[2] + " END"
	})
}

// 6. NVL2(a, b, c) → CASE WHEN a IS NOT NULL THEN b ELSE c END
func rewriteNVL2(sql string) string {
	return replaceFuncCall(sql, "NVL2", func(args []string) string {
		if len(args) < 3 {
			return "NVL2(" + strings.Join(args, ", ") + ")"
		}
		return "CASE WHEN " + args[0] + " IS NOT NULL THEN " + args[1] + " ELSE " + args[2] + " END"
	})
}

// 6. NVL(a, b) → COALESCE(a, b)
func rewriteNVL(sql string) string {
	return replaceFuncCall(sql, "NVL", func(args []string) string {
		return "COALESCE(" + strings.Join(args, ", ") + ")"
	})
}

// 7. DIV0NULL(a, b) → CASE WHEN b = 0 THEN NULL ELSE a / b END
func rewriteDIV0NULL(sql string) string {
	return replaceFuncCall(sql, "DIV0NULL", func(args []string) string {
		if len(args) < 2 {
			return "DIV0NULL(" + strings.Join(args, ", ") + ")"
		}
		return "CASE WHEN " + args[1] + " = 0 THEN NULL ELSE " + args[0] + " / " + args[1] + " END"
	})
}

// 7. DIV0(a, b) → CASE WHEN b = 0 THEN 0 ELSE a / b END
func rewriteDIV0(sql string) string {
	return replaceFuncCall(sql, "DIV0", func(args []string) string {
		if len(args) < 2 {
			return "DIV0(" + strings.Join(args, ", ") + ")"
		}
		return "CASE WHEN " + args[1] + " = 0 THEN 0 ELSE " + args[0] + " / " + args[1] + " END"
	})
}

// 7. SQUARE(x) → POWER(x, 2)
func rewriteSquare(sql string) string {
	return replaceFuncCall(sql, "SQUARE", func(args []string) string {
		if len(args) < 1 {
			return "SQUARE()"
		}
		return "POWER(" + args[0] + ", 2)"
	})
}

// 8. ARRAY_CONSTRUCT(a, b, c) → [a, b, c]
func rewriteArrayConstruct(sql string) string {
	return replaceFuncCall(sql, "ARRAY_CONSTRUCT", func(args []string) string {
		return "[" + strings.Join(args, ", ") + "]"
	})
}

// 8. ARRAY_SIZE(arr) → len(arr)
func rewriteArraySize(sql string) string {
	return replaceFuncCall(sql, "ARRAY_SIZE", func(args []string) string {
		if len(args) < 1 {
			return "len()"
		}
		return "len(" + args[0] + ")"
	})
}

// 9. OBJECT_CONSTRUCT('k1', v1, 'k2', v2) → {'k1': v1, 'k2': v2}
func rewriteObjectConstruct(sql string) string {
	return replaceFuncCall(sql, "OBJECT_CONSTRUCT", func(args []string) string {
		if len(args)%2 != 0 {
			// odd number of args — can't form pairs, pass through
			return "OBJECT_CONSTRUCT(" + strings.Join(args, ", ") + ")"
		}
		var pairs []string
		for i := 0; i < len(args); i += 2 {
			pairs = append(pairs, args[i]+": "+args[i+1])
		}
		return "{" + strings.Join(pairs, ", ") + "}"
	})
}

// 10. PARSE_JSON(str) → str::JSON
func rewriteParseJSON(sql string) string {
	return replaceFuncCall(sql, "PARSE_JSON", func(args []string) string {
		if len(args) < 1 {
			return "PARSE_JSON()"
		}
		return args[0] + "::JSON"
	})
}

// 20. REGEXP_LIKE(col, pattern) → regexp_matches(col, pattern)
func rewriteRegexpLike(sql string) string {
	return replaceFuncCall(sql, "REGEXP_LIKE", func(args []string) string {
		return "regexp_matches(" + strings.Join(args, ", ") + ")"
	})
}

// 20. col RLIKE pattern → regexp_matches(col, pattern)
var reRlike = regexp.MustCompile(`(?i)\b(\S+)\s+RLIKE\s+(\S+)`)

func rewriteRLike(sql string) string {
	return reRlike.ReplaceAllString(sql, "regexp_matches($1, $2)")
}

// 21. LISTAGG(col, ',') → STRING_AGG(col, ',')
func rewriteListAgg(sql string) string {
	return replaceFuncCall(sql, "LISTAGG", func(args []string) string {
		return "STRING_AGG(" + strings.Join(args, ", ") + ")"
	})
}

// 22. STRTOK_TO_ARRAY(str, delim) → STRING_SPLIT(str, delim)
func rewriteStrtokToArray(sql string) string {
	return replaceFuncCall(sql, "STRTOK_TO_ARRAY", func(args []string) string {
		return "STRING_SPLIT(" + strings.Join(args, ", ") + ")"
	})
}

// 12. DATEADD(part, n, col) → col + INTERVAL 'n part'
func rewriteDateAdd(sql string) string {
	return replaceFuncCall(sql, "DATEADD", func(args []string) string {
		if len(args) < 3 {
			return "DATEADD(" + strings.Join(args, ", ") + ")"
		}
		part := strings.Trim(strings.TrimSpace(args[0]), "'\"")
		n := strings.TrimSpace(args[1])
		col := strings.TrimSpace(args[2])
		return col + " + INTERVAL '" + n + " " + part + "'"
	})
}

// 12. DATEDIFF(part, a, b) → DATE_DIFF('part', a, b)
func rewriteDateDiff(sql string) string {
	return replaceFuncCall(sql, "DATEDIFF", func(args []string) string {
		if len(args) < 3 {
			return "DATEDIFF(" + strings.Join(args, ", ") + ")"
		}
		part := strings.Trim(strings.TrimSpace(args[0]), "'\"")
		return "DATE_DIFF('" + part + "', " + args[1] + ", " + args[2] + ")"
	})
}

// 11. TO_VARCHAR / TO_CHAR → CAST(x AS VARCHAR)
func rewriteToVarchar(sql string) string {
	sql = replaceFuncCall(sql, "TO_VARCHAR", func(args []string) string {
		if len(args) < 1 {
			return "TO_VARCHAR()"
		}
		if len(args) == 2 {
			// TO_VARCHAR(x, fmt) — format is Snowflake-specific, best effort
			return "CAST(" + args[0] + " AS VARCHAR)"
		}
		return "CAST(" + args[0] + " AS VARCHAR)"
	})
	sql = replaceFuncCall(sql, "TO_CHAR", func(args []string) string {
		if len(args) < 1 {
			return "TO_CHAR()"
		}
		return "CAST(" + args[0] + " AS VARCHAR)"
	})
	return sql
}

// 11. TO_NUMBER → CAST(x AS NUMERIC) or CAST(x AS DECIMAL(p,s))
func rewriteToNumber(sql string) string {
	return replaceFuncCall(sql, "TO_NUMBER", func(args []string) string {
		if len(args) < 1 {
			return "TO_NUMBER()"
		}
		if len(args) == 3 {
			return "CAST(" + args[0] + " AS DECIMAL(" + strings.TrimSpace(args[1]) + ", " + strings.TrimSpace(args[2]) + "))"
		}
		return "CAST(" + args[0] + " AS NUMERIC)"
	})
}

// 11. TO_DATE → CAST(x AS DATE)
func rewriteToDate(sql string) string {
	return replaceFuncCall(sql, "TO_DATE", func(args []string) string {
		if len(args) < 1 {
			return "TO_DATE()"
		}
		return "CAST(" + args[0] + " AS DATE)"
	})
}

// 11. TO_TIMESTAMP → CAST(x AS TIMESTAMP)
func rewriteToTimestamp(sql string) string {
	return replaceFuncCall(sql, "TO_TIMESTAMP", func(args []string) string {
		if len(args) < 1 {
			return "TO_TIMESTAMP()"
		}
		return "CAST(" + args[0] + " AS TIMESTAMP)"
	})
}

// 23. Data type mapping in CREATE TABLE / column definitions
var dataTypeReplacements = []struct {
	pat  *regexp.Regexp
	repl string
}{
	// NUMBER(38,0) → BIGINT (exact match for the common Snowflake integer pattern)
	{regexp.MustCompile(`(?i)\bNUMBER\s*\(\s*38\s*,\s*0\s*\)`), "BIGINT"},
	// NUMBER(p,s) → DECIMAL(p,s)
	{regexp.MustCompile(`(?i)\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)`), "DECIMAL($1, $2)"},
	// VARIANT → JSON
	{regexp.MustCompile(`(?i)\bVARIANT\b`), "JSON"},
	// TIMESTAMP_NTZ → TIMESTAMP
	{regexp.MustCompile(`(?i)\bTIMESTAMP_NTZ\b`), "TIMESTAMP"},
	// TIMESTAMP_LTZ → TIMESTAMPTZ
	{regexp.MustCompile(`(?i)\bTIMESTAMP_LTZ\b`), "TIMESTAMPTZ"},
	// TIMESTAMP_TZ → TIMESTAMPTZ
	{regexp.MustCompile(`(?i)\bTIMESTAMP_TZ\b`), "TIMESTAMPTZ"},
	// BINARY → BLOB
	{regexp.MustCompile(`(?i)\bBINARY\b`), "BLOB"},
}

// reNumberBare matches bare NUMBER (not followed by parens). Since Go regexp
// lacks negative lookahead, we match NUMBER with optional trailing parens and
// only replace the bare ones.
var reNumberWithContext = regexp.MustCompile(`(?i)\bNUMBER\s*(\()?`)

func rewriteDataTypes(sql string) string {
	// Only apply data type rewrites in DDL-like statements
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upper, "CREATE") && !strings.HasPrefix(upper, "ALTER") {
		return sql
	}
	for _, r := range dataTypeReplacements {
		sql = r.pat.ReplaceAllString(sql, r.repl)
	}
	// Handle bare NUMBER (not followed by '(') — must run after NUMBER(p,s) rules
	sql = reNumberWithContext.ReplaceAllStringFunc(sql, func(match string) string {
		if strings.Contains(match, "(") {
			return match // NUMBER( — already handled by earlier rules or pass through
		}
		return "BIGINT"
	})
	return sql
}
