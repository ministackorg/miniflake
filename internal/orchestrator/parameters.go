package orchestrator

import (
	"fmt"
	"regexp"
	"strings"
)

// snowflakeParameter is one row of SHOW PARAMETERS output.
// Columns match Snowflake: key, value, default, level, description.
type snowflakeParameter struct {
	Key         string
	Value       string
	Default     string
	Level       string
	Description string
}

// defaultSessionParameters are the session-level defaults real Snowflake
// exposes. Drivers (JDBC, .NET, Python, gosnowflake) probe these at connect
// time; an empty result set makes some of them fail.
//
// Values follow Snowflake's documented defaults. MiniFlake does not honour
// ALTER SESSION yet, so value always equals default and level is empty.
var defaultSessionParameters = []snowflakeParameter{
	{"ABORT_DETACHED_QUERY", "false", "false", "", "abort queries when the client disconnects"},
	{"AUTOCOMMIT", "true", "true", "", "autocommit mode"},
	{"BINARY_OUTPUT_FORMAT", "HEX", "HEX", "", "display format for binary"},
	{"CLIENT_SESSION_KEEP_ALIVE", "false", "false", "", "keep session alive between requests"},
	{"CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "3600", "3600", "", "heartbeat frequency in seconds"},
	{"DATE_OUTPUT_FORMAT", "YYYY-MM-DD", "YYYY-MM-DD", "", "display format for date"},
	{"ERROR_ON_NONDETERMINISTIC_MERGE", "true", "true", "", "error on nondeterministic MERGE"},
	{"ERROR_ON_NONDETERMINISTIC_UPDATE", "false", "false", "", "error on nondeterministic UPDATE"},
	{"GEOGRAPHY_OUTPUT_FORMAT", "GeoJSON", "GeoJSON", "", "display format for GEOGRAPHY"},
	{"JSON_INDENT", "2", "2", "", "indent for JSON output"},
	{"LOCK_TIMEOUT", "43200", "43200", "", "lock timeout in seconds"},
	{"QUERY_TAG", "", "", "", "query tag"},
	{"QUOTED_IDENTIFIERS_IGNORE_CASE", "false", "false", "", "case-insensitive quoted identifiers"},
	{"ROWS_PER_RESULTSET", "0", "0", "", "max rows per result set (0 = unlimited)"},
	{"SIMULATED_DATA_SHARING_CONSUMER", "", "", "", "simulated data sharing consumer"},
	{"STATEMENT_TIMEOUT_IN_SECONDS", "0", "0", "", "statement timeout in seconds (0 = none)"},
	{"TIMESTAMP_LTZ_OUTPUT_FORMAT", "", "", "", "display format for TIMESTAMP_LTZ"},
	{"TIMESTAMP_NTZ_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF3", "YYYY-MM-DD HH24:MI:SS.FF3", "", "display format for TIMESTAMP_NTZ"},
	{"TIMESTAMP_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM", "", "display format for timestamps"},
	{"TIMESTAMP_TZ_OUTPUT_FORMAT", "", "", "", "display format for TIMESTAMP_TZ"},
	{"TIMEZONE", "America/Los_Angeles", "America/Los_Angeles", "", "time zone"},
	{"TIME_INPUT_FORMAT", "AUTO", "AUTO", "", "input format for time"},
	{"TIME_OUTPUT_FORMAT", "HH24:MI:SS", "HH24:MI:SS", "", "display format for time"},
	{"TRANSACTION_ABORT_ON_ERROR", "false", "false", "", "abort transaction on statement error"},
	{"TWO_DIGIT_CENTURY_START", "1970", "1970", "", "century start for two-digit years"},
	{"UNSUPPORTED_DDL_ACTION", "ignore", "ignore", "", "action on unsupported DDL"},
	{"USE_CACHED_RESULT", "true", "true", "", "reuse cached query results"},
}

// defaultAccountParameters are account-level extras returned by
// SHOW PARAMETERS IN ACCOUNT. Session parameters are included too, matching
// Snowflake's IN ACCOUNT behaviour.
var defaultAccountParameters = []snowflakeParameter{
	{"MAX_CONCURRENCY_LEVEL", "8", "8", "", "max concurrency level"},
	{"PERIODIC_DATA_REKEYING", "true", "true", "", "periodic data rekeying"},
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
		rows = append(rows, []interface{}{p.Key, p.Value, p.Default, p.Level, p.Description})
	}
	return &QueryResult{
		Columns:       []string{"key", "value", "default", "level", "description"},
		Rows:          rows,
		StatementType: "SHOW",
	}, true, nil
}
