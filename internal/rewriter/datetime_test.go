package rewriter

import "testing"

func TestRewriteDateTime(t *testing.T) {
	tests := []struct {
		name, input, expect string
	}{
		// Niladic functions with parentheses (#34).
		{"current_timestamp()", "SELECT CURRENT_TIMESTAMP()", "SELECT CURRENT_TIMESTAMP"},
		{"current_timestamp(precision)", "SELECT CURRENT_TIMESTAMP(3)", "SELECT CURRENT_TIMESTAMP"},
		{"bare current_timestamp untouched", "SELECT CURRENT_TIMESTAMP", "SELECT CURRENT_TIMESTAMP"},
		{"localtimestamp()", "SELECT LOCALTIMESTAMP()", "SELECT CURRENT_TIMESTAMP"},
		{"getdate()", "SELECT GETDATE()", "SELECT CURRENT_TIMESTAMP"},
		{"sysdate() is UTC NTZ", "SELECT SYSDATE()", "SELECT timezone('UTC', CURRENT_TIMESTAMP)"},
		{"current_time()", "SELECT CURRENT_TIME()", "SELECT " + localTime},
		{"bare current_time", "SELECT CURRENT_TIME", "SELECT " + localTime},
		{"localtime", "SELECT LOCALTIME()", "SELECT " + localTime},
		{"current_time in string untouched", "SELECT 'CURRENT_TIME'", "SELECT 'CURRENT_TIME'"},
		{"dateadd over current_timestamp()",
			"SELECT DATEADD(day, -1, CURRENT_TIMESTAMP())",
			"SELECT CURRENT_TIMESTAMP + INTERVAL '-1 day'"},

		// CONVERT_TIMEZONE.
		{"convert_timezone 3-arg",
			"SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', ts) FROM t",
			"SELECT timezone('America/New_York', timezone('UTC', CAST(ts AS TIMESTAMP))) FROM t"},
		{"convert_timezone 2-arg",
			"SELECT CONVERT_TIMEZONE('America/New_York', ts) FROM t",
			"SELECT timezone('America/New_York', CAST(ts AS TIMESTAMPTZ)) FROM t"},

		// Timestamp types outside DDL.
		{"cast ::timestamp_ntz", "SELECT '2026-01-02'::TIMESTAMP_NTZ", "SELECT '2026-01-02'::TIMESTAMP"},
		{"cast AS timestamp_ltz", "SELECT CAST(x AS TIMESTAMP_LTZ)", "SELECT CAST(x AS TIMESTAMPTZ)"},
		{"cast timestamp_tz with precision", "SELECT x::timestamp_tz(9)", "SELECT x::TIMESTAMPTZ"},
		{"offset literal space removed",
			"SELECT '2026-01-02 00:00:00 +00:00'::TIMESTAMP_TZ",
			"SELECT '2026-01-02 00:00:00+00:00'::TIMESTAMPTZ"},
		{"type name in string untouched", "SELECT 'TIMESTAMP_NTZ'", "SELECT 'TIMESTAMP_NTZ'"},
		{"ddl still mapped", "CREATE TABLE t (c TIMESTAMP_NTZ, d TIMESTAMP_LTZ)", "CREATE TABLE t (c TIMESTAMP, d TIMESTAMPTZ)"},
		{"cast ::variant", "SELECT x::VARIANT", "SELECT x::JSON"},
		{"to_timestamp_ntz", "SELECT TO_TIMESTAMP_NTZ('2026-01-02')", "SELECT CAST('2026-01-02' AS TIMESTAMP)"},
		{"to_timestamp_tz", "SELECT TO_TIMESTAMP_TZ(s)", "SELECT CAST(s AS TIMESTAMPTZ)"},

		// Format arguments.
		{"to_varchar date format",
			"SELECT TO_VARCHAR(d, 'DD/MM/YYYY')",
			"SELECT strftime(d, '%d/%m/%Y')"},
		{"to_char lowercase elements and quoted literal",
			`SELECT TO_CHAR(ts, 'yyyy-mm-dd"T"hh24:mi:ss')`,
			"SELECT strftime(ts, '%Y-%m-%dT%H:%M:%S')"},
		{"to_char HH is HH24, names",
			"SELECT TO_CHAR(ts, 'DY, DD MON YYYY HH')",
			"SELECT strftime(ts, '%a, %d %b %Y %H')"},
		{"to_char 12-hour clock",
			"SELECT TO_CHAR(ts, 'HH12:MI AM')",
			"SELECT strftime(ts, '%I:%M %p')"},
		{"to_char FF3",
			"SELECT TO_CHAR(ts, 'SS.FF3')",
			"SELECT (strftime(ts, '%S.') || substr(strftime(ts, '%f') || '000', 1, 3))"},
		{"to_char percent escaped", "SELECT TO_CHAR(d, 'YYYY%')", "SELECT strftime(d, '%Y%%')"},
		{"to_varchar numeric format not translated", "SELECT TO_VARCHAR(n, '999.00')", "SELECT CAST(n AS VARCHAR)"},
		{"to_varchar no format", "SELECT TO_VARCHAR(n)", "SELECT CAST(n AS VARCHAR)"},
		{"to_varchar non-literal format", "SELECT TO_VARCHAR(d, f)", "SELECT CAST(d AS VARCHAR)"},
		{"to_date with format",
			"SELECT TO_DATE('02/01/2026', 'DD/MM/YYYY')",
			"SELECT CAST(strptime('02/01/2026', '%d/%m/%Y') AS DATE)"},
		{"to_timestamp with format",
			"SELECT TO_TIMESTAMP(s, 'YYYY-MM-DD HH24:MI')",
			"SELECT CAST(strptime(s, '%Y-%m-%d %H:%M') AS TIMESTAMP)"},
		{"to_date auto", "SELECT TO_DATE(s, 'AUTO')", "SELECT CAST(s AS DATE)"},

		// The VARIANT path rule no longer reaches into string literals.
		{"colon in literal untouched", "SELECT 'a:b' AS s", "SELECT 'a:b' AS s"},
		{"variant path next to literal", "SELECT col:key, 'x:y' FROM t", "SELECT col->>'key', 'x:y' FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Rewrite(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.expect {
				t.Errorf("Rewrite(%q)\n got  %q\n want %q", tt.input, got, tt.expect)
			}
		})
	}
}
