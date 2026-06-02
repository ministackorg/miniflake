package rewriter

import (
	"strings"
	"testing"
)

func TestRewrite(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		// 1. VARIANT / semi-structured access
		{
			name:   "variant colon access",
			input:  "SELECT col:key FROM t",
			expect: "SELECT col->>'key' FROM t",
		},
		{
			name:   "variant colon access nested",
			input:  "SELECT col:key1.key2 FROM t",
			expect: "SELECT col->>'key1'->>'key2' FROM t",
		},
		{
			name:   "variant colon access with cast",
			input:  "SELECT col:key::VARCHAR FROM t",
			expect: "SELECT CAST(col->>'key' AS VARCHAR) FROM t",
		},
		{
			name:   "variant array access",
			input:  "SELECT col[0] FROM t",
			expect: "SELECT col->>0 FROM t",
		},

		// 2. FLATTEN
		{
			name:   "flatten table function",
			input:  "SELECT * FROM TABLE(FLATTEN(input => col))",
			expect: "SELECT * FROM unnest(col)",
		},
		{
			name:   "flatten without input =>",
			input:  "SELECT * FROM TABLE(FLATTEN(col))",
			expect: "SELECT * FROM unnest(col)",
		},
		{
			name:   "lateral flatten",
			input:  "SELECT f.value FROM t, LATERAL FLATTEN(input => t.arr) AS f",
			expect: "SELECT f.value FROM t, LATERAL unnest(t.arr) AS f",
		},

		// 3. QUALIFY — pass through
		{
			name:   "qualify pass through",
			input:  "SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1",
			expect: "SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1",
		},

		// 4. TRY_CAST — pass through
		{
			name:   "try_cast pass through",
			input:  "SELECT TRY_CAST('123' AS INTEGER)",
			expect: "SELECT TRY_CAST('123' AS INTEGER)",
		},

		// 5. IFF
		{
			name:   "iff function",
			input:  "SELECT IFF(x > 0, 'pos', 'neg') FROM t",
			expect: "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t",
		},

		// 6. NVL / NVL2
		{
			name:   "nvl function",
			input:  "SELECT NVL(a, b) FROM t",
			expect: "SELECT COALESCE(a, b) FROM t",
		},
		{
			name:   "nvl2 function",
			input:  "SELECT NVL2(a, b, c) FROM t",
			expect: "SELECT CASE WHEN a IS NOT NULL THEN b ELSE c END FROM t",
		},

		// 7. SQUARE / DIV0 / DIV0NULL
		{
			name:   "square function",
			input:  "SELECT SQUARE(x) FROM t",
			expect: "SELECT POWER(x, 2) FROM t",
		},
		{
			name:   "div0 function",
			input:  "SELECT DIV0(a, b) FROM t",
			expect: "SELECT CASE WHEN b = 0 THEN 0 ELSE a / b END FROM t",
		},
		{
			name:   "div0null function",
			input:  "SELECT DIV0NULL(a, b) FROM t",
			expect: "SELECT CASE WHEN b = 0 THEN NULL ELSE a / b END FROM t",
		},

		// 8. ARRAY_CONSTRUCT / ARRAY_SIZE
		{
			name:   "array_construct",
			input:  "SELECT ARRAY_CONSTRUCT(1, 2, 3)",
			expect: "SELECT [1, 2, 3]",
		},
		{
			name:   "array_size",
			input:  "SELECT ARRAY_SIZE(arr) FROM t",
			expect: "SELECT len(arr) FROM t",
		},

		// 9. OBJECT_CONSTRUCT
		{
			name:   "object_construct",
			input:  "SELECT OBJECT_CONSTRUCT('k1', v1, 'k2', v2)",
			expect: "SELECT {'k1': v1, 'k2': v2}",
		},

		// 10. PARSE_JSON
		{
			name:   "parse_json",
			input:  "SELECT PARSE_JSON('{\"a\": 1}')",
			expect: "SELECT '{\"a\": 1}'::JSON",
		},

		// 11. TO_VARCHAR / TO_CHAR / TO_NUMBER / TO_DATE / TO_TIMESTAMP
		{
			name:   "to_varchar",
			input:  "SELECT TO_VARCHAR(x) FROM t",
			expect: "SELECT CAST(x AS VARCHAR) FROM t",
		},
		{
			name:   "to_char",
			input:  "SELECT TO_CHAR(x) FROM t",
			expect: "SELECT CAST(x AS VARCHAR) FROM t",
		},
		{
			name:   "to_number",
			input:  "SELECT TO_NUMBER('123') FROM t",
			expect: "SELECT CAST('123' AS NUMERIC) FROM t",
		},
		{
			name:   "to_number with precision",
			input:  "SELECT TO_NUMBER('123.45', 10, 2) FROM t",
			expect: "SELECT CAST('123.45' AS DECIMAL(10, 2)) FROM t",
		},
		{
			name:   "to_date",
			input:  "SELECT TO_DATE('2024-01-01') FROM t",
			expect: "SELECT CAST('2024-01-01' AS DATE) FROM t",
		},
		{
			name:   "to_timestamp",
			input:  "SELECT TO_TIMESTAMP('2024-01-01 00:00:00') FROM t",
			expect: "SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP) FROM t",
		},

		// 12. DATEADD / DATEDIFF
		{
			name:   "dateadd",
			input:  "SELECT DATEADD(day, 5, col) FROM t",
			expect: "SELECT col + INTERVAL '5 day' FROM t",
		},
		{
			name:   "datediff",
			input:  "SELECT DATEDIFF(day, a, b) FROM t",
			expect: "SELECT DATE_DIFF('day', a, b) FROM t",
		},

		// 13. SHOW commands
		{
			name:   "show databases",
			input:  "SHOW DATABASES",
			expect: "SELECT DISTINCT catalog_name AS \"name\" FROM information_schema.schemata",
		},
		{
			name:   "show schemas",
			input:  "SHOW SCHEMAS",
			expect: "SELECT schema_name AS \"name\" FROM information_schema.schemata",
		},
		{
			name:   "show schemas in database",
			input:  "SHOW SCHEMAS IN DATABASE mydb",
			expect: "SELECT schema_name AS \"name\" FROM information_schema.schemata WHERE catalog_name = 'mydb'",
		},
		{
			name:   "show tables",
			input:  "SHOW TABLES",
			expect: "SELECT table_name AS \"name\" FROM information_schema.tables",
		},
		{
			name:   "show tables in schema",
			input:  "SHOW TABLES IN SCHEMA public",
			expect: "SELECT table_name AS \"name\" FROM information_schema.tables WHERE table_schema = 'public'",
		},
		{
			name:   "show columns",
			input:  "SHOW COLUMNS IN my_table",
			expect: "SELECT column_name AS \"name\", data_type AS \"type\" FROM information_schema.columns WHERE table_name = 'my_table'",
		},
		{
			name:   "show warehouses",
			input:  "SHOW WAREHOUSES",
			expect: "SELECT 'MINIFLAKE_WH' AS \"name\", 'STARTED' AS \"state\", 'X-SMALL' AS \"size\"",
		},

		// 14. DESCRIBE / DESC TABLE
		{
			name:   "describe table",
			input:  "DESCRIBE TABLE my_table",
			expect: "DESCRIBE my_table",
		},
		{
			name:   "desc table",
			input:  "DESC TABLE my_table",
			expect: "DESCRIBE my_table",
		},

		// 15. CREATE OR REPLACE — pass through
		{
			name:   "create or replace pass through",
			input:  "CREATE OR REPLACE TABLE t (id INT)",
			expect: "CREATE OR REPLACE TABLE t (id INT)",
		},

		// 16. COPY INTO
		{
			name:   "copy into table",
			input:  "COPY INTO my_table FROM @my_stage/path",
			expect: "/* MINIFLAKE_COPY_INTO COPY INTO my_table FROM @my_stage/path */",
		},

		// 17. PUT / GET
		{
			name:   "put command",
			input:  "PUT file:///tmp/data.csv @my_stage",
			expect: "/* MINIFLAKE_PUT PUT file:///tmp/data.csv @my_stage */",
		},
		{
			name:   "get command",
			input:  "GET @my_stage/data.csv file:///tmp/",
			expect: "/* MINIFLAKE_GET GET @my_stage/data.csv file:///tmp/ */",
		},

		// 18. USE statements
		{
			name:   "use database",
			input:  "USE DATABASE mydb",
			expect: "/* MINIFLAKE_USE_DATABASE mydb */",
		},
		{
			name:   "use schema",
			input:  "USE SCHEMA public",
			expect: "/* MINIFLAKE_USE_SCHEMA public */",
		},
		{
			name:   "use warehouse",
			input:  "USE WAREHOUSE compute_wh",
			expect: "/* MINIFLAKE_USE_WAREHOUSE compute_wh */",
		},
		{
			name:   "use role",
			input:  "USE ROLE sysadmin",
			expect: "/* MINIFLAKE_USE_ROLE sysadmin */",
		},

		// 19. IDENTIFIER()
		{
			name:   "identifier function",
			input:  "SELECT IDENTIFIER('col_name') FROM t",
			expect: "SELECT col_name FROM t",
		},

		// 20. REGEXP_LIKE / RLIKE
		{
			name:   "regexp_like",
			input:  "SELECT REGEXP_LIKE(col, '^abc') FROM t",
			expect: "SELECT regexp_matches(col, '^abc') FROM t",
		},
		{
			name:   "rlike operator",
			input:  "SELECT * FROM t WHERE col RLIKE '^abc'",
			expect: "SELECT * FROM t WHERE regexp_matches(col, '^abc')",
		},

		// 21. LISTAGG
		{
			name:   "listagg",
			input:  "SELECT LISTAGG(col, ',') FROM t",
			expect: "SELECT STRING_AGG(col, ',') FROM t",
		},

		// 22. STRTOK_TO_ARRAY
		{
			name:   "strtok_to_array",
			input:  "SELECT STRTOK_TO_ARRAY(str, ',') FROM t",
			expect: "SELECT STRING_SPLIT(str, ',') FROM t",
		},

		// 23. Data types in CREATE TABLE
		{
			name:   "number(38,0) to bigint",
			input:  "CREATE TABLE t (id NUMBER(38,0))",
			expect: "CREATE TABLE t (id BIGINT)",
		},
		{
			name:   "number(p,s) to decimal",
			input:  "CREATE TABLE t (val NUMBER(10,2))",
			expect: "CREATE TABLE t (val DECIMAL(10, 2))",
		},
		{
			name:   "variant to json",
			input:  "CREATE TABLE t (data VARIANT)",
			expect: "CREATE TABLE t (data JSON)",
		},
		{
			name:   "timestamp_ntz to timestamp",
			input:  "CREATE TABLE t (ts TIMESTAMP_NTZ)",
			expect: "CREATE TABLE t (ts TIMESTAMP)",
		},
		{
			name:   "timestamp_ltz to timestamptz",
			input:  "CREATE TABLE t (ts TIMESTAMP_LTZ)",
			expect: "CREATE TABLE t (ts TIMESTAMPTZ)",
		},
		{
			name:   "timestamp_tz to timestamptz",
			input:  "CREATE TABLE t (ts TIMESTAMP_TZ)",
			expect: "CREATE TABLE t (ts TIMESTAMPTZ)",
		},
		{
			name:   "binary to blob",
			input:  "CREATE TABLE t (data BINARY)",
			expect: "CREATE TABLE t (data BLOB)",
		},
		{
			name:   "number without precision to bigint",
			input:  "CREATE TABLE t (id NUMBER)",
			expect: "CREATE TABLE t (id BIGINT)",
		},

		// DATE_TRUNC (DuckDB supports it natively — pass through)
		{
			name:   "date_trunc pass through",
			input:  "SELECT DATE_TRUNC('month', col) FROM t",
			expect: "SELECT DATE_TRUNC('month', col) FROM t",
		},

		// ARRAY_AGG (DuckDB supports it natively — pass through)
		{
			name:   "array_agg pass through",
			input:  "SELECT ARRAY_AGG(col) FROM t",
			expect: "SELECT ARRAY_AGG(col) FROM t",
		},

		// Edge: empty input
		{
			name:   "empty input",
			input:  "",
			expect: "",
		},

		// Edge: plain SELECT pass through
		{
			name:   "plain select pass through",
			input:  "SELECT 1",
			expect: "SELECT 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Rewrite(tc.input)
			if err != nil {
				t.Fatalf("Rewrite() error: %v", err)
			}
			if got != tc.expect {
				t.Errorf("Rewrite(%q)\n  got:    %q\n  expect: %q", tc.input, got, tc.expect)
			}
		})
	}
}

func TestRewriteMulti(t *testing.T) {
	input := "SELECT 1; SELECT 2; SELECT 3"
	results, err := RewriteMulti(input)
	if err != nil {
		t.Fatalf("RewriteMulti() error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(results))
	}
	for i, expect := range []string{"SELECT 1", "SELECT 2", "SELECT 3"} {
		if results[i] != expect {
			t.Errorf("statement %d: got %q, expect %q", i, results[i], expect)
		}
	}
}

func TestRewriteMultiWithRewrites(t *testing.T) {
	input := "SELECT NVL(a, b); SELECT IFF(x > 0, 'y', 'n')"
	results, err := RewriteMulti(input)
	if err != nil {
		t.Fatalf("RewriteMulti() error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(results))
	}
	if results[0] != "SELECT COALESCE(a, b)" {
		t.Errorf("stmt 0: got %q", results[0])
	}
	if !strings.Contains(results[1], "CASE WHEN") {
		t.Errorf("stmt 1: expected CASE WHEN rewrite, got %q", results[1])
	}
}

func TestRewriteMultiQuotedSemicolon(t *testing.T) {
	input := "SELECT 'a;b'; SELECT 2"
	results, err := RewriteMulti(input)
	if err != nil {
		t.Fatalf("RewriteMulti() error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(results), results)
	}
	if results[0] != "SELECT 'a;b'" {
		t.Errorf("stmt 0: got %q", results[0])
	}
}

func TestDataTypesNotInSelect(t *testing.T) {
	// Data type rewrites should NOT apply to non-DDL statements
	input := "SELECT VARIANT FROM t"
	got, err := Rewrite(input)
	if err != nil {
		t.Fatal(err)
	}
	// VARIANT in a SELECT should not be rewritten to JSON
	if strings.Contains(got, "JSON") {
		t.Errorf("data type rewrite should not apply in SELECT: got %q", got)
	}
}

func TestCaseInsensitivity(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"select nvl(a, b) from t", "select COALESCE(a, b) from t"},
		{"SELECT iff(x > 0, 'a', 'b') FROM t", "SELECT CASE WHEN x > 0 THEN 'a' ELSE 'b' END FROM t"},
		{"select square(x)", "select POWER(x, 2)"},
		{"show databases", "SELECT DISTINCT catalog_name AS \"name\" FROM information_schema.schemata"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := Rewrite(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.expect {
				t.Errorf("got %q, expect %q", got, tc.expect)
			}
		})
	}
}
