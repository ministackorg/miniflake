package rewriter

import (
	"strings"
	"testing"
)

func mustRewrite(t *testing.T, sql string) string {
	t.Helper()
	got, err := Rewrite(sql)
	if err != nil {
		t.Fatalf("rewrite(%q): %v", sql, err)
	}
	return got
}

func TestRewriteStreams(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"CREATE STREAM s ON TABLE t":            "MINIFLAKE_CREATE_STREAM",
		"CREATE OR REPLACE STREAM s ON TABLE t": "MINIFLAKE_CREATE_STREAM",
		"DROP STREAM s":                         "MINIFLAKE_DROP_STREAM s 0",
		"DROP STREAM IF EXISTS s":               "MINIFLAKE_DROP_STREAM s 1",
		"SHOW STREAMS":                          "MINIFLAKE_SHOW_STREAMS",
		"SHOW STREAMS IN SCHEMA db.s":           "MINIFLAKE_SHOW_STREAMS db.s",
	}
	for sql, want := range cases {
		got := mustRewrite(t, sql)
		if !strings.Contains(got, want) {
			t.Errorf("%q → %q (want substring %q)", sql, got, want)
		}
	}
}

func TestRewriteStageCommands(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"LIST @s":                            "MINIFLAKE_LIST",
		"LS @s":                              "MINIFLAKE_LIST",
		"LIST @db.sch.s/path PATTERN = '.*'": "MINIFLAKE_LIST",
		"REMOVE @s":                          "MINIFLAKE_REMOVE",
		"RM @s/path":                         "MINIFLAKE_REMOVE",
		"LIST @s;":                           "MINIFLAKE_LIST LIST @s ",
	}
	for sql, want := range cases {
		got := mustRewrite(t, sql)
		if !strings.Contains(got, want) {
			t.Errorf("%q → %q (want substring %q)", sql, got, want)
		}
	}

	// A LIST that isn't a stage command must not be wrapped.
	if got := mustRewrite(t, "LISTAGG(x)"); strings.Contains(got, "MINIFLAKE_LIST") {
		t.Errorf("LISTAGG wrongly wrapped: %q", got)
	}
}

func TestRewriteTasks(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"CREATE TASK t SCHEDULE = '1 MINUTE' AS SELECT 1":                           "MINIFLAKE_CREATE_TASK",
		"CREATE OR REPLACE TASK t WAREHOUSE = wh SCHEDULE = '5 MINUTE' AS SELECT 1": "MINIFLAKE_CREATE_TASK",
		"DROP TASK t":            "MINIFLAKE_DROP_TASK t 0",
		"DROP TASK IF EXISTS t":  "MINIFLAKE_DROP_TASK t 1",
		"ALTER TASK t RESUME":    "MINIFLAKE_ALTER_TASK t RESUME",
		"ALTER TASK t SUSPEND":   "MINIFLAKE_ALTER_TASK t SUSPEND",
		"SHOW TASKS":             "MINIFLAKE_SHOW_TASKS",
		"SHOW TASKS IN SCHEMA s": "MINIFLAKE_SHOW_TASKS s",
		"EXECUTE TASK t":         "MINIFLAKE_EXECUTE_TASK t",
	}
	for sql, want := range cases {
		got := mustRewrite(t, sql)
		if !strings.Contains(got, want) {
			t.Errorf("%q → %q (want substring %q)", sql, got, want)
		}
	}
}

func TestRewriteShowParameters(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"SHOW PARAMETERS":                         "MINIFLAKE_SHOW_PARAMETERS SESSION |",
		"SHOW PARAMETERS LIKE 'TIME%'":            "MINIFLAKE_SHOW_PARAMETERS SESSION | TIME%",
		"SHOW PARAMETERS IN ACCOUNT":              "MINIFLAKE_SHOW_PARAMETERS ACCOUNT |",
		"SHOW PARAMETERS LIKE 'AUTO%' IN SESSION": "MINIFLAKE_SHOW_PARAMETERS SESSION | AUTO%",
		"SHOW PARAMETERS FOR ACCOUNT":             "MINIFLAKE_SHOW_PARAMETERS ACCOUNT |",
	}
	for sql, want := range cases {
		got := mustRewrite(t, sql)
		if !strings.Contains(got, want) {
			t.Errorf("%q → %q (want substring %q)", sql, got, want)
		}
	}
}

func TestRewritePipes(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"CREATE PIPE p AS COPY INTO t FROM @s":            "MINIFLAKE_CREATE_PIPE",
		"CREATE OR REPLACE PIPE p AS COPY INTO t FROM @s": "MINIFLAKE_CREATE_PIPE",
		"DROP PIPE p":            "MINIFLAKE_DROP_PIPE p 0",
		"DROP PIPE IF EXISTS p":  "MINIFLAKE_DROP_PIPE p 1",
		"SHOW PIPES":             "MINIFLAKE_SHOW_PIPES",
		"SHOW PIPES IN SCHEMA s": "MINIFLAKE_SHOW_PIPES s",
	}
	for sql, want := range cases {
		got := mustRewrite(t, sql)
		if !strings.Contains(got, want) {
			t.Errorf("%q → %q (want substring %q)", sql, got, want)
		}
	}
}

func TestRewriteUndrop(t *testing.T) {
	t.Parallel()
	got := mustRewrite(t, "UNDROP TABLE t")
	if !strings.Contains(got, "MINIFLAKE_UNDROP_TABLE t") {
		t.Errorf("UNDROP: %q", got)
	}
}

func TestRewriteMergeUpsert(t *testing.T) {
	t.Parallel()
	got := mustRewrite(t, `MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)`)
	if !strings.Contains(got, "INSERT INTO target") {
		t.Errorf("MERGE upsert target: %q", got)
	}
	if !strings.Contains(got, "ON CONFLICT (id) DO UPDATE SET") {
		t.Errorf("MERGE upsert conflict: %q", got)
	}
	if !strings.Contains(got, "excluded.v") {
		t.Errorf("MERGE upsert excluded ref: %q", got)
	}
}

func TestRewriteMergeUnsupportedFallsThrough(t *testing.T) {
	t.Parallel()
	// MERGE with WHEN MATCHED ... AND <condition> isn't a simple upsert —
	// we pass it through unchanged so DuckDB's parser surfaces a real error
	// instead of a silently-wrong rewrite.
	sql := "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED AND s.flag THEN DELETE"
	got := mustRewrite(t, sql)
	if strings.Contains(got, "ON CONFLICT") {
		t.Errorf("complex MERGE should not be rewritten: %q", got)
	}
}
