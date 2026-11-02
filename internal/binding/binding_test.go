package binding

import (
	"encoding/json"
	"strings"
	"testing"
)

func str(t *testing.T, typ, v string) Binding {
	t.Helper()
	raw, _ := json.Marshal(v)
	return Binding{Type: typ, Value: raw}
}

func TestSubstitute(t *testing.T) {
	t.Parallel()
	null := Binding{Type: "TEXT", Value: json.RawMessage("null")}

	tests := []struct {
		name string
		sql  string
		b    map[string]Binding
		want string
	}{
		{
			name: "jdbc query from issue",
			sql:  "SELECT ITEM_ID FROM TEST WHERE ITEM_ID = ? ORDER BY ITEM_ID ASC LIMIT ?",
			b:    map[string]Binding{"1": str(t, "TEXT", "1406218"), "2": str(t, "FIXED", "1")},
			want: "SELECT ITEM_ID FROM TEST WHERE ITEM_ID = '1406218' ORDER BY ITEM_ID ASC LIMIT 1",
		},
		{
			name: "no bindings leaves sql alone",
			sql:  "SELECT '?' , ?",
			b:    nil,
			want: "SELECT '?' , ?",
		},
		{
			name: "text is quote-escaped",
			sql:  "SELECT ?",
			b:    map[string]Binding{"1": str(t, "TEXT", "x'; DROP TABLE t; --")},
			want: "SELECT 'x''; DROP TABLE t; --'",
		},
		{
			name: "placeholders in strings, identifiers and comments are skipped",
			sql:  "SELECT '?', \"a?b\", ? -- ?\n, ? /* ? */ , $$?$$",
			b:    map[string]Binding{"1": str(t, "FIXED", "1"), "2": str(t, "FIXED", "2")},
			want: "SELECT '?', \"a?b\", 1 -- ?\n, 2 /* ? */ , $$?$$",
		},
		{
			name: "escaped quote does not end the string",
			sql:  "SELECT 'it''s ?', ?",
			b:    map[string]Binding{"1": str(t, "FIXED", "7")},
			want: "SELECT 'it''s ?', 7",
		},
		{
			name: "numbered placeholders, reuse and casts",
			sql:  "SELECT :2, :1, :1::VARCHAR",
			b:    map[string]Binding{"1": str(t, "FIXED", "1"), "2": str(t, "FIXED", "2")},
			want: "SELECT 2, 1, 1::VARCHAR",
		},
		{
			name: "double colon cast is not a placeholder",
			sql:  "SELECT x::INT, ?",
			b:    map[string]Binding{"1": str(t, "FIXED", "1")},
			want: "SELECT x::INT, 1",
		},
		{
			name: "negative numbers are parenthesised",
			sql:  "SELECT 5 -?",
			b:    map[string]Binding{"1": str(t, "FIXED", "-1")},
			want: "SELECT 5 -(-1)",
		},
		{
			name: "null",
			sql:  "SELECT ?",
			b:    map[string]Binding{"1": null},
			want: "SELECT NULL",
		},
		{
			name: "boolean",
			sql:  "SELECT ?, ?",
			b:    map[string]Binding{"1": str(t, "BOOLEAN", "true"), "2": str(t, "BOOLEAN", "false")},
			want: "SELECT TRUE, FALSE",
		},
		{
			name: "real",
			sql:  "SELECT ?",
			b:    map[string]Binding{"1": str(t, "REAL", "1.5")},
			want: "SELECT 1.5::DOUBLE",
		},
		{
			name: "date from epoch millis",
			sql:  "SELECT ?",
			b:    map[string]Binding{"1": str(t, "DATE", "1700000000000")},
			want: "SELECT CAST('2023-11-14' AS DATE)",
		},
		{
			name: "timestamp ntz from epoch nanos",
			sql:  "SELECT ?",
			b:    map[string]Binding{"1": str(t, "TIMESTAMP_NTZ", "1700000000000000000")},
			want: "SELECT CAST('2023-11-14 22:13:20.000000' AS TIMESTAMP)",
		},
		{
			name: "timestamp tz with offset index",
			sql:  "SELECT ?",
			b:    map[string]Binding{"1": str(t, "TIMESTAMP_TZ", "1700000000000000000 1440")},
			want: "SELECT CAST('2023-11-14 22:13:20.000000+00' AS TIMESTAMPTZ)",
		},
		{
			name: "time from nanos",
			sql:  "SELECT ?",
			b:    map[string]Binding{"1": str(t, "TIME", "3723000000000")},
			want: "SELECT CAST('01:02:03.000000' AS TIME)",
		},
		{
			name: "binary",
			sql:  "SELECT ?",
			b:    map[string]Binding{"1": str(t, "BINARY", "DEADbeef")},
			want: "SELECT from_hex('DEADbeef')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Substitute(tt.sql, tt.b)
			if err != nil {
				t.Fatalf("Substitute: %v", err)
			}
			if got != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestSubstitute_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		b       map[string]Binding
		wantErr string
	}{
		{"missing binding", "SELECT ?, ?", map[string]Binding{"1": str(t, "FIXED", "1")}, "bind variable 2 not set"},
		{"fixed injection rejected", "SELECT ?", map[string]Binding{"1": str(t, "FIXED", "1; DROP TABLE t")}, "cannot bind"},
		{"bad boolean", "SELECT ?", map[string]Binding{"1": str(t, "BOOLEAN", "maybe")}, "cannot bind"},
		{"bad binary", "SELECT ?", map[string]Binding{"1": str(t, "BINARY", "zz'")}, "cannot bind"},
		{"array bind", "SELECT ?", map[string]Binding{"1": {Type: "FIXED", Value: json.RawMessage(`["1","2"]`)}}, "array bindings"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Substitute(tt.sql, tt.b)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}
