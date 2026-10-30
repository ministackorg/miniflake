package binding

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func arr(typ string, vals ...interface{}) Binding {
	raw, _ := json.Marshal(vals)
	return Binding{Type: typ, Value: raw}
}

func TestExpand_InsertValuesFoldsToOneStatement(t *testing.T) {
	t.Parallel()
	b := map[string]Binding{
		"1": arr("FIXED", "1", "2", "3"),
		"2": arr("TEXT", "a", nil, "it's"),
	}
	tests := []string{
		"INSERT INTO t VALUES (?, ?)",
		"insert into t (id, name) values (?, ?);",
		"INSERT INTO t(id, name)\nVALUES\n  (?, ?)",
	}
	for _, sql := range tests {
		got, err := Expand(sql, b)
		if err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
		if len(got) != 1 {
			t.Fatalf("%q: got %d statements, want 1: %q", sql, len(got), got)
		}
		if !strings.Contains(got[0], "(1, 'a'), (2, NULL), (3, 'it''s')") {
			t.Errorf("%q: unexpected statement %q", sql, got[0])
		}
		if !strings.HasPrefix(strings.ToUpper(got[0]), "INSERT INTO T") {
			t.Errorf("%q: prefix lost: %q", sql, got[0])
		}
	}
}

func TestExpand_PerRowFallback(t *testing.T) {
	t.Parallel()
	b := map[string]Binding{
		"1": arr("TEXT", "x", "y"),
		"2": str(t, "FIXED", "9"), // scalar repeats for every row
	}
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{"update", "UPDATE t SET a = ? WHERE id = ?", []string{"UPDATE t SET a = 'x' WHERE id = 9", "UPDATE t SET a = 'y' WHERE id = 9"}},
		{"insert select", "INSERT INTO t SELECT ?, ?", []string{"INSERT INTO t SELECT 'x', 9", "INSERT INTO t SELECT 'y', 9"}},
		{"placeholder before values", "INSERT INTO t (c) SELECT ? FROM (VALUES (1)) v", []string{"INSERT INTO t (c) SELECT 'x' FROM (VALUES (1)) v", "INSERT INTO t (c) SELECT 'y' FROM (VALUES (1)) v"}},
		{"two tuples", "INSERT INTO t VALUES (?, 1), (?, 2)", []string{"INSERT INTO t VALUES ('x', 1), (9, 2)", "INSERT INTO t VALUES ('y', 1), (9, 2)"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Expand(tt.sql, b)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestExpand_ScalarUnchanged(t *testing.T) {
	t.Parallel()
	got, err := Expand("SELECT ?", map[string]Binding{"1": str(t, "FIXED", "1")})
	if err != nil || !reflect.DeepEqual(got, []string{"SELECT 1"}) {
		t.Fatalf("got %q, %v", got, err)
	}
	got, err = Expand("SELECT 1", nil)
	if err != nil || !reflect.DeepEqual(got, []string{"SELECT 1"}) {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestExpand_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		b    map[string]Binding
		want string
	}{
		{"length mismatch", map[string]Binding{"1": arr("FIXED", "1", "2"), "2": arr("FIXED", "1")}, "same length"},
		{"empty array", map[string]Binding{"1": {Type: "FIXED", Value: json.RawMessage("[]")}, "2": str(t, "TEXT", "a")}, "must not be empty"},
		{"bad element", map[string]Binding{"1": arr("FIXED", "1", "1; DROP TABLE t"), "2": arr("TEXT", "a", "b")}, "cannot bind"},
		{"missing variable", map[string]Binding{"1": arr("FIXED", "1")}, "bind variable 2 not set"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Expand("INSERT INTO t VALUES (?, ?)", tt.b)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want containing %q", err, tt.want)
			}
		})
	}
}
