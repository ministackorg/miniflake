package scripting

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParse_Accepts(t *testing.T) {
	for _, body := range []string{
		"BEGIN RETURN 'ok'; END;",
		"BEGIN RETURN 'ok'; END",
		" BEGIN NULL; END ",
		"DECLARE x INT; y DEFAULT 2; z NUMBER(10,2) := 1.5; BEGIN x := y + 1; RETURN x; END;",
		"BEGIN IF (1 = 1) THEN RETURN 1; ELSEIF (2 = 2) THEN RETURN 2; ELSE RETURN 3; END IF; END;",
		"BEGIN BEGIN LET a := 1; END; DECLARE b INT DEFAULT 1; BEGIN RETURN b; END; END;",
		"BEGIN SELECT 1 INTO :x; END",
		"BEGIN BEGIN TRANSACTION; INSERT INTO t VALUES (1); COMMIT; END",
		"BEGIN -- comment; with semicolon\n RETURN 'a;b'; /* block; comment */ END",
		"<<outer>> BEGIN RETURN CASE WHEN 1 = 1 THEN 'y' ELSE 'n' END; END",
	} {
		if _, err := Parse(body); err != nil {
			t.Errorf("Parse(%q): %v", body, err)
		}
	}
}

func TestParse_Rejects(t *testing.T) {
	for _, tc := range []struct {
		body        string
		unsupported bool
	}{
		{"BEGIN RETURN 1;", false},
		{"RETURN 1;", false},
		{"BEGIN IF (1=1) THEN RETURN 1; END;", false},
		{"BEGIN RETURN 1; END; RETURN 2;", false},
		{"BEGIN FOR i IN 1 TO 3 DO NULL; END FOR; END", true},
		{"BEGIN WHILE (TRUE) DO NULL; END WHILE; END", true},
		{"BEGIN LOOP BREAK; END LOOP; END", true},
		{"DECLARE c CURSOR FOR SELECT 1; BEGIN RETURN 1; END", true},
		{"DECLARE r RESULTSET; BEGIN RETURN 1; END", true},
		{"BEGIN RETURN 1; EXCEPTION WHEN OTHER THEN RETURN 2; END", true},
		{"BEGIN EXECUTE IMMEDIATE 'SELECT 1'; END", true},
	} {
		_, err := Parse(tc.body)
		if err == nil {
			t.Errorf("Parse(%q) succeeded", tc.body)
			continue
		}
		var u *UnsupportedError
		if errors.As(err, &u) != tc.unsupported {
			t.Errorf("Parse(%q): err = %v, unsupported = %v, want %v", tc.body, err, errors.As(err, &u), tc.unsupported)
		}
	}
}

// fakeExec records the SQL the interpreter sends and answers SELECT <literal>.
type fakeExec struct{ sent []string }

func (f *fakeExec) exec(_ context.Context, sql string) (*Result, error) {
	f.sent = append(f.sent, sql)
	if strings.HasPrefix(sql, "SELECT ") && !strings.Contains(sql, " FROM ") {
		return &Result{Columns: []string{"x"}, Rows: [][]interface{}{{strings.TrimPrefix(sql, "SELECT ")}}}, nil
	}
	return &Result{IsDML: true, RowsAffected: 2}, nil
}

func TestRun_SubstitutesBindsAndBareNames(t *testing.T) {
	p, err := Parse(`BEGIN
		INSERT INTO t VALUES (:a, :b, 'keep :a', x::INT, col:path);
		RETURN a || :b;
	END`)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeExec{}
	got, err := p.Run(context.Background(), f.exec, func(s string) string { return s }, []Arg{
		{Name: "A", Value: Value{V: "it's", Type: "VARCHAR"}},
		{Name: "B", Value: Value{V: int64(7), Type: "BIGINT"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "INSERT INTO t VALUES ('it''s', 7, 'keep :a', x::INT, col:path)"; f.sent[0] != want {
		t.Errorf("statement sent = %q\nwant %q", f.sent[0], want)
	}
	if got.V != "'it''s' || 7" {
		t.Errorf("RETURN expression = %v", got.V)
	}
}

func TestLiteral(t *testing.T) {
	ts := time.Date(2026, 3, 4, 5, 6, 7, 123000000, time.UTC)
	for _, tc := range []struct {
		v    Value
		want string
	}{
		{Value{}, "NULL"},
		{Value{Type: "BIGINT"}, "CAST(NULL AS BIGINT)"},
		{Value{Type: "SQLNULL"}, "NULL"},
		{Value{V: "a'b"}, "'a''b'"},
		{Value{V: true}, "TRUE"},
		{Value{V: int32(5)}, "5"},
		{Value{V: 1.5}, "CAST(1.5 AS DOUBLE)"},
		{Value{V: ts, Type: "DATE"}, "DATE '2026-03-04'"},
		{Value{V: ts, Type: "TIME"}, "TIME '05:06:07.123'"},
		{Value{V: ts, Type: "TIMESTAMP"}, "TIMESTAMP '2026-03-04 05:06:07.123'"},
		{Value{V: ts, Type: "TIMESTAMP WITH TIME ZONE"}, "TIMESTAMPTZ '2026-03-04 05:06:07.123+00'"},
		{Value{V: "1.25", Type: "DECIMAL(10,2)"}, "CAST('1.25' AS DECIMAL(10,2))"},
		{Value{V: []byte{0xab}}, "from_hex('ab')"},
		{Value{V: map[string]interface{}{"k": 1}}, `CAST('{"k":1}' AS JSON)`},
	} {
		if got := Literal(tc.v); got != tc.want {
			t.Errorf("Literal(%#v) = %s, want %s", tc.v, got, tc.want)
		}
	}
}
