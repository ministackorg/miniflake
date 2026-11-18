package udf

import (
	"reflect"
	"testing"
)

func TestParseCreate(t *testing.T) {
	d, err := ParseCreate(`CREATE OR REPLACE SECURE FUNCTION db1.sch1."MixedName"(a NUMBER(10, 2), "b" VARCHAR DEFAULT 'x,y')
		RETURNS NUMBER(10,2) NOT NULL LANGUAGE SQL COMMENT = 'c' AS $$ a + 1 $$`)
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != KindFunction || !d.OrReplace || !d.Secure || d.IfNotExists {
		t.Errorf("flags = %+v", d)
	}
	if !reflect.DeepEqual(d.NameParts, []string{"db1", "sch1", "MixedName"}) || !d.QuotedName {
		t.Errorf("name = %v quoted=%v", d.NameParts, d.QuotedName)
	}
	wantArgs := []UDFArg{{Name: "A", Type: "NUMBER(10, 2)"}, {Name: "b", Type: "VARCHAR", Default: "'x,y'"}}
	if !reflect.DeepEqual(d.Args, wantArgs) {
		t.Errorf("args = %+v", d.Args)
	}
	if d.ReturnType != "NUMBER(10,2)" || d.Language != LangSQL || d.Body != " a + 1 " {
		t.Errorf("returns=%q language=%q body=%q", d.ReturnType, d.Language, d.Body)
	}

	d, err = ParseCreate(`create procedure if not exists p() returns table (x int, y string) language python as 'it''s'`)
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != KindProcedure || !d.IfNotExists || d.Language != LangPython || d.Body != "it's" {
		t.Errorf("procedure = %+v", d)
	}
	if !reflect.DeepEqual(d.ReturnTable, []UDFArg{{Name: "X", Type: "int"}, {Name: "Y", Type: "string"}}) {
		t.Errorf("return table = %+v", d.ReturnTable)
	}

	for _, bad := range []string{
		"CREATE FUNCTION f RETURNS INT AS '1'",
		"CREATE FUNCTION f(a) RETURNS INT AS '1'",
		"CREATE FUNCTION f(a INT) AS '1'",
		"CREATE FUNCTION f(a INT) RETURNS INT AS $$ 1",
	} {
		if _, err := ParseCreate(bad); err == nil {
			t.Errorf("ParseCreate(%q) succeeded", bad)
		}
	}
}

func TestRoutineArity(t *testing.T) {
	r := &Routine{Args: []UDFArg{{Name: "A"}, {Name: "B", Default: "1"}}}
	for n, want := range map[int]bool{0: false, 1: true, 2: true, 3: false} {
		if r.AcceptsArity(n) != want {
			t.Errorf("AcceptsArity(%d) = %v", n, !want)
		}
	}
}
