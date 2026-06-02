package udf

import (
	"strings"
	"testing"
)

func TestRegister(t *testing.T) {
	r := NewRegistry()

	udf := &UDF{
		Name:       "add_nums",
		Database:   "mydb",
		Schema:     "public",
		Language:   LangSQL,
		Args:       []UDFArg{{Name: "x", Type: "INT"}, {Name: "y", Type: "INT"}},
		ReturnType: "INT",
		Body:       "x + y",
	}

	if err := r.Register(udf); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Duplicate should fail.
	if err := r.Register(udf); err == nil {
		t.Fatal("expected error on duplicate register")
	}
}

func TestRegisterEmptyName(t *testing.T) {
	r := NewRegistry()
	udf := &UDF{Language: LangSQL}
	if err := r.Register(udf); err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestGetAndDrop(t *testing.T) {
	r := NewRegistry()
	udf := &UDF{
		Name:     "my_func",
		Database: "db1",
		Schema:   "public",
		Language: LangSQL,
		Args:     []UDFArg{{Name: "a", Type: "VARCHAR"}},
		Body:     "UPPER(a)",
	}
	_ = r.Register(udf)

	got, err := r.Get("db1", "public", "my_func", []string{"VARCHAR"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "my_func" {
		t.Errorf("expected my_func, got %s", got.Name)
	}

	// Not found.
	_, err = r.Get("db1", "public", "nonexistent", []string{})
	if err == nil {
		t.Fatal("expected error for nonexistent UDF")
	}

	// Drop.
	if err := r.Drop("db1", "public", "my_func"); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	_, err = r.Get("db1", "public", "my_func", []string{"VARCHAR"})
	if err == nil {
		t.Fatal("expected error after drop")
	}

	// Drop nonexistent.
	if err := r.Drop("db1", "public", "my_func"); err == nil {
		t.Fatal("expected error dropping nonexistent")
	}
}

func TestList(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&UDF{Name: "f1", Database: "db1", Schema: "public", Language: LangSQL, Body: "1"})
	_ = r.Register(&UDF{Name: "f2", Database: "db1", Schema: "public", Language: LangSQL, Body: "2"})
	_ = r.Register(&UDF{Name: "f3", Database: "db2", Schema: "public", Language: LangSQL, Body: "3"})

	list := r.List("db1", "public")
	if len(list) != 2 {
		t.Fatalf("expected 2 UDFs, got %d", len(list))
	}
}

func TestExecuteSQL(t *testing.T) {
	r := NewRegistry()
	udf := &UDF{
		Name:     "add_nums",
		Database: "db1",
		Schema:   "public",
		Language: LangSQL,
		Args:     []UDFArg{{Name: "x", Type: "INT"}, {Name: "y", Type: "INT"}},
		Body:     "x + y",
	}

	result, err := r.ExecuteSQL(udf, map[string]interface{}{"x": 10, "y": 20})
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if result != "10 + 20" {
		t.Errorf("expected '10 + 20', got '%s'", result)
	}
}

func TestExecuteSQLWithStrings(t *testing.T) {
	r := NewRegistry()
	udf := &UDF{
		Name:     "greet",
		Database: "db1",
		Schema:   "public",
		Language: LangSQL,
		Args:     []UDFArg{{Name: "name", Type: "VARCHAR"}},
		Body:     "CONCAT('Hello, ', name)",
	}

	result, err := r.ExecuteSQL(udf, map[string]interface{}{"name": "World"})
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if !strings.Contains(result, "'World'") {
		t.Errorf("expected quoted World in result, got '%s'", result)
	}
}

func TestExecuteSQLWrongLanguage(t *testing.T) {
	r := NewRegistry()
	udf := &UDF{Language: LangJavaScript, Body: "return 1;"}
	_, err := r.ExecuteSQL(udf, nil)
	if err == nil {
		t.Fatal("expected error for wrong language")
	}
}

func TestExecuteJavaScript(t *testing.T) {
	r := NewRegistry()
	udf := &UDF{
		Name:     "js_add",
		Database: "db1",
		Schema:   "public",
		Language: LangJavaScript,
		Args:     []UDFArg{{Name: "x", Type: "INT"}, {Name: "y", Type: "INT"}},
		Body:     "return x + y;",
	}

	_, err := r.ExecuteJavaScript(udf, map[string]interface{}{"x": 1, "y": 2})
	if err != nil {
		// goja is not available, so this should return an error.
		if !strings.Contains(err.Error(), "not available") {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Skipf("JavaScript execution not available (goja not linked): %v", err)
	}
}

func TestExecuteJavaScriptWrongLanguage(t *testing.T) {
	r := NewRegistry()
	udf := &UDF{Language: LangSQL, Body: "1+1"}
	_, err := r.ExecuteJavaScript(udf, nil)
	if err == nil {
		t.Fatal("expected error for wrong language")
	}
}
