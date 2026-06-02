package udf

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// UDFLanguage identifies the language of a user-defined function.
type UDFLanguage string

const (
	LangSQL        UDFLanguage = "SQL"
	LangJavaScript UDFLanguage = "JAVASCRIPT"
	LangPython     UDFLanguage = "PYTHON"
)

// UDF represents a user-defined function or stored procedure.
type UDF struct {
	Name       string
	Database   string
	Schema     string
	Language   UDFLanguage
	Args       []UDFArg
	ReturnType string
	Body       string
	IsProc     bool // true for stored procedures
	CreatedAt  time.Time
}

// UDFArg describes a single function argument.
type UDFArg struct {
	Name string
	Type string
}

// Registry stores and manages UDFs and stored procedures.
type Registry struct {
	mu   sync.RWMutex
	udfs map[string]*UDF // key: db.schema.name(arg_types)
}

// NewRegistry creates an empty UDF registry.
func NewRegistry() *Registry {
	return &Registry{
		udfs: make(map[string]*UDF),
	}
}

// makeKey builds the registry key for a UDF.
func makeKey(db, schema, name string, argTypes []string) string {
	return fmt.Sprintf("%s.%s.%s(%s)",
		strings.ToUpper(db),
		strings.ToUpper(schema),
		strings.ToUpper(name),
		strings.ToUpper(strings.Join(argTypes, ",")),
	)
}

// makeKeyFromUDF builds the registry key from a UDF struct.
func makeKeyFromUDF(u *UDF) string {
	argTypes := make([]string, len(u.Args))
	for i, a := range u.Args {
		argTypes[i] = a.Type
	}
	return makeKey(u.Database, u.Schema, u.Name, argTypes)
}

// Register adds a UDF to the registry.
func (r *Registry) Register(udf *UDF) error {
	if udf.Name == "" {
		return fmt.Errorf("udf: name is required")
	}
	if udf.CreatedAt.IsZero() {
		udf.CreatedAt = time.Now()
	}

	key := makeKeyFromUDF(udf)

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.udfs[key]; exists {
		return fmt.Errorf("udf: '%s' already exists", key)
	}
	r.udfs[key] = udf
	return nil
}

// Get retrieves a UDF by its fully qualified name and argument types.
func (r *Registry) Get(db, schema, name string, argTypes []string) (*UDF, error) {
	key := makeKey(db, schema, name, argTypes)
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.udfs[key]
	if !ok {
		return nil, fmt.Errorf("udf: '%s' not found", key)
	}
	return u, nil
}

// Drop removes a UDF from the registry. It matches by db, schema, and name
// regardless of argument types (drops first match).
func (r *Registry) Drop(db, schema, name string) error {
	prefix := fmt.Sprintf("%s.%s.%s(",
		strings.ToUpper(db),
		strings.ToUpper(schema),
		strings.ToUpper(name),
	)

	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.udfs {
		if strings.HasPrefix(key, prefix) {
			delete(r.udfs, key)
			return nil
		}
	}
	return fmt.Errorf("udf: '%s.%s.%s' not found", db, schema, name)
}

// List returns all UDFs in a given database and schema.
func (r *Registry) List(db, schema string) []*UDF {
	prefix := fmt.Sprintf("%s.%s.", strings.ToUpper(db), strings.ToUpper(schema))
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*UDF
	for key, u := range r.udfs {
		if strings.HasPrefix(key, prefix) {
			result = append(result, u)
		}
	}
	return result
}

// ExecuteSQL executes a SQL UDF by substituting argument values into the body.
// The body should reference arguments by name (e.g., "x + y").
// This returns the substituted SQL expression as a string (caller executes it).
func (r *Registry) ExecuteSQL(udf *UDF, args map[string]interface{}) (string, error) {
	if udf.Language != LangSQL {
		return "", fmt.Errorf("udf: expected SQL language, got %s", udf.Language)
	}
	body := udf.Body
	for _, arg := range udf.Args {
		val := args[arg.Name]
		var replacement string
		switch v := val.(type) {
		case string:
			replacement = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		case nil:
			replacement = "NULL"
		default:
			replacement = fmt.Sprintf("%v", v)
		}
		body = strings.ReplaceAll(body, arg.Name, replacement)
	}
	return body, nil
}

// ExecuteJavaScript executes a JavaScript UDF using the goja runtime.
// Returns an error if goja is not available (stubbed without goja dependency).
func (r *Registry) ExecuteJavaScript(udf *UDF, args map[string]interface{}) (interface{}, error) {
	if udf.Language != LangJavaScript {
		return nil, fmt.Errorf("udf: expected JAVASCRIPT language, got %s", udf.Language)
	}
	// Stubbed: goja (github.com/dop251/goja) is not linked.
	return nil, fmt.Errorf("udf: JavaScript execution not available (goja not linked)")
}

// ExecutePython executes a Python UDF by spawning a python3 subprocess.
// It wraps the UDF body in a function call, passes arguments as JSON,
// and captures stdout as the return value.
func (r *Registry) ExecutePython(udf *UDF, args map[string]interface{}) (interface{}, error) {
	if udf.Language != LangPython {
		return nil, fmt.Errorf("udf: expected PYTHON language, got %s", udf.Language)
	}

	// Build argument assignments.
	var setup strings.Builder
	setup.WriteString("import json, sys\n")
	for _, arg := range udf.Args {
		val := args[arg.Name]
		jsonVal, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("udf python: marshal arg %s: %w", arg.Name, err)
		}
		fmt.Fprintf(&setup, "%s = json.loads('%s')\n", arg.Name, string(jsonVal))
	}

	// The body is expected to be a Python expression.
	script := setup.String() + "\n_result = " + udf.Body + "\nprint(json.dumps(_result))\n"

	cmd := exec.Command("python3", "-c", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("udf python: execution failed: %s: %w", strings.TrimSpace(string(out)), err)
	}

	var result interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(out))), &result); err != nil {
		return strings.TrimSpace(string(out)), nil
	}
	return result, nil
}
