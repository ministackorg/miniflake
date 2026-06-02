package extfunc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateIntegration(t *testing.T) {
	m := NewManager()

	if err := m.CreateIntegration("my_api", "aws_api_gateway", "https://example.com/api", "secret-key"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Duplicate should fail.
	if err := m.CreateIntegration("my_api", "aws_api_gateway", "https://example.com/api", ""); err == nil {
		t.Fatal("expected error for duplicate integration")
	}

	// Empty name should fail.
	if err := m.CreateIntegration("", "", "", ""); err == nil {
		t.Fatal("expected error for empty name")
	}

	// Show integrations.
	intgs := m.ShowIntegrations()
	if len(intgs) != 1 {
		t.Fatalf("expected 1 integration, got %d", len(intgs))
	}
	if intgs[0].Name != "my_api" {
		t.Errorf("expected name 'my_api', got '%s'", intgs[0].Name)
	}

	// Drop.
	if err := m.DropIntegration("my_api"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.DropIntegration("my_api"); err == nil {
		t.Fatal("expected error for non-existent integration")
	}
}

func TestCreateFunction(t *testing.T) {
	m := NewManager()

	// Create integration first.
	if err := m.CreateIntegration("test_api", "aws_api_gateway", "https://example.com", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	args := []FuncArg{
		{Name: "input", Type: "VARCHAR"},
	}

	// Function without valid integration should fail.
	if err := m.CreateFunction("DB1", "PUBLIC", "MY_FUNC", args, "VARCHAR", "nonexistent", "/endpoint"); err == nil {
		t.Fatal("expected error for non-existent integration")
	}

	// Create function.
	if err := m.CreateFunction("DB1", "PUBLIC", "MY_FUNC", args, "VARCHAR", "test_api", "/endpoint"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Duplicate should fail.
	if err := m.CreateFunction("DB1", "PUBLIC", "MY_FUNC", args, "VARCHAR", "test_api", "/endpoint"); err == nil {
		t.Fatal("expected error for duplicate function")
	}

	// Show functions.
	fns := m.ShowFunctions("DB1", "PUBLIC")
	if len(fns) != 1 {
		t.Fatalf("expected 1 function, got %d", len(fns))
	}
	if fns[0].Name != "MY_FUNC" {
		t.Errorf("expected name 'MY_FUNC', got '%s'", fns[0].Name)
	}

	// Drop.
	if err := m.DropFunction("DB1", "PUBLIC", "MY_FUNC"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := m.DropFunction("DB1", "PUBLIC", "MY_FUNC"); err == nil {
		t.Fatal("expected error for non-existent function")
	}
}

func TestCall(t *testing.T) {
	// Set up a mock HTTP server that echoes input uppercased.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req callRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Echo back: for each row [row_num, val], return [row_num, "ECHO:val"]
		resp := callResponse{Data: make([][]interface{}, len(req.Data))}
		for i, row := range req.Data {
			if len(row) >= 2 {
				resp.Data[i] = []interface{}{row[0], "ECHO:" + row[1].(string)}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	m := NewManager()

	if err := m.CreateIntegration("mock_api", "aws_api_gateway", server.URL, "test-key"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	args := []FuncArg{{Name: "input", Type: "VARCHAR"}}
	if err := m.CreateFunction("DB1", "PUBLIC", "ECHO_FUNC", args, "VARCHAR", "mock_api", server.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Call with two rows.
	results, err := m.Call(context.Background(), "DB1", "PUBLIC", "ECHO_FUNC", [][]interface{}{
		{"hello"},
		{"world"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0] != "ECHO:hello" {
		t.Errorf("expected 'ECHO:hello', got '%v'", results[0])
	}
	if results[1] != "ECHO:world" {
		t.Errorf("expected 'ECHO:world', got '%v'", results[1])
	}

	// Call non-existent function should fail.
	if _, err := m.Call(context.Background(), "DB1", "PUBLIC", "NOPE", nil); err == nil {
		t.Fatal("expected error for non-existent function")
	}
}
