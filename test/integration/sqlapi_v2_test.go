//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// These tests call the SQL API v2 over plain HTTP, as a non-driver client
// does, and check the response shape from the Snowflake SQL API reference:
// https://docs.snowflake.com/en/developer-guide/sql-api/reference

// postV2 sends one statement to /api/v2/statements and decodes the body.
func postV2(t *testing.T, statement string) (int, map[string]interface{}) {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"statement": statement})
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/api/v2/statements", testPort)
	resp, err := http.Post(url, "application/json", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if _, ok := body["success"]; ok {
		t.Errorf("%q: body is wrapped in the session API envelope: %s", statement, raw)
	}
	return resp.StatusCode, body
}

// mustPostV2 runs a statement that must succeed and returns its body.
func mustPostV2(t *testing.T, statement string) map[string]interface{} {
	t.Helper()
	status, body := postV2(t, statement)
	if status != http.StatusOK {
		t.Fatalf("%q: status %d, body %v", statement, status, body)
	}
	if body["code"] != "090001" || body["statementHandle"] == nil {
		t.Fatalf("%q: not a ResultSet: %v", statement, body)
	}
	return body
}

func TestSQLAPIv2_Select(t *testing.T) {
	body := mustPostV2(t, "SELECT 1 AS n, 'x' AS s, NULL AS z")

	meta, ok := body["resultSetMetaData"].(map[string]interface{})
	if !ok {
		t.Fatalf("resultSetMetaData missing at top level: %v", body)
	}
	if meta["numRows"] != float64(1) || meta["format"] != "jsonv2" {
		t.Errorf("resultSetMetaData = %v", meta)
	}
	if rowType, _ := meta["rowType"].([]interface{}); len(rowType) != 3 {
		t.Errorf("rowType = %v, want 3 columns", meta["rowType"])
	}
	data, _ := body["data"].([]interface{})
	if len(data) != 1 {
		t.Fatalf("data = %v, want one row", body["data"])
	}
	row := data[0].([]interface{})
	if row[0] != "1" || row[1] != "x" || row[2] != nil {
		t.Errorf("row = %v, want [\"1\", \"x\", null]", row)
	}
}

func TestSQLAPIv2_DML(t *testing.T) {
	mustPostV2(t, "CREATE OR REPLACE TABLE sqlapi_v2_dml (a INT)")
	t.Cleanup(func() { postV2(t, "DROP TABLE IF EXISTS sqlapi_v2_dml") })

	tests := []struct {
		statement, column, stat string
		want                    float64
	}{
		{"INSERT INTO sqlapi_v2_dml VALUES (1), (2), (3)", "number of rows inserted", "numRowsInserted", 3},
		{"UPDATE sqlapi_v2_dml SET a = 10 WHERE a > 1", "number of rows updated", "numRowsUpdated", 2},
		{"DELETE FROM sqlapi_v2_dml WHERE a = 1", "number of rows deleted", "numRowsDeleted", 1},
	}
	for _, tt := range tests {
		body := mustPostV2(t, tt.statement)

		stats, _ := body["stats"].(map[string]interface{})
		if stats[tt.stat] != tt.want {
			t.Errorf("%q: stats = %v, want %s: %v", tt.statement, body["stats"], tt.stat, tt.want)
		}
		rowType := body["resultSetMetaData"].(map[string]interface{})["rowType"].([]interface{})
		if name := rowType[0].(map[string]interface{})["name"]; name != tt.column {
			t.Errorf("%q: column = %v, want %q", tt.statement, name, tt.column)
		}
	}

	body := mustPostV2(t, "SELECT a FROM sqlapi_v2_dml ORDER BY a")
	if got := fmt.Sprint(body["data"]); got != "[[10] [10]]" {
		t.Errorf("table after DML = %s, want [[10] [10]]", got)
	}
}

func TestSQLAPIv2_SQLError(t *testing.T) {
	status, body := postV2(t, "SELECT * FROM sqlapi_v2_no_such_table")

	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body %v)", status, body)
	}
	if body["code"] != "002043" {
		t.Errorf("code = %v, want 002043", body["code"])
	}
	if msg, _ := body["message"].(string); !strings.Contains(msg, "sqlapi_v2_no_such_table") {
		t.Errorf("message = %q, want it to name the table", msg)
	}
	handle, _ := body["statementHandle"].(string)
	if handle == "" || body["statementStatusUrl"] != "/api/v2/statements/"+handle {
		t.Errorf("statementHandle/statementStatusUrl = %v / %v", body["statementHandle"], body["statementStatusUrl"])
	}
}

func TestSQLAPIv2_EmptyStatement(t *testing.T) {
	status, body := postV2(t, "   ")

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %v)", status, body)
	}
	if body["message"] != "empty statement" {
		t.Errorf("message = %v, want \"empty statement\"", body["message"])
	}
}
