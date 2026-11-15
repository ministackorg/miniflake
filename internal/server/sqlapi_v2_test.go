package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// SQL API v2 response shapes
//
// Regression for #31: the v2 handlers wrapped every answer in the session
// API's {data, code, message, success} envelope. SQL API clients read
// resultSetMetaData / data / code at the top level.
// ---------------------------------------------------------------------------

// v2Do sends one request to the SQL API v2 and decodes the JSON body.
func v2Do(t *testing.T, s *Server, method, path, body string) (int, map[string]interface{}) {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	s.httpServer.Handler.ServeHTTP(w, r)
	var got map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return w.Code, got
}

// assertNoSessionEnvelope fails if the body carries the session API's
// {data, code, message, success} wrapper.
func assertNoSessionEnvelope(t *testing.T, got map[string]interface{}) {
	t.Helper()
	if _, ok := got["success"]; ok {
		t.Errorf("body has a success key (session API envelope): %v", got)
	}
	if inner, ok := got["data"].(map[string]interface{}); ok {
		t.Errorf("data is an object (wrapped result), want rows: %v", inner)
	}
}

func TestV2Statements_QueryAnswersAtTopLevel(t *testing.T) {
	s := newTestServer(t, &fakeEngine{
		cols: []string{"N", "S"},
		rows: [][]interface{}{{int64(1), "x"}, {int64(2), nil}},
	})

	status, got := v2Do(t, s, http.MethodPost, "/api/v2/statements", `{"statement":"SELECT n, s FROM t"}`)

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	assertNoSessionEnvelope(t, got)
	for key, want := range map[string]string{"code": "090001", "sqlState": "00000", "message": "Statement executed successfully."} {
		if got[key] != want {
			t.Errorf("%s = %v, want %q", key, got[key], want)
		}
	}
	handle, _ := got["statementHandle"].(string)
	if handle == "" {
		t.Fatalf("statementHandle missing: %v", got)
	}
	if got["statementStatusUrl"] != "/api/v2/statements/"+handle {
		t.Errorf("statementStatusUrl = %v", got["statementStatusUrl"])
	}
	if _, ok := got["createdOn"].(float64); !ok {
		t.Errorf("createdOn missing: %v", got)
	}

	meta, ok := got["resultSetMetaData"].(map[string]interface{})
	if !ok {
		t.Fatalf("resultSetMetaData missing at top level: %v", got)
	}
	if meta["numRows"] != float64(2) || meta["format"] != "jsonv2" {
		t.Errorf("resultSetMetaData = %v", meta)
	}
	rowType, _ := meta["rowType"].([]interface{})
	if len(rowType) != 2 || rowType[0].(map[string]interface{})["name"] != "N" {
		t.Errorf("rowType = %v", rowType)
	}

	data, _ := got["data"].([]interface{})
	if len(data) != 2 {
		t.Fatalf("data = %v, want 2 rows", got["data"])
	}
	second := data[1].([]interface{})
	if second[0] != "2" || second[1] != nil {
		t.Errorf("row 2 = %v, want [\"2\", null] (NULL is JSON null)", second)
	}
}

func TestV2Statements_DMLReportsRowCounts(t *testing.T) {
	tests := []struct {
		sql, column, stat string
	}{
		{"INSERT INTO t VALUES (1), (2)", "number of rows inserted", "numRowsInserted"},
		{"UPDATE t SET a = 1", "number of rows updated", "numRowsUpdated"},
		{"DELETE FROM t", "number of rows deleted", "numRowsDeleted"},
	}
	for _, tt := range tests {
		t.Run(tt.stat, func(t *testing.T) {
			s := newTestServer(t, &fakeEngine{cols: []string{"Count"}, rows: [][]interface{}{{int64(2)}}})

			status, got := v2Do(t, s, http.MethodPost, "/api/v2/statements", `{"statement":"`+tt.sql+`"}`)

			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
			assertNoSessionEnvelope(t, got)
			stats, _ := got["stats"].(map[string]interface{})
			if stats[tt.stat] != float64(2) {
				t.Errorf("stats = %v, want %s: 2", got["stats"], tt.stat)
			}
			rowType := got["resultSetMetaData"].(map[string]interface{})["rowType"].([]interface{})
			if name := rowType[0].(map[string]interface{})["name"]; name != tt.column {
				t.Errorf("column = %v, want %q", name, tt.column)
			}
			if data := got["data"].([]interface{}); data[0].([]interface{})[0] != "2" {
				t.Errorf("data = %v, want [[\"2\"]]", data)
			}
		})
	}
}

func TestV2Statements_ExecNoResultFallbackReportsRowCount(t *testing.T) {
	s := newTestServer(t, &fakeEngine{err: errors.New("no result set"), execNoResultAffected: 3})

	status, got := v2Do(t, s, http.MethodPost, "/api/v2/statements", `{"statement":"DELETE FROM t"}`)

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if stats, _ := got["stats"].(map[string]interface{}); stats["numRowsDeleted"] != float64(3) {
		t.Errorf("stats = %v, want numRowsDeleted: 3", got["stats"])
	}
	if _, ok := got["rowsAffected"]; ok {
		t.Errorf("rowsAffected is not a SQL API field: %v", got)
	}
}

func TestV2Statements_SQLErrorIsQueryFailureStatus(t *testing.T) {
	tests := []struct {
		name, engineErr, code, sqlState, message string
	}{
		{"generic", "Catalog Error: Table with name nope does not exist!", "002043", "", "Catalog Error: Table with name nope does not exist!"},
		{"division by zero", "Out of Range Error: Division by zero", "100051", "22012", "Division by zero"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fail := errors.New(tt.engineErr)
			s := newTestServer(t, &fakeEngine{err: fail, execNoResultErr: fail})

			status, got := v2Do(t, s, http.MethodPost, "/api/v2/statements", `{"statement":"SELECT 1/0"}`)

			if status != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422", status)
			}
			assertNoSessionEnvelope(t, got)
			if got["code"] != tt.code || got["message"] != tt.message {
				t.Errorf("code/message = %v / %v, want %s / %s", got["code"], got["message"], tt.code, tt.message)
			}
			if tt.sqlState != "" && got["sqlState"] != tt.sqlState {
				t.Errorf("sqlState = %v, want %s", got["sqlState"], tt.sqlState)
			}
			handle, _ := got["statementHandle"].(string)
			if handle == "" || got["statementStatusUrl"] != "/api/v2/statements/"+handle {
				t.Errorf("statementHandle/statementStatusUrl = %v / %v", got["statementHandle"], got["statementStatusUrl"])
			}
			if _, ok := got["data"]; ok {
				t.Errorf("error body has data: %v", got)
			}
		})
	}
}

func TestV2Statements_RejectedRequests(t *testing.T) {
	tests := []struct {
		name, method, body string
		status             int
		message            string
	}{
		{"empty statement", http.MethodPost, `{"statement":"  "}`, http.StatusBadRequest, "empty statement"},
		{"bad json", http.MethodPost, `{`, http.StatusBadRequest, "invalid request body"},
		{"wrong method", http.MethodGet, ``, http.StatusMethodNotAllowed, "method not allowed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t, nil)

			status, got := v2Do(t, s, tt.method, "/api/v2/statements", tt.body)

			if status != tt.status {
				t.Fatalf("status = %d, want %d", status, tt.status)
			}
			assertNoSessionEnvelope(t, got)
			if got["message"] != tt.message {
				t.Errorf("message = %v, want %q", got["message"], tt.message)
			}
			if _, ok := got["statementHandle"]; ok {
				t.Errorf("rejected request has a statementHandle: %v", got)
			}
		})
	}
}

func TestV2Statements_BindErrorIsQueryFailureStatus(t *testing.T) {
	s := newTestServer(t, nil)

	status, got := v2Do(t, s, http.MethodPost, "/api/v2/statements",
		`{"statement":"SELECT ?","bindings":{"1":{"type":"FIXED","value":"abc"}}}`)

	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", status)
	}
	assertNoSessionEnvelope(t, got)
	if got["code"] != "000008" {
		t.Errorf("code = %v, want 000008", got["code"])
	}
}

func TestV2StatementsID_PollAndCancelAnswerAtTopLevel(t *testing.T) {
	tests := []struct {
		name, method, path, code string
	}{
		{"poll", http.MethodGet, "/api/v2/statements/abc", "090001"},
		{"cancel", http.MethodPost, "/api/v2/statements/abc/cancel", "000604"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t, nil)

			status, got := v2Do(t, s, tt.method, tt.path, "")

			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
			assertNoSessionEnvelope(t, got)
			if got["code"] != tt.code || got["statementHandle"] != "abc" || got["statementStatusUrl"] != "/api/v2/statements/abc" {
				t.Errorf("body = %v", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// singleCount
// ---------------------------------------------------------------------------

func TestSingleCount(t *testing.T) {
	tests := []struct {
		name   string
		cols   []string
		rows   [][]interface{}
		want   int64
		wantOK bool
	}{
		{"int64", []string{"Count"}, [][]interface{}{{int64(3)}}, 3, true},
		{"int32", []string{"Count"}, [][]interface{}{{int32(4)}}, 4, true},
		{"int", []string{"Count"}, [][]interface{}{{5}}, 5, true},
		{"text", []string{"Count"}, [][]interface{}{{"5"}}, 0, false},
		{"no rows", []string{"Count"}, nil, 0, false},
		{"two columns", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}}, 0, false},
		{"two rows", []string{"a"}, [][]interface{}{{int64(1)}, {int64(2)}}, 0, false},
	}
	for _, tt := range tests {
		got, ok := singleCount(tt.cols, tt.rows)
		if got != tt.want || ok != tt.wantOK {
			t.Errorf("singleCount(%s) = %d, %v, want %d, %v", tt.name, got, ok, tt.want, tt.wantOK)
		}
	}
}
