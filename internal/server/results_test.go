package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/miniflakedb/miniflake/internal/orchestrator"
)

// snowflake-sdk's query ID check (dist/lib/connection/connection.js).
var reNodeQueryID = regexp.MustCompile(`(?i)^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|00000000-0000-0000-0000-000000000000)$`)

func TestGenerateQueryID_IsUUIDShaped(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := generateQueryID()
		if !reNodeQueryID.MatchString(id) || id != strings.ToLower(id) {
			t.Fatalf("query ID %q is not lowercase 8-4-4-4-12 hex", id)
		}
		if seen[id] {
			t.Fatalf("duplicate query ID %q", id)
		}
		seen[id] = true
	}
}

func postQuery(t *testing.T, s *Server, sql string) map[string]interface{} {
	t.Helper()
	buf, _ := json.Marshal(queryRequestBody{SQLText: sql})
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(buf))
	w := httptest.NewRecorder()
	s.httpServer.Handler.ServeHTTP(w, r)
	var got map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, w.Body.String())
	}
	return got
}

func get(t *testing.T, s *Server, path string) (int, map[string]interface{}) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	s.httpServer.Handler.ServeHTTP(w, r)
	var got map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return w.Code, got
}

func TestQueryResult_ReturnsStoredSuccess(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, &fakeEngine{cols: []string{"N"}, rows: [][]interface{}{{int64(42)}}})

	first := postQuery(t, s, "SELECT 42 AS N")
	qid := first["data"].(map[string]interface{})["queryId"].(string)

	status, got := get(t, s, "/queries/"+qid+"/result")
	if status != http.StatusOK || got["success"] != true {
		t.Fatalf("result fetch: status %d body %v", status, got)
	}
	data := got["data"].(map[string]interface{})
	if data["queryId"] != qid || data["queryStatus"] != "SUCCESS" {
		t.Errorf("queryId/queryStatus = %v / %v", data["queryId"], data["queryStatus"])
	}
	if rs, _ := json.Marshal(data["rowset"]); string(rs) != `[["42"]]` {
		t.Errorf("rowset = %s, want [[\"42\"]]", rs)
	}
	if len(data["rowtype"].([]interface{})) != 1 {
		t.Errorf("rowtype = %v", data["rowtype"])
	}

	// Monitoring reports the same query in data.queries[0] (gosnowflake and
	// snowflake-sdk read that shape).
	_, mon := get(t, s, "/monitoring/queries/"+qid)
	queries := mon["data"].(map[string]interface{})["queries"].([]interface{})
	q := queries[0].(map[string]interface{})
	if q["id"] != qid || q["status"] != "SUCCESS" || q["sqlText"] != "SELECT 42 AS N" {
		t.Errorf("monitoring entry = %v", q)
	}
}

func TestQueryResult_ReturnsStoredFailure(t *testing.T) {
	t.Parallel()
	fail := errors.New("Catalog Error: Table with name NOPE does not exist!")
	s := newTestServer(t, &fakeEngine{err: fail})

	first := postQuery(t, s, "SELECT * FROM NOPE")
	if first["success"] != false {
		t.Fatalf("expected failure, got %v", first)
	}
	data := first["data"].(map[string]interface{})
	qid := data["queryId"].(string)
	if data["sqlState"] != "42S02" || first["code"] != "002003" {
		t.Errorf("code/sqlState = %v / %v, want 002003 / 42S02", first["code"], data["sqlState"])
	}

	_, got := get(t, s, "/queries/"+qid+"/result")
	if got["success"] != false || got["code"] != "002003" || got["message"] != fail.Error() {
		t.Errorf("stored failure = %v", got)
	}

	_, mon := get(t, s, "/monitoring/queries/"+qid)
	q := mon["data"].(map[string]interface{})["queries"].([]interface{})[0].(map[string]interface{})
	if q["status"] != "FAILED_WITH_ERROR" || q["errorCode"] != "002003" {
		t.Errorf("monitoring entry = %v", q)
	}
}

func TestQueryResult_UnknownID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	status, got := get(t, s, "/queries/01b2c3d4-0000-1234-0000-00012345abcd/result")
	if status != http.StatusOK || got["success"] != false || got["code"] != "000709" {
		t.Errorf("unknown ID: status %d body %v", status, got)
	}
	if status, _ := get(t, s, "/queries/x/y/z"); status != http.StatusNotFound {
		t.Errorf("bad path: status %d, want 404", status)
	}
}

func TestResultStore_EvictsOldest(t *testing.T) {
	t.Parallel()
	rs := newResultStore(2)
	for _, id := range []string{"a", "b", "c"} {
		rs.put(id, snowflakeResponse{Data: queryResponseData{QueryID: id}, Success: true})
	}
	if _, ok := rs.get("a"); ok {
		t.Error("oldest entry was not evicted")
	}
	for _, id := range []string{"b", "c"} {
		if _, ok := rs.get(id); !ok {
			t.Errorf("entry %s missing", id)
		}
	}
	rs.reset()
	if _, ok := rs.get("c"); ok {
		t.Error("reset kept entries")
	}
}

func TestMapEngineError_EveryFailureHasSQLState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err            error
		code, sqlState string
	}{
		{errors.New("Parser Error: syntax error at or near \"SELEC\""), "001003", "42000"},
		{errors.New("Catalog Error: Table with name t does not exist!"), "002003", "42S02"},
		{errors.New("Catalog Error: Scalar Function with name f does not exist!"), "002140", "42601"},
		{errors.New("Binder Error: Referenced column \"x\" not found in FROM clause!"), "000904", "42000"},
		{errors.New("Conversion Error: Could not convert string 'a' to INT32"), "100038", "22018"},
		{errors.New("Out of Range Error: Division by zero"), "100051", "22012"},
		{errors.New("Invalid Input Error: anything else"), "002043", "42000"},
		{&orchestrator.SnowflakeError{Code: "002043", SQLState: "02000", Message: "Object does not exist, or operation cannot be performed."}, "002043", "02000"},
	}
	for _, tt := range tests {
		code, _, sqlState := mapEngineError(tt.err)
		if code != tt.code || sqlState != tt.sqlState {
			t.Errorf("%v: code/sqlState = %s / %s, want %s / %s", tt.err, code, sqlState, tt.code, tt.sqlState)
		}
	}
}

func TestWireColumnTypes_DeclaredTemporalTypes(t *testing.T) {
	t.Parallel()
	tm := time.Date(1, 1, 1, 12, 34, 56, 500000000, time.UTC)
	d := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	old := time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)
	rows := [][]interface{}{{tm, d, d, d, old, nil}}
	types := wireColumnTypes(
		[]string{"t", "d", "ts", "tstz", "old", "n"},
		[]string{"TIME", "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "DATE", "TIME"},
		rows)
	want := []string{"time", "date", "timestamp_ntz", "timestamp_ltz", "date", "time"}
	for i := range want {
		if types[i] != want[i] {
			t.Errorf("column %d: type %s, want %s", i, types[i], want[i])
		}
	}
	got := buildRowSetTyped(rows, types)[0]
	for i, w := range []interface{}{"45296.500000000", "20455", "1767312000.000000000", "1767312000.000000000", "-1", nil} {
		if got[i] != w {
			t.Errorf("cell %d = %#v, want %#v", i, got[i], w)
		}
	}
}
