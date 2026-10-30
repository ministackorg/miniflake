package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miniflakedb/miniflake/internal/orchestrator"
	"github.com/miniflakedb/miniflake/internal/session"
)

// fakeEngine is a minimal QueryEngine for server-level tests. Real DuckDB
// behavior is covered by internal/engine tests; here we just need predictable
// behavior to exercise wire-protocol paths.
type fakeEngine struct {
	rows [][]interface{}
	cols []string
	err  error

	execNoResultAffected int64
	execNoResultErr      error
}

func (f *fakeEngine) Execute(_ context.Context, _ string, _ ...interface{}) ([]string, [][]interface{}, error) {
	return f.cols, f.rows, f.err
}

func (f *fakeEngine) ExecNoResult(_ context.Context, _ string, _ ...interface{}) (int64, error) {
	return f.execNoResultAffected, f.execNoResultErr
}

func newTestServer(t *testing.T, eng *fakeEngine) *Server {
	t.Helper()
	if eng == nil {
		eng = &fakeEngine{}
	}
	return New(eng, session.NewManager(), "127.0.0.1", 0, t.TempDir())
}

// ---------------------------------------------------------------------------
// buildRowSet
// ---------------------------------------------------------------------------

func TestBuildRowSet_TypesAndNulls(t *testing.T) {
	t.Parallel()
	now := time.Unix(1700000000, 123456789).UTC()
	rows := [][]interface{}{
		{nil, int64(42), 3.14, true, false, "hello", []byte{0xde, 0xad}, now},
	}
	got := buildRowSet(rows)
	if len(got) != 1 || len(got[0]) != 8 {
		t.Fatalf("shape: %d rows, row[0] cells=%d", len(got), len(got[0]))
	}
	row := got[0]
	// nil → JSON null (Go nil), not the string "null".
	if row[0] != nil {
		t.Errorf("nil cell: want Go nil, got %#v", row[0])
	}
	// All non-nil cells must be strings per the Snowflake wire format.
	for i := 1; i < len(row); i++ {
		if _, ok := row[i].(string); !ok {
			t.Errorf("cell %d: want string, got %T (%v)", i, row[i], row[i])
		}
	}
	if row[1].(string) != "42" {
		t.Errorf("int: %v", row[1])
	}
	// Snowflake wire format encodes booleans as "1"/"0" (see cellToString).
	if row[3].(string) != "1" || row[4].(string) != "0" {
		t.Errorf("bool: %v / %v", row[3], row[4])
	}
	if !strings.HasPrefix(row[7].(string), "1700000000.") {
		t.Errorf("timestamp: %v", row[7])
	}
}

// ---------------------------------------------------------------------------
// snowflakeTypeName
// ---------------------------------------------------------------------------

func TestSnowflakeTypeName(t *testing.T) {
	t.Parallel()
	cases := map[string]interface{}{
		"fixed":         int64(7),
		"real":          3.0,
		"boolean":       true,
		"timestamp_ntz": time.Now(),
		"binary":        []byte("x"),
		"text":          "s",
	}
	for want, v := range cases {
		if got := snowflakeTypeName(v); got != want {
			t.Errorf("type(%T): got %q want %q", v, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// rowtype column metadata (strict-driver compatibility)
// ---------------------------------------------------------------------------

func TestSnowflakeColumnMetadata(t *testing.T) {
	t.Parallel()
	deref := func(p *int) interface{} {
		if p == nil {
			return nil
		}
		return *p
	}
	for _, tc := range []struct {
		typeName                        string
		length, precision, scale, bytes interface{}
	}{
		{"fixed", nil, 38, 0, nil},
		{"real", nil, nil, nil, nil},
		{"text", 16777216, nil, nil, 16777216},
		{"binary", 8388608, nil, nil, 8388608},
		{"timestamp_ntz", nil, 0, 9, nil},
		{"boolean", nil, nil, nil, nil},
		{"date", nil, nil, nil, nil},
	} {
		l, p, s, b := snowflakeColumnMetadata(tc.typeName)
		if deref(l) != tc.length || deref(p) != tc.precision || deref(s) != tc.scale || deref(b) != tc.bytes {
			t.Errorf("%s: got (len=%v prec=%v scale=%v byte=%v), want (%v %v %v %v)",
				tc.typeName, deref(l), deref(p), deref(s), deref(b),
				tc.length, tc.precision, tc.scale, tc.bytes)
		}
	}
}

// TestQuery_RowTypeCarriesStrictDriverKeys is the regression test for the
// Python-connector query failure: ResultMetadata.from_column reads
// col["length"], col["precision"] and col["scale"] by direct key access, so a
// missing key is a KeyError. Every rowtype entry must carry those keys (null is
// fine), with Snowflake's per-type values. Decoded into a raw map so absence is
// distinguishable from a null value.
func TestQuery_RowTypeCarriesStrictDriverKeys(t *testing.T) {
	t.Parallel()
	eng := &fakeEngine{
		cols: []string{"n", "s", "t"},
		rows: [][]interface{}{{int64(1), "hi", time.Unix(1700000000, 0).UTC()}},
	}
	s := newTestServer(t, eng)

	buf, _ := json.Marshal(queryRequestBody{SQLText: "SELECT n, s, t FROM x"})
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(buf))
	w := httptest.NewRecorder()
	s.handleQueryRequest(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}

	var resp struct {
		Data struct {
			RowType []map[string]json.RawMessage `json:"rowtype"`
		} `json:"data"`
		Success bool `json:"success"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.Success || len(resp.Data.RowType) != 3 {
		t.Fatalf("unexpected response: success=%v cols=%d", resp.Success, len(resp.Data.RowType))
	}

	// Every column must carry the keys the strict driver dereferences.
	for i, col := range resp.Data.RowType {
		for _, key := range []string{"name", "type", "length", "precision", "scale", "byteLength", "nullable"} {
			if _, ok := col[key]; !ok {
				t.Errorf("column %d (%v) missing key %q", i, string(col["name"]), key)
			}
		}
	}
	// Spot-check per-type values match Snowflake's conventions.
	if got := string(resp.Data.RowType[0]["precision"]); got != "38" { // fixed
		t.Errorf("fixed precision = %s, want 38", got)
	}
	if got := string(resp.Data.RowType[0]["scale"]); got != "0" {
		t.Errorf("fixed scale = %s, want 0", got)
	}
	if got := string(resp.Data.RowType[1]["length"]); got == "null" || got == "" { // text
		t.Errorf("text length = %s, want a number", got)
	}
	if got := string(resp.Data.RowType[2]["scale"]); got != "9" { // timestamp_ntz
		t.Errorf("timestamp scale = %s, want 9", got)
	}
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func TestHealth_ReturnsOK(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/_miniflake/health", nil)
	s.handleHealth(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["status"] != "ok" {
		t.Errorf("body: %v", body)
	}
}

func TestReset_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/_miniflake/reset", nil)
	// Go through the real Handler so withStateLock is exercised (GET must
	// not take the exclusive lock before returning 405).
	s.httpServer.Handler.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status: %d", w.Code)
	}
}

func TestReset_GETDoesNotTakeExclusiveLock(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)

	// Hold the exclusive lock as if a POST reset were in flight.
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/_miniflake/reset", nil)
		s.httpServer.Handler.ServeHTTP(w, r)
		done <- w.Code
	}()

	select {
	case code := <-done:
		if code != http.StatusMethodNotAllowed {
			t.Fatalf("status: %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GET /_miniflake/reset blocked on exclusive lock; method check must run before Lock")
	}
}

func TestReset_WithoutOrchestrator(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/_miniflake/reset", nil)
	s.handleReset(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: %d", w.Code)
	}
}

func TestStateLock_HealthStaysUpDuringExclusiveLock(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	// Simulate an in-flight reset holding the exclusive lock.
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/_miniflake/health", nil)
	s.httpServer.Handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("health should stay unlocked during reset, got %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Method validation
// ---------------------------------------------------------------------------

func TestLogin_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/session/v1/login-request", nil)
	s.handleLogin(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status: %d", w.Code)
	}
}

func TestQuery_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/queries/v1/query-request", nil)
	s.handleQueryRequest(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status: %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Login + concurrent SessionID uniqueness
// ---------------------------------------------------------------------------

func loginRequest(t *testing.T, s *Server, user, pass string) (status int, body snowflakeResponse) {
	t.Helper()
	payload := loginRequestBody{}
	payload.Data.AccountName = "miniflake"
	payload.Data.LoginName = user
	payload.Data.Password = pass
	buf, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader(buf))
	s.handleLogin(w, r)
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body
}

func TestLogin_SuccessAndSessionFields(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	status, body := loginRequest(t, s, "test", "test")
	if status != http.StatusOK {
		t.Fatalf("status: %d", status)
	}
	if !body.Success {
		t.Fatalf("not success: %#v", body)
	}
}

func TestLogin_IncludesSessionInfoAndParameters(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)

	payload := loginRequestBody{}
	payload.Data.AccountName = "miniflake"
	payload.Data.LoginName = "test"
	payload.Data.Password = "test"
	payload.Data.DatabaseName = "TESTDB"
	payload.Data.SchemaName = "PUBLIC"
	buf, _ := json.Marshal(payload)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader(buf))
	s.handleLogin(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}

	var resp struct {
		Data    loginResponseData `json:"data"`
		Success bool              `json:"success"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.Success {
		t.Fatalf("not success: %s", w.Body.String())
	}
	if got := resp.Data.SessionInfo.DatabaseName; got != "TESTDB" {
		t.Errorf("sessionInfo.databaseName = %q, want TESTDB", got)
	}
	if got := resp.Data.SessionInfo.SchemaName; got != "PUBLIC" {
		t.Errorf("sessionInfo.schemaName = %q, want PUBLIC", got)
	}
	if len(resp.Data.Parameters) == 0 {
		t.Error("parameters must be a non-empty array")
	}
}

type loginWireParameter struct {
	Name  string          `json:"name"`
	Value json.RawMessage `json:"value"`
}

func loginWireParameters(t *testing.T, s *Server) []loginWireParameter {
	t.Helper()
	buf, err := json.Marshal(loginRequestBody{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader(buf))
	s.handleLogin(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}

	var resp struct {
		Data struct {
			Parameters []loginWireParameter `json:"parameters"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return resp.Data.Parameters
}

// Regression for #25: the JDBC driver unboxes AUTOCOMMIT as a Boolean
func TestLogin_ParametersIssue25ExpectedValues(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)

	got := map[string]string{}
	for _, p := range loginWireParameters(t, s) {
		got[p.Name] = string(p.Value)
	}
	want := map[string]string{
		"AUTOCOMMIT":               `true`,
		"TIMEZONE":                 `"America/Los_Angeles"`,
		"TIMESTAMP_OUTPUT_FORMAT":  `"YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"`,
		"CLIENT_RESULT_CHUNK_SIZE": `160`,
	}
	for name, raw := range want {
		if got[name] != raw {
			t.Errorf("parameter %s = %s, want %s", name, got[name], raw)
		}
	}
}

func TestLogin_ParametersHaveTypedJSONValues(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)

	params := loginWireParameters(t, s)
	if len(params) == 0 {
		t.Fatal("parameters must be a non-empty array")
	}

	integer := regexp.MustCompile(`^-?\d+$`)
	seen := map[string]bool{}
	for _, p := range params {
		if p.Name == "" {
			t.Error("parameter with empty name")
		}
		if seen[p.Name] {
			t.Errorf("duplicate parameter %s", p.Name)
		}
		seen[p.Name] = true

		raw := string(p.Value)
		switch {
		case raw == "true" || raw == "false":
		case integer.MatchString(raw):
		case strings.HasPrefix(raw, `"`):
			var str string
			if err := json.Unmarshal(p.Value, &str); err != nil {
				t.Errorf("parameter %s: invalid JSON string %s: %v", p.Name, raw, err)
				continue
			}
			if str == "true" || str == "false" || integer.MatchString(str) {
				t.Errorf("parameter %s = %s: boolean/number encoded as a JSON string", p.Name, raw)
			}
		default:
			t.Errorf("parameter %s = %s: not a JSON boolean, integer, or string", p.Name, raw)
		}
	}
}

func TestLogin_ParametersMatchOrchestratorDefaults(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)

	params := loginWireParameters(t, s)
	defaults := orchestrator.DefaultSessionParameters()
	if len(params) != len(defaults) {
		t.Fatalf("login returned %d parameters, orchestrator defines %d", len(params), len(defaults))
	}
	for i, d := range defaults {
		if params[i].Name != d.Name {
			t.Errorf("parameter[%d] = %s, want %s", i, params[i].Name, d.Name)
		}
	}
}

func TestLogin_ConcurrentSessionIDsAreUnique(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	const N = 50
	ids := make([]int64, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			payload := loginRequestBody{}
			payload.Data.AccountName = "miniflake"
			payload.Data.LoginName = fmt.Sprintf("u%d", idx)
			payload.Data.Password = "p"
			buf, _ := json.Marshal(payload)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/session/v1/login-request", bytes.NewReader(buf))
			s.handleLogin(w, r)
			var resp snowflakeResponse
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if data, ok := resp.Data.(map[string]interface{}); ok {
				if id, ok := data["sessionId"].(float64); ok {
					ids[idx] = int64(id)
				}
			}
		}(i)
	}
	wg.Wait()
	seen := make(map[int64]bool, N)
	for i, id := range ids {
		if id == 0 {
			t.Errorf("login %d returned no sessionId", i)
			continue
		}
		if seen[id] {
			t.Errorf("duplicate sessionId %d at index %d", id, i)
		}
		seen[id] = true
	}
}

// ---------------------------------------------------------------------------
// Query path: error responses
// ---------------------------------------------------------------------------

func TestQuery_EmptySQLRejected(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	buf, _ := json.Marshal(queryRequestBody{SQLText: ""})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", bytes.NewReader(buf))
	s.handleQueryRequest(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestQuery_InvalidJSONRejected(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", strings.NewReader("not json"))
	s.handleQueryRequest(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status: %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Session lifecycle: heartbeat updates activity, logout deletes
// ---------------------------------------------------------------------------

func TestSession_LogoutDeletes(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	sess := s.sessionMgr.CreateSession("u", "miniflake", "main", "wh", "role")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodDelete, "/session", nil)
	r.Header.Set("Authorization", `Snowflake Token="`+sess.Token+`"`)
	s.handleSession(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}
	if _, ok := s.sessionMgr.GetSession(sess.Token); ok {
		t.Errorf("session still present after logout")
	}
}

func TestSession_HeartbeatUpdatesActivity(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	sess := s.sessionMgr.CreateSession("u", "miniflake", "main", "wh", "role")
	before := sess.LastActiveAt
	time.Sleep(10 * time.Millisecond)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/session/heartbeat", nil)
	r.Header.Set("Authorization", `Snowflake Token="`+sess.Token+`"`)
	s.handleHeartbeat(w, r)
	got, _ := s.sessionMgr.GetSession(sess.Token)
	if !got.LastActiveAt.After(before) {
		t.Errorf("LastActiveAt not updated: %v vs %v", got.LastActiveAt, before)
	}
}

// ---------------------------------------------------------------------------
// gzip request bodies
// ---------------------------------------------------------------------------

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func TestHasGzipEncoding(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		header string
		want   bool
	}{
		{"", false},
		{"gzip", true},
		{"GZIP", true},
		{" gzip ", true},
		{"deflate, gzip", true},
		{"gzip, deflate", true},
		{"deflate", false},
		{"identity", false},
		{"x-gzip", false},
	} {
		if got := hasGzipEncoding(tc.header); got != tc.want {
			t.Errorf("hasGzipEncoding(%q) = %v, want %v", tc.header, got, tc.want)
		}
	}
}

func TestWithDecompressedBody_InflatesGzip(t *testing.T) {
	t.Parallel()
	var got string
	h := withDecompressedBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		got = string(b)
		if enc := r.Header.Get("Content-Encoding"); enc != "" {
			t.Errorf("Content-Encoding still set: %q", enc)
		}
	}))

	r := httptest.NewRequest(http.MethodPost, "/session/v1/login-request",
		bytes.NewReader(gzipBytes(t, []byte(`{"hello":"world"}`))))
	r.Header.Set("Content-Encoding", "gzip")
	h.ServeHTTP(httptest.NewRecorder(), r)

	if got != `{"hello":"world"}` {
		t.Fatalf("body = %q, want the inflated JSON", got)
	}
}

func TestWithDecompressedBody_PassesThroughUncompressed(t *testing.T) {
	t.Parallel()
	var got string
	h := withDecompressedBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
	}))

	r := httptest.NewRequest(http.MethodPost, "/session/v1/login-request",
		strings.NewReader(`{"hello":"world"}`))
	h.ServeHTTP(httptest.NewRecorder(), r)

	if got != `{"hello":"world"}` {
		t.Fatalf("body = %q, want it untouched", got)
	}
}

func TestWithDecompressedBody_MalformedGzipIsBadRequest(t *testing.T) {
	t.Parallel()
	called := false
	h := withDecompressedBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	r := httptest.NewRequest(http.MethodPost, "/session/v1/login-request",
		strings.NewReader("this is not gzip"))
	r.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if called {
		t.Fatal("handler ran on a malformed gzip body")
	}
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

// TestLogin_AcceptsGzippedBody is the regression test for the driver-facing
// bug: snowflake-connector-python gzips every request body, so a gzipped
// login-request must authenticate rather than fail to parse.
func TestLogin_AcceptsGzippedBody(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)

	payload := loginRequestBody{}
	payload.Data.AccountName = "miniflake"
	payload.Data.LoginName = "test"
	payload.Data.Password = "test"
	buf, _ := json.Marshal(payload)

	r := httptest.NewRequest(http.MethodPost, "/session/v1/login-request",
		bytes.NewReader(gzipBytes(t, buf)))
	r.Header.Set("Content-Encoding", "gzip")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Drive the composed handler chain, not the bare handler, so the
	// middleware is exercised the way a real request would be.
	s.httpServer.Handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
	}
	var body snowflakeResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !body.Success {
		t.Fatalf("login failed on a gzipped body: %#v", body)
	}
}

type recordingEngine struct {
	fakeEngine
	mu  sync.Mutex
	sql []string
}

func (r *recordingEngine) Execute(ctx context.Context, q string, a ...interface{}) ([]string, [][]interface{}, error) {
	r.mu.Lock()
	r.sql = append(r.sql, q)
	r.mu.Unlock()
	return r.fakeEngine.Execute(ctx, q, a...)
}

func (r *recordingEngine) ExecNoResult(ctx context.Context, q string, a ...interface{}) (int64, error) {
	r.mu.Lock()
	r.sql = append(r.sql, q)
	r.mu.Unlock()
	return r.fakeEngine.ExecNoResult(ctx, q, a...)
}

func TestQuery_BindingsAreSubstituted(t *testing.T) {
	t.Parallel()
	eng := &recordingEngine{fakeEngine: fakeEngine{cols: []string{"n"}, rows: [][]interface{}{{int64(1)}}}}
	s := New(eng, session.NewManager(), "127.0.0.1", 0, t.TempDir())

	body := `{"sqlText":"SELECT n FROM t WHERE id = ? LIMIT ?","bindings":{"1":{"type":"TEXT","value":"1406218"},"2":{"type":"FIXED","value":"1"}}}`
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleQueryRequest(w, r)

	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"success":true`) {
		t.Fatalf("unexpected response %d: %s", w.Code, w.Body.String())
	}
	want := "SELECT n FROM t WHERE id = '1406218' LIMIT 1"
	if len(eng.sql) != 1 || !strings.Contains(eng.sql[0], want) {
		t.Errorf("engine got %q, want it to contain %q", eng.sql, want)
	}
}

func TestQuery_MissingBindingFails(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)

	body := `{"sqlText":"SELECT ?, ?","bindings":{"1":{"type":"FIXED","value":"1"}}}`
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleQueryRequest(w, r)

	if !strings.Contains(w.Body.String(), `"success":false`) || !strings.Contains(w.Body.String(), "bind variable 2 not set") {
		t.Fatalf("expected bind failure, got %s", w.Body.String())
	}
}

func TestQuery_BulkInsertBindingsFoldToOneStatement(t *testing.T) {
	t.Parallel()
	eng := &recordingEngine{fakeEngine: fakeEngine{execNoResultAffected: 3}}
	s := New(eng, session.NewManager(), "127.0.0.1", 0, t.TempDir())

	body := `{"sqlText":"INSERT INTO t VALUES (?, ?)","bindings":{"1":{"type":"FIXED","value":["1","2","3"]},"2":{"type":"TEXT","value":["a","b",null]}}}`
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleQueryRequest(w, r)

	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"success":true`) {
		t.Fatalf("unexpected response %d: %s", w.Code, w.Body.String())
	}
	want := "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, NULL)"
	if len(eng.sql) != 1 || eng.sql[0] != want {
		t.Errorf("engine got %q, want [%q]", eng.sql, want)
	}
}

func TestQuery_BindStageRejectedWithClearError(t *testing.T) {
	t.Parallel()
	eng := &recordingEngine{}
	s := New(eng, session.NewManager(), "127.0.0.1", 0, t.TempDir())

	body := `{"sqlText":"INSERT INTO t VALUES (?, ?)","bindStage":"@SYSTEM$BIND/abc"}`
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleQueryRequest(w, r)

	if !strings.Contains(w.Body.String(), `"success":false`) || !strings.Contains(w.Body.String(), "bindStage") {
		t.Fatalf("expected bindStage failure, got %s", w.Body.String())
	}
	if len(eng.sql) != 0 {
		t.Errorf("engine must not run the statement, got %q", eng.sql)
	}
}

func TestStatementTypeID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		sql  string
		want int64
	}{
		{"SELECT 1", 0x1000},
		{"  with x as (select 1) select * from x", 0x1000},
		{"insert into t values (1)", 0x3100},
		{"UPDATE t SET a = 1", 0x3200},
		{"DELETE FROM t", 0x3300},
		{"MERGE INTO t USING s ON 1=1", 0x3400},
		{"SHOW TABLES", 0x4400},
		{"DESC TABLE t", 0x4500},
		{"USE DATABASE d", 0x4301},
		{"USE SCHEMA s", 0x4302},
		{"ALTER SESSION SET X=1", 0x4100},
		{"BEGIN", 0x5000},
		{"CREATE TABLE t (a INT)", 0x6000},
		{"DROP TABLE t", 0x6000},
		{"", 0},
		{"PUT file:///x @s", 0},
	}
	for _, tt := range tests {
		if got := statementTypeID(tt.sql); got != tt.want {
			t.Errorf("statementTypeID(%q) = %#x, want %#x", tt.sql, got, tt.want)
		}
	}
}

func queryResponse(t *testing.T, s *Server, body string) map[string]interface{} {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/queries/v1/query-request", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleQueryRequest(w, r)
	var resp struct {
		Data    map[string]interface{} `json:"data"`
		Success bool                   `json:"success"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || !resp.Success {
		t.Fatalf("unexpected response (err=%v): %s", err, w.Body.String())
	}
	return resp.Data
}

func TestQuery_DescribeOnlyDoesNotExecuteDML(t *testing.T) {
	t.Parallel()
	eng := &recordingEngine{}
	s := New(eng, session.NewManager(), "127.0.0.1", 0, t.TempDir())

	data := queryResponse(t, s, `{"sqlText":"INSERT INTO t VALUES (?, ?, ?)","describeOnly":true}`)

	if len(eng.sql) != 0 {
		t.Fatalf("describe-only must not execute DML, engine got %q", eng.sql)
	}
	if got := data["statementTypeId"]; got != float64(0x3100) {
		t.Errorf("statementTypeId = %v, want %d", got, 0x3100)
	}
	if data["arrayBindSupported"] != true {
		t.Errorf("arrayBindSupported = %v, want true for INSERT", data["arrayBindSupported"])
	}
}

func TestQuery_DescribeOnlyQueryFillsPlaceholdersAndReturnsNoRows(t *testing.T) {
	t.Parallel()
	eng := &recordingEngine{fakeEngine: fakeEngine{cols: []string{"n"}, rows: [][]interface{}{{int64(1)}}}}
	s := New(eng, session.NewManager(), "127.0.0.1", 0, t.TempDir())

	data := queryResponse(t, s, `{"sqlText":"SELECT n FROM t WHERE id = ? LIMIT ?","describeOnly":true}`)

	if want := "SELECT n FROM t WHERE id = NULL LIMIT NULL"; len(eng.sql) != 1 || eng.sql[0] != want {
		t.Errorf("engine got %q, want [%q]", eng.sql, want)
	}
	if rows, _ := data["rowset"].([]interface{}); len(rows) != 0 {
		t.Errorf("describe-only must return no rows, got %v", data["rowset"])
	}
	if cols, _ := data["rowtype"].([]interface{}); len(cols) != 1 {
		t.Errorf("rowtype = %v, want 1 column", data["rowtype"])
	}
}
