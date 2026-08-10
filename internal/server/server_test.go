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
	"strings"
	"sync"
	"testing"
	"time"

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
	if row[3].(string) != "true" || row[4].(string) != "false" {
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
	if resp.Data.Parameters == nil {
		t.Error("parameters must be a non-nil array, got null")
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
