package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miniflakedb/miniflake/internal/orchestrator"
	"github.com/miniflakedb/miniflake/internal/session"
)

// sessionIDCounter avoids collisions on concurrent logins. Snowflake clients
// dedupe sessions by SessionID; a time.UnixMilli()%100k value (the prior
// derivation) collides under load.
var sessionIDCounter uint64

// QueryEngine is the interface the server uses to execute SQL. It matches the
// signatures of internal/engine.Engine so that package never needs to be
// imported here.
type QueryEngine interface {
	Execute(ctx context.Context, sql string, args ...interface{}) ([]string, [][]interface{}, error)
	ExecNoResult(ctx context.Context, sql string, args ...interface{}) (int64, error)
}

// Server is the HTTP front-end that speaks the Snowflake wire protocol.
type Server struct {
	engine       QueryEngine
	orchestrator *orchestrator.Orchestrator
	sessionMgr   *session.Manager
	host         string
	port         int
	stageDir     string

	httpServer *http.Server
	tlsConfig  *tls.Config
	certPath   string

	// stateMu gates access to shared MiniFlake state the same way ministack
	// serializes /_ministack/reset: exclusive Lock for reset, RLock for every
	// other request so a wipe never overlaps with a query.
	stateMu sync.RWMutex
}

// New creates a Server. Call ListenAndServe to start it.
// If orch is non-nil, query handling goes through the orchestrator.
func New(engine QueryEngine, sessionMgr *session.Manager, host string, port int, stageDir string, orch ...*orchestrator.Orchestrator) *Server {
	s := &Server{
		engine:     engine,
		sessionMgr: sessionMgr,
		host:       host,
		port:       port,
		stageDir:   stageDir,
	}
	if len(orch) > 0 && orch[0] != nil {
		s.orchestrator = orch[0]
	}

	mux := http.NewServeMux()

	// Snowflake wire-protocol routes.
	mux.HandleFunc("/session/v1/login-request", s.handleLogin)
	mux.HandleFunc("/session/token-request", s.handleTokenRefresh)
	mux.HandleFunc("/session/authenticator-request", s.handleAuthenticator)
	mux.HandleFunc("/session", s.handleSession) // DELETE = logout
	mux.HandleFunc("/queries/v1/query-request", s.handleQueryRequest)
	mux.HandleFunc("/queries/v1/abort-request", s.handleAbortRequest)
	mux.HandleFunc("/monitoring/queries/", s.handleQueryStatus)

	// SQL API v2.
	mux.HandleFunc("/api/v2/statements", s.handleV2Statements)
	mux.HandleFunc("/api/v2/statements/", s.handleV2StatementsID)

	// Telemetry (stub — gosnowflake sends telemetry data here).
	mux.HandleFunc("/telemetry/send", s.handleTelemetry)
	mux.HandleFunc("/session/heartbeat", s.handleHeartbeat)

	// Snowpipe REST ingest.
	mux.HandleFunc("/v1/data/pipes/", s.handleSnowpipeIngest)

	// Internal.
	mux.HandleFunc("/_miniflake/health", s.handleHealth)
	mux.HandleFunc("/_miniflake/reset", s.handleReset)

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, port),
		Handler: s.withStateLock(mux),
	}

	return s
}

// withStateLock wraps the serve mux so POST /_miniflake/reset takes an
// exclusive lock while every other request (except health) takes a shared
// one. Health stays unlocked so liveness checks still work during a wipe.
// Non-POST methods on /_miniflake/reset do not take the exclusive lock
// (they only need to return 405).
func (s *Server) withStateLock(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_miniflake/health":
			next.ServeHTTP(w, r)
			return
		case "/_miniflake/reset":
			if r.Method != http.MethodPost {
				next.ServeHTTP(w, r)
				return
			}
			s.stateMu.Lock()
			defer s.stateMu.Unlock()
			next.ServeHTTP(w, r)
			return
		default:
			s.stateMu.RLock()
			defer s.stateMu.RUnlock()
			next.ServeHTTP(w, r)
		}
	})
}

// ListenAndServe starts the server (blocking). When TLS is enabled (see
// EnableTLS) it accepts HTTPS and plain HTTP on the same port; otherwise it
// serves plain HTTP only.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	if s.tlsConfig != nil {
		ln = &autoListener{Listener: ln, tlsConfig: s.tlsConfig}
	}
	return s.httpServer.Serve(ln)
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

type snowflakeResponse struct {
	Data    interface{} `json:"data"`
	Code    *string     `json:"code"`
	Message *string     `json:"message"`
	Success bool        `json:"success"`
}

type loginRequestBody struct {
	Data struct {
		LoginName     string `json:"LOGIN_NAME"`
		Password      string `json:"PASSWORD"`
		AccountName   string `json:"ACCOUNT_NAME"`
		DatabaseName  string `json:"DATABASE_NAME"`
		SchemaName    string `json:"SCHEMA_NAME"`
		WarehouseName string `json:"WAREHOUSE_NAME"`
	} `json:"data"`
}

type loginResponseData struct {
	Token               string               `json:"token"`
	MasterToken         string               `json:"masterToken"`
	SessionID           int64                `json:"sessionId"`
	MasterValidityInSec int                  `json:"masterValidityInSeconds"`
	DisplayUserName     string               `json:"displayUserName"`
	ServerVersion       string               `json:"serverVersion"`
	SessionInfo         loginSessionInfo     `json:"sessionInfo"`
	Parameters          []nameValueParameter `json:"parameters"`
}

type queryRequestBody struct {
	SQLText             string `json:"sqlText"`
	AsyncExec           bool   `json:"asyncExec"`
	SequenceID          int    `json:"sequenceId"`
	QuerySubmissionTime int64  `json:"querySubmissionTime"`
}

type rowTypeField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type queryResponseData struct {
	QueryID           string          `json:"queryId"`
	SQLText           string          `json:"sqlText"`
	QueryResultFormat string          `json:"queryResultFormat"`
	RowType           []rowTypeField  `json:"rowtype"`
	RowSet            [][]interface{} `json:"rowset"`
	Total             int             `json:"total"`
	Returned          int             `json:"returned"`
	QueryStatus       string          `json:"queryStatus"`
}

type loginSessionInfo struct {
	DatabaseName  string `json:"databaseName"`
	SchemaName    string `json:"schemaName"`
	WarehouseName string `json:"warehouseName"`
	RoleName      string `json:"roleName"`
}

type nameValueParameter struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errorResponse(w http.ResponseWriter, httpStatus int, code, message string) {
	writeJSON(w, httpStatus, snowflakeResponse{
		Data:    nil,
		Code:    &code,
		Message: &message,
		Success: false,
	})
}

func successResponse(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusOK, snowflakeResponse{
		Data:    data,
		Code:    nil,
		Message: nil,
		Success: true,
	})
}

// extractToken pulls the session token from the Authorization header.
// Snowflake uses: Authorization: Snowflake Token="<token>"
func extractToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	// Format: Snowflake Token="xxx"
	const prefix = "Snowflake Token=\""
	idx := strings.Index(auth, prefix)
	if idx < 0 {
		return ""
	}
	rest := auth[idx+len(prefix):]
	end := strings.Index(rest, "\"")
	if end < 0 {
		return rest
	}
	return rest[:end]
}

// generateQueryID creates a unique query identifier.
func generateQueryID() string {
	// Reuse the session package's UUID format via crypto/rand.
	return fmt.Sprintf("%016x-%04x-%04x",
		time.Now().UnixNano(),
		time.Now().Nanosecond()&0xffff,
		time.Now().UnixMicro()&0xffff)
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

// snowflakeTypeName maps a Go/DuckDB value to the Snowflake type string.
func snowflakeTypeName(v interface{}) string {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "fixed"
	case float32, float64:
		return "real"
	case bool:
		return "boolean"
	case time.Time:
		return "timestamp_ntz"
	case []byte:
		return "binary"
	default:
		return "text"
	}
}

// cellToString converts a value from the engine into a string for the rowset.
// Snowflake wire format sends dates as epoch days and timestamps as epoch
// seconds with nanosecond fractions. The gosnowflake driver expects this.
func cellToString(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch val := v.(type) {
	case time.Time:
		// Return as epoch seconds with nanosecond precision.
		// The gosnowflake driver parses this based on the type field.
		epoch := val.Unix()
		nanos := val.Nanosecond()
		if nanos == 0 {
			return fmt.Sprintf("%d.000000000", epoch)
		}
		return fmt.Sprintf("%d.%09d", epoch, nanos)
	case []byte:
		return fmt.Sprintf("%x", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// buildRowSet converts engine rows into the Snowflake-wire-format rowset.
// Per the Snowflake HTTP JSON protocol, every non-null cell is emitted as a
// string (cellToString does the per-type formatting); nulls flow through as
// JSON null so the gosnowflake driver hits its IsNull path correctly.
func buildRowSet(rows [][]interface{}) [][]interface{} {
	out := make([][]interface{}, len(rows))
	for i, row := range rows {
		outRow := make([]interface{}, len(row))
		for j, cell := range row {
			if cell == nil {
				outRow[j] = nil
				continue
			}
			outRow[j] = cellToString(cell)
		}
		out[i] = outRow
	}
	return out
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}

	var body loginRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		errorResponse(w, http.StatusBadRequest, "400", "invalid request body")
		return
	}

	// gosnowflake passes the connection-level database/schema/warehouse as
	// URL query parameters (?databaseName=...&schemaName=...&warehouse=...)
	// instead of body fields. The body-form is honored too in case any
	// SDK does send them that way — body takes precedence over query.
	q := r.URL.Query()
	database := body.Data.DatabaseName
	if database == "" {
		database = q.Get("databaseName")
	}
	schema := body.Data.SchemaName
	if schema == "" {
		schema = q.Get("schemaName")
	}
	warehouse := body.Data.WarehouseName
	if warehouse == "" {
		warehouse = q.Get("warehouse")
	}

	sess := s.sessionMgr.CreateSession(
		body.Data.LoginName,
		database,
		schema,
		warehouse,
		"SYSADMIN", // default role
	)

	successResponse(w, loginResponseData{
		Token:               sess.Token,
		MasterToken:         sess.ID, // use session ID as master token
		SessionID:           int64(atomic.AddUint64(&sessionIDCounter, 1)),
		MasterValidityInSec: 14400,
		DisplayUserName:     strings.ToUpper(body.Data.LoginName),
		ServerVersion:       "8.0.0",
		SessionInfo: loginSessionInfo{
			DatabaseName:  database,
			SchemaName:    schema,
			WarehouseName: warehouse,
			RoleName:      "SYSADMIN",
		},
		Parameters: []nameValueParameter{},
	})
}

func (s *Server) handleTokenRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}

	token := extractToken(r)
	sess, ok := s.sessionMgr.GetSession(token)
	if !ok {
		errorResponse(w, http.StatusUnauthorized, "390100", "session token is invalid")
		return
	}
	s.sessionMgr.UpdateActivity(token)

	successResponse(w, loginResponseData{
		Token:               sess.Token,
		MasterToken:         sess.ID,
		SessionID:           int64(atomic.AddUint64(&sessionIDCounter, 1)),
		MasterValidityInSec: 14400,
		DisplayUserName:     strings.ToUpper(sess.User),
	})
}

func (s *Server) handleAuthenticator(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}
	// Stub: always succeed.
	successResponse(w, map[string]interface{}{
		"tokenUrl": "",
		"ssoUrl":   "",
		"proofKey": "",
	})
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	// gosnowflake sends DELETE /session?delete=true&requestId=...
	// Accept both DELETE method and ?delete=true query param.
	if r.Method != http.MethodDelete && r.URL.Query().Get("delete") != "true" {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}

	token := extractToken(r)
	if token != "" {
		s.sessionMgr.DeleteSession(token)
	}
	successResponse(w, nil)
}

func (s *Server) handleQueryRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}

	token := extractToken(r)
	if token != "" {
		s.sessionMgr.UpdateActivity(token)
	}

	var body queryRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		errorResponse(w, http.StatusBadRequest, "400", "invalid request body")
		return
	}

	sqlText := strings.TrimSpace(body.SQLText)
	if sqlText == "" {
		errorResponse(w, http.StatusBadRequest, "400", "empty SQL text")
		return
	}

	queryID := generateQueryID()
	ctx := r.Context()

	// If orchestrator is wired, route through it.
	if s.orchestrator != nil {
		// A non-empty token that doesn't resolve = invalid/expired session.
		// Reject rather than silently falling through to a temp session
		// (which would let an attacker query without re-authenticating).
		// An empty token is allowed and creates a temp session below.
		sess, ok := s.sessionMgr.GetSession(token)
		if !ok && token != "" {
			errorResponse(w, http.StatusUnauthorized, "390100", "session token is invalid")
			return
		}
		if sess == nil {
			// Create a temporary session for unauthenticated queries.
			sess = &session.Session{
				Database:  "miniflake",
				Schema:    "main",
				Warehouse: "COMPUTE_WH",
				Role:      "SYSADMIN",
			}
		}
		result, err := s.orchestrator.ExecuteSQL(ctx, sess, sqlText)
		if err != nil {
			code := "002043"
			msg := err.Error()
			writeJSON(w, http.StatusOK, snowflakeResponse{
				Data:    map[string]interface{}{"queryId": queryID, "queryStatus": "FAILED_WITH_ERROR", "sqlText": sqlText},
				Code:    &code,
				Message: &msg,
				Success: false,
			})
			return
		}

		rowType := make([]rowTypeField, len(result.Columns))
		for i, col := range result.Columns {
			typeName := "text"
			if len(result.Rows) > 0 && i < len(result.Rows[0]) {
				typeName = snowflakeTypeName(result.Rows[0][i])
			}
			rowType[i] = rowTypeField{
				Name:     col,
				Type:     typeName,
				Nullable: true,
			}
		}

		rowSet := buildRowSet(result.Rows)

		code := "090000"
		writeJSON(w, http.StatusOK, snowflakeResponse{
			Data: queryResponseData{
				QueryID:           queryID,
				SQLText:           sqlText,
				QueryResultFormat: "json",
				RowType:           rowType,
				RowSet:            rowSet,
				Total:             len(rowSet),
				Returned:          len(rowSet),
				QueryStatus:       "SUCCESS",
			},
			Code:    &code,
			Message: nil,
			Success: true,
		})
		return
	}

	// Fallback: direct engine execution (no orchestrator).
	upper := strings.ToUpper(sqlText)
	isQuery := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "DESCRIBE") ||
		strings.HasPrefix(upper, "DESC") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "LIST") ||
		strings.HasPrefix(upper, "WITH")

	if isQuery {
		cols, rows, err := s.engine.Execute(ctx, sqlText)
		if err != nil {
			code := "002043"
			msg := err.Error()
			writeJSON(w, http.StatusOK, snowflakeResponse{
				Data:    map[string]interface{}{"queryId": queryID, "queryStatus": "FAILED_WITH_ERROR", "sqlText": sqlText},
				Code:    &code,
				Message: &msg,
				Success: false,
			})
			return
		}

		rowType := make([]rowTypeField, len(cols))
		for i, col := range cols {
			typeName := "text"
			if len(rows) > 0 && i < len(rows[0]) {
				typeName = snowflakeTypeName(rows[0][i])
			}
			rowType[i] = rowTypeField{
				Name:     col,
				Type:     typeName,
				Nullable: true,
			}
		}

		rowSet := buildRowSet(rows)

		code := "090000"
		writeJSON(w, http.StatusOK, snowflakeResponse{
			Data: queryResponseData{
				QueryID:           queryID,
				SQLText:           sqlText,
				QueryResultFormat: "json",
				RowType:           rowType,
				RowSet:            rowSet,
				Total:             len(rowSet),
				Returned:          len(rowSet),
				QueryStatus:       "SUCCESS",
			},
			Code:    &code,
			Message: nil,
			Success: true,
		})
	} else {
		affected, err := s.engine.ExecNoResult(ctx, sqlText)
		if err != nil {
			code := "002043"
			msg := err.Error()
			writeJSON(w, http.StatusOK, snowflakeResponse{
				Data:    map[string]interface{}{"queryId": queryID, "queryStatus": "FAILED_WITH_ERROR", "sqlText": sqlText},
				Code:    &code,
				Message: &msg,
				Success: false,
			})
			return
		}

		code := "090000"
		writeJSON(w, http.StatusOK, snowflakeResponse{
			Data: queryResponseData{
				QueryID:           queryID,
				SQLText:           sqlText,
				QueryResultFormat: "json",
				RowType:           []rowTypeField{{Name: "rows_affected", Type: "fixed", Nullable: false}},
				RowSet:            [][]interface{}{{fmt.Sprintf("%d", affected)}},
				Total:             1,
				Returned:          1,
				QueryStatus:       "SUCCESS",
			},
			Code:    &code,
			Message: nil,
			Success: true,
		})
	}
}

func (s *Server) handleAbortRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}
	// Stub: queries run synchronously, so there's nothing to abort.
	successResponse(w, map[string]interface{}{})
}

func (s *Server) handleQueryStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}

	// Extract query ID from path: /monitoring/queries/{queryId}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/monitoring/queries/"), "/")
	queryID := parts[0]

	// All queries are synchronous, so any queried ID is either done or unknown.
	successResponse(w, map[string]interface{}{
		"queryId":     queryID,
		"queryStatus": "SUCCESS",
	})
}

// ---------------------------------------------------------------------------
// SQL API v2
// ---------------------------------------------------------------------------

func (s *Server) handleV2Statements(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}

	token := extractToken(r)
	if token != "" {
		s.sessionMgr.UpdateActivity(token)
	}

	var body struct {
		Statement string `json:"statement"`
		Timeout   int    `json:"timeout"`
		Database  string `json:"database"`
		Schema    string `json:"schema"`
		Warehouse string `json:"warehouse"`
		Role      string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		errorResponse(w, http.StatusBadRequest, "400", "invalid request body")
		return
	}

	sqlText := strings.TrimSpace(body.Statement)
	if sqlText == "" {
		errorResponse(w, http.StatusBadRequest, "400", "empty statement")
		return
	}

	queryID := generateQueryID()
	ctx := r.Context()

	cols, rows, err := s.engine.Execute(ctx, sqlText)
	if err != nil {
		// Try as DML.
		affected, err2 := s.engine.ExecNoResult(ctx, sqlText)
		if err2 != nil {
			errorResponse(w, http.StatusUnprocessableEntity, "002043", err.Error())
			return
		}
		successResponse(w, map[string]interface{}{
			"statementHandle":   queryID,
			"status":            "SUCCESS",
			"rowsAffected":      affected,
			"resultSetMetaData": map[string]interface{}{"numRows": 0, "format": "jsonv2", "rowType": []interface{}{}},
			"data":              [][]string{},
		})
		return
	}

	rowType := make([]map[string]interface{}, len(cols))
	for i, col := range cols {
		typeName := "text"
		if len(rows) > 0 && i < len(rows[0]) {
			typeName = snowflakeTypeName(rows[0][i])
		}
		rowType[i] = map[string]interface{}{
			"name":     col,
			"type":     typeName,
			"nullable": true,
		}
	}

	rowSet := make([][]string, len(rows))
	for i, row := range rows {
		rowSet[i] = make([]string, len(row))
		for j, cell := range row {
			rowSet[i][j] = cellToString(cell)
		}
	}

	successResponse(w, map[string]interface{}{
		"statementHandle": queryID,
		"status":          "SUCCESS",
		"resultSetMetaData": map[string]interface{}{
			"numRows": len(rowSet),
			"format":  "jsonv2",
			"rowType": rowType,
		},
		"data": rowSet,
	})
}

func (s *Server) handleV2StatementsID(w http.ResponseWriter, r *http.Request) {
	// Path: /api/v2/statements/{handle} or /api/v2/statements/{handle}/cancel
	path := strings.TrimPrefix(r.URL.Path, "/api/v2/statements/")
	parts := strings.Split(path, "/")
	handle := parts[0]

	if len(parts) >= 2 && parts[1] == "cancel" {
		// Cancel endpoint.
		successResponse(w, map[string]interface{}{
			"statementHandle": handle,
			"status":          "ABORTED",
		})
		return
	}

	if r.Method != http.MethodGet {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}

	// Poll/fetch — all queries are synchronous, return done.
	successResponse(w, map[string]interface{}{
		"statementHandle": handle,
		"status":          "SUCCESS",
	})
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	// Accept and discard telemetry data from the gosnowflake driver.
	successResponse(w, map[string]interface{}{})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	token := extractToken(r)
	if token != "" {
		s.sessionMgr.UpdateActivity(token)
	}
	successResponse(w, map[string]interface{}{})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReset implements POST /_miniflake/reset. It wipes DuckDB user objects
// and in-process subsystem state so CI can isolate runs without restarting
// the process. GET and other methods return 405. The exclusive state lock is
// taken by withStateLock before this handler runs.
func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}
	if s.orchestrator == nil {
		errorResponse(w, http.StatusServiceUnavailable, "503", "orchestrator not configured")
		return
	}

	if err := s.orchestrator.Reset(r.Context()); err != nil {
		errorResponse(w, http.StatusInternalServerError, "500", err.Error())
		return
	}
	if s.sessionMgr != nil {
		s.sessionMgr.Reset()
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleSnowpipeIngest implements the Snowpipe REST ingest endpoint:
//
//	POST /v1/data/pipes/{db}/{schema}/{pipe}/insertFiles
//	Body: {"files": [{"path": "path/in/stage"}, ...]}
//
// Real Snowflake returns a request id and the file load status. We hand the
// file list off to the snowpipe engine via the orchestrator and return the
// same shape — the orchestrator is required (no fallback) because pipe state
// lives in the snowpipe.Engine which is only constructed via orchestrator.
func (s *Server) handleSnowpipeIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "405", "method not allowed")
		return
	}
	if s.orchestrator == nil {
		errorResponse(w, http.StatusServiceUnavailable, "503", "orchestrator not configured")
		return
	}
	// Path: /v1/data/pipes/{db}/{schema}/{pipe}/insertFiles
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/data/pipes/"), "/")
	if len(parts) != 4 || parts[3] != "insertFiles" {
		errorResponse(w, http.StatusNotFound, "404", "unknown pipe endpoint")
		return
	}
	db, schema, pipe := parts[0], parts[1], parts[2]

	var body struct {
		Files []struct {
			Path string `json:"path"`
			Size int64  `json:"size"`
		} `json:"files"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		errorResponse(w, http.StatusBadRequest, "400", "invalid request body")
		return
	}
	if len(body.Files) == 0 {
		errorResponse(w, http.StatusBadRequest, "400", "files list is required")
		return
	}
	files := make([]string, 0, len(body.Files))
	for _, f := range body.Files {
		files = append(files, f.Path)
	}
	if err := s.orchestrator.InsertPipeFiles(r.Context(), db, schema, pipe, files); err != nil {
		errorResponse(w, http.StatusBadRequest, "002043", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"requestId":  generateQueryID(),
		"pipe":       fmt.Sprintf("%s.%s.%s", db, schema, pipe),
		"statusCode": 200,
		"message":    "SUCCESS",
	})
}
