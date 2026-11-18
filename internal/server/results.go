package server

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// maxStoredResults bounds how many finished query answers are kept for
// GET /queries/{id}/result and /monitoring/queries/{id}. Older ones are
// evicted first. Snowflake keeps results for 24 hours; an emulator only
// needs to cover the "submit now, fetch shortly after" pattern of async
// clients.
const maxStoredResults = 1000

type storedResult struct {
	resp      snowflakeResponse
	sqlText   string
	startTime int64
	endTime   int64
}

// queryStatusEntry is one element of data.queries in a monitoring response,
// the shape gosnowflake and snowflake-sdk decode.
type queryStatusEntry struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	SQLText      string `json:"sqlText"`
	StartTime    int64  `json:"startTime"`
	EndTime      int64  `json:"endTime"`
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

func (r storedResult) status() queryStatusEntry {
	e := queryStatusEntry{
		SQLText:   r.sqlText,
		StartTime: r.startTime,
		EndTime:   r.endTime,
		Status:    "SUCCESS",
	}
	switch d := r.resp.Data.(type) {
	case queryResponseData:
		e.ID = d.QueryID
	case map[string]interface{}:
		e.ID, _ = d["queryId"].(string)
	}
	if !r.resp.Success {
		e.Status = "FAILED_WITH_ERROR"
		if r.resp.Code != nil {
			e.ErrorCode = *r.resp.Code
		}
		if r.resp.Message != nil {
			e.ErrorMessage = *r.resp.Message
		}
	}
	return e
}

// resultStore is a bounded, insertion-ordered map of query ID to answer.
type resultStore struct {
	mu    sync.Mutex
	max   int
	order []string
	byID  map[string]storedResult
}

func newResultStore(max int) *resultStore {
	return &resultStore{max: max, byID: make(map[string]storedResult)}
}

func (s *resultStore) put(queryID string, resp snowflakeResponse) {
	if s == nil || queryID == "" {
		return
	}
	now := time.Now().UnixMilli()
	r := storedResult{resp: resp, startTime: now, endTime: now}
	switch d := resp.Data.(type) {
	case queryResponseData:
		r.sqlText = d.SQLText
	case map[string]interface{}:
		r.sqlText, _ = d["sqlText"].(string)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byID[queryID]; !exists {
		s.order = append(s.order, queryID)
	}
	s.byID[queryID] = r
	for len(s.order) > s.max {
		delete(s.byID, s.order[0])
		s.order = s.order[1:]
	}
}

func (s *resultStore) get(queryID string) (storedResult, bool) {
	if s == nil {
		return storedResult{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.byID[queryID]
	return r, ok
}

func (s *resultStore) reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.order = nil
	s.byID = make(map[string]storedResult)
}

// ---------------------------------------------------------------------------
// Typed result columns
// ---------------------------------------------------------------------------

// wireTypeFromDeclared maps a DuckDB column type to the Snowflake wire type
// for the types whose Go value alone is ambiguous: DATE, TIME and the
// timestamps all scan into time.Time. Other types return "" and are
// inferred from the value.
func wireTypeFromDeclared(duckType string) string {
	switch t := strings.ToUpper(strings.TrimSpace(duckType)); {
	case t == "DATE":
		return "date"
	case t == "TIME" || strings.HasPrefix(t, "TIME("):
		return "time"
	case t == "TIMESTAMPTZ" || t == "TIMESTAMP WITH TIME ZONE":
		return "timestamp_ltz"
	case strings.HasPrefix(t, "TIMESTAMP"):
		return "timestamp_ntz"
	}
	return ""
}

// wireColumnTypes picks the Snowflake wire type of every result column: the
// declared DuckDB type when it settles the question, otherwise the first
// row's value.
func wireColumnTypes(cols, declared []string, rows [][]interface{}) []string {
	out := make([]string, len(cols))
	for i := range cols {
		if i < len(declared) {
			if t := wireTypeFromDeclared(declared[i]); t != "" {
				out[i] = t
				continue
			}
		}
		out[i] = "text"
		if len(rows) > 0 && i < len(rows[0]) {
			out[i] = snowflakeTypeName(rows[0][i])
		}
	}
	return out
}

func buildRowType(cols, wireTypes []string) []rowTypeField {
	rowType := make([]rowTypeField, len(cols))
	for i, col := range cols {
		typeName := wireTypes[i]
		length, precision, scale, byteLength := snowflakeColumnMetadata(typeName)
		rowType[i] = rowTypeField{
			Name:       col,
			Type:       typeName,
			Length:     length,
			Precision:  precision,
			Scale:      scale,
			ByteLength: byteLength,
			Nullable:   true,
		}
	}
	return rowType
}

// buildRowSetTyped is buildRowSet with per-column wire types, so DATE and
// TIME values use their own encodings: epoch days for date, seconds since
// midnight for time (both with the drivers' expected formatting).
func buildRowSetTyped(rows [][]interface{}, wireTypes []string) [][]interface{} {
	out := make([][]interface{}, len(rows))
	for i, row := range rows {
		outRow := make([]interface{}, len(row))
		for j, cell := range row {
			if cell == nil {
				continue
			}
			wt := ""
			if j < len(wireTypes) {
				wt = wireTypes[j]
			}
			outRow[j] = cellToWire(cell, wt)
		}
		out[i] = outRow
	}
	return out
}

func cellToWire(v interface{}, wireType string) string {
	t, ok := v.(time.Time)
	if !ok {
		return cellToString(v)
	}
	switch wireType {
	case "date":
		// Days since the epoch. Floor so dates before 1970 stay correct.
		days := int64(math.Floor(float64(t.Unix()) / 86400))
		return fmt.Sprintf("%d", days)
	case "time":
		secs := t.Hour()*3600 + t.Minute()*60 + t.Second()
		return fmt.Sprintf("%d.%09d", secs, t.Nanosecond())
	}
	return cellToString(v)
}
