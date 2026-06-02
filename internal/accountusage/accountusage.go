package accountusage

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// QueryHistory represents a row in SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY.
type QueryHistory struct {
	QueryID         string
	QueryText       string
	DatabaseName    string
	SchemaName      string
	UserName        string
	RoleName        string
	WarehouseName   string
	StartTime       time.Time
	EndTime         time.Time
	Status          string // SUCCESS, FAIL, RUNNING
	ErrorCode       string
	ErrorMessage    string
	BytesScanned    int64
	RowsProduced    int64
	ExecutionTimeMs int64
}

// LoginHistory represents a row in SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY.
type LoginHistory struct {
	EventTimestamp time.Time
	UserName       string
	ClientIP       string
	IsSuccess      bool
	ErrorCode      string
	ErrorMessage   string
}

// StorageUsage represents a row in SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE.
type StorageUsage struct {
	UsageDate     time.Time
	DatabaseName  string
	AverageBytes  int64
	DeletedBytes  int64
	FailsafeBytes int64
}

// Store holds in-memory account usage data with configurable eviction.
type Store struct {
	mu           sync.RWMutex
	queryHistory []QueryHistory
	loginHistory []LoginHistory
	storageUsage []StorageUsage
	maxHistory   int
}

// NewStore creates a Store that keeps at most maxHistory entries per category.
func NewStore(maxHistory int) *Store {
	if maxHistory <= 0 {
		maxHistory = 10000
	}
	return &Store{
		queryHistory: make([]QueryHistory, 0),
		loginHistory: make([]LoginHistory, 0),
		storageUsage: make([]StorageUsage, 0),
		maxHistory:   maxHistory,
	}
}

// RecordQuery appends a query history entry, evicting oldest if at capacity.
func (s *Store) RecordQuery(qh QueryHistory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryHistory = append(s.queryHistory, qh)
	if len(s.queryHistory) > s.maxHistory {
		s.queryHistory = s.queryHistory[len(s.queryHistory)-s.maxHistory:]
	}
}

// RecordLogin appends a login history entry, evicting oldest if at capacity.
func (s *Store) RecordLogin(lh LoginHistory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loginHistory = append(s.loginHistory, lh)
	if len(s.loginHistory) > s.maxHistory {
		s.loginHistory = s.loginHistory[len(s.loginHistory)-s.maxHistory:]
	}
}

// RecordStorage appends a storage usage entry, evicting oldest if at capacity.
func (s *Store) RecordStorage(su StorageUsage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storageUsage = append(s.storageUsage, su)
	if len(s.storageUsage) > s.maxHistory {
		s.storageUsage = s.storageUsage[len(s.storageUsage)-s.maxHistory:]
	}
}

// GetQueryHistory returns query history entries, optionally filtered.
// Supported filter keys: DATABASE_NAME, SCHEMA_NAME, USER_NAME, STATUS, WAREHOUSE_NAME.
func (s *Store) GetQueryHistory(limit int, filters map[string]string) []QueryHistory {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []QueryHistory
	// Iterate newest-first.
	for i := len(s.queryHistory) - 1; i >= 0; i-- {
		qh := s.queryHistory[i]
		if matchesFilters(qh, filters) {
			result = append(result, qh)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result
}

func matchesFilters(qh QueryHistory, filters map[string]string) bool {
	for k, v := range filters {
		switch strings.ToUpper(k) {
		case "DATABASE_NAME":
			if !strings.EqualFold(qh.DatabaseName, v) {
				return false
			}
		case "SCHEMA_NAME":
			if !strings.EqualFold(qh.SchemaName, v) {
				return false
			}
		case "USER_NAME":
			if !strings.EqualFold(qh.UserName, v) {
				return false
			}
		case "STATUS":
			if !strings.EqualFold(qh.Status, v) {
				return false
			}
		case "WAREHOUSE_NAME":
			if !strings.EqualFold(qh.WarehouseName, v) {
				return false
			}
		}
	}
	return true
}

// GetLoginHistory returns the most recent login history entries.
func (s *Store) GetLoginHistory(limit int) []LoginHistory {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n := len(s.loginHistory)
	if limit <= 0 || limit > n {
		limit = n
	}
	result := make([]LoginHistory, limit)
	for i := 0; i < limit; i++ {
		result[i] = s.loginHistory[n-1-i]
	}
	return result
}

// GetStorageUsage returns storage usage entries within the given date range (inclusive).
func (s *Store) GetStorageUsage(startDate, endDate time.Time) []StorageUsage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []StorageUsage
	for _, su := range s.storageUsage {
		day := su.UsageDate.Truncate(24 * time.Hour)
		if (day.Equal(startDate) || day.After(startDate)) && (day.Equal(endDate) || day.Before(endDate)) {
			result = append(result, su)
		}
	}
	return result
}

// AsQueryResult formats the given ACCOUNT_USAGE view as column names + rows,
// suitable for returning from a SELECT statement.
func (s *Store) AsQueryResult(viewName string, limit int) ([]string, [][]interface{}) {
	switch strings.ToUpper(viewName) {
	case "QUERY_HISTORY":
		return s.queryHistoryResult(limit)
	case "LOGIN_HISTORY":
		return s.loginHistoryResult(limit)
	case "STORAGE_USAGE":
		return s.storageUsageResult(limit)
	default:
		return nil, nil
	}
}

func (s *Store) queryHistoryResult(limit int) ([]string, [][]interface{}) {
	cols := []string{
		"QUERY_ID", "QUERY_TEXT", "DATABASE_NAME", "SCHEMA_NAME",
		"USER_NAME", "ROLE_NAME", "WAREHOUSE_NAME",
		"START_TIME", "END_TIME", "STATUS",
		"ERROR_CODE", "ERROR_MESSAGE",
		"BYTES_SCANNED", "ROWS_PRODUCED", "EXECUTION_TIME",
	}
	entries := s.GetQueryHistory(limit, nil)
	rows := make([][]interface{}, len(entries))
	for i, qh := range entries {
		rows[i] = []interface{}{
			qh.QueryID, qh.QueryText, qh.DatabaseName, qh.SchemaName,
			qh.UserName, qh.RoleName, qh.WarehouseName,
			qh.StartTime.Format(time.RFC3339), qh.EndTime.Format(time.RFC3339), qh.Status,
			qh.ErrorCode, qh.ErrorMessage,
			fmt.Sprintf("%d", qh.BytesScanned), fmt.Sprintf("%d", qh.RowsProduced), fmt.Sprintf("%d", qh.ExecutionTimeMs),
		}
	}
	return cols, rows
}

func (s *Store) loginHistoryResult(limit int) ([]string, [][]interface{}) {
	cols := []string{
		"EVENT_TIMESTAMP", "USER_NAME", "CLIENT_IP",
		"IS_SUCCESS", "ERROR_CODE", "ERROR_MESSAGE",
	}
	entries := s.GetLoginHistory(limit)
	rows := make([][]interface{}, len(entries))
	for i, lh := range entries {
		success := "NO"
		if lh.IsSuccess {
			success = "YES"
		}
		rows[i] = []interface{}{
			lh.EventTimestamp.Format(time.RFC3339), lh.UserName, lh.ClientIP,
			success, lh.ErrorCode, lh.ErrorMessage,
		}
	}
	return cols, rows
}

func (s *Store) storageUsageResult(limit int) ([]string, [][]interface{}) {
	cols := []string{
		"USAGE_DATE", "DATABASE_NAME",
		"AVERAGE_DATABASE_BYTES", "AVERAGE_DATABASE_DELETED_BYTES", "AVERAGE_FAILSAFE_BYTES",
	}
	s.mu.RLock()
	entries := make([]StorageUsage, len(s.storageUsage))
	copy(entries, s.storageUsage)
	s.mu.RUnlock()

	if limit > 0 && limit < len(entries) {
		entries = entries[len(entries)-limit:]
	}
	rows := make([][]interface{}, len(entries))
	for i, su := range entries {
		rows[i] = []interface{}{
			su.UsageDate.Format("2006-01-02"), su.DatabaseName,
			fmt.Sprintf("%d", su.AverageBytes), fmt.Sprintf("%d", su.DeletedBytes), fmt.Sprintf("%d", su.FailsafeBytes),
		}
	}
	return cols, rows
}
