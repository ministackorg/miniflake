package protocol

// --- Login ---

// LoginRequest is sent by the Snowflake driver to authenticate.
type LoginRequest struct {
	Data LoginRequestData `json:"data"`
}

// LoginRequestData contains the authentication fields.
type LoginRequestData struct {
	LoginName        string `json:"LOGIN_NAME"`
	Password         string `json:"PASSWORD"`
	AccountName      string `json:"ACCOUNT_NAME"`
	DatabaseName     string `json:"DATABASE_NAME,omitempty"`
	SchemaName       string `json:"SCHEMA_NAME,omitempty"`
	WarehouseName    string `json:"WAREHOUSE_NAME,omitempty"`
	RoleName         string `json:"ROLE_NAME,omitempty"`
	ClientAppID      string `json:"CLIENT_APP_ID,omitempty"`
	ClientAppVersion string `json:"CLIENT_APP_VERSION,omitempty"`
}

// LoginResponse is returned after authentication.
type LoginResponse struct {
	Data    *LoginResponseData `json:"data"`
	Code    *string            `json:"code"`
	Message *string            `json:"message"`
	Success bool               `json:"success"`
}

// LoginResponseData contains the session details after login.
type LoginResponseData struct {
	Token                   string      `json:"token"`
	MasterToken             string      `json:"masterToken"`
	SessionID               int64       `json:"sessionId"`
	MasterValidityInSeconds int         `json:"masterValidityInSeconds"`
	DisplayUserName         string      `json:"displayUserName"`
	ServerVersion           string      `json:"serverVersion"`
	FirstLogin              bool        `json:"firstLogin"`
	HealthCheckInterval     int         `json:"healthCheckInterval"`
	NewClientForUpgrade     string      `json:"newClientForUpgrade"`
	SessionInfo             SessionInfo `json:"sessionInfo"`
}

// SessionInfo holds the current session context.
type SessionInfo struct {
	DatabaseName  string `json:"databaseName"`
	SchemaName    string `json:"schemaName"`
	WarehouseName string `json:"warehouseName"`
	RoleName      string `json:"roleName"`
}

// --- Query ---

// QueryRequest is sent by the driver to execute SQL.
type QueryRequest struct {
	SQLText             string                  `json:"sqlText"`
	AsyncExec           bool                    `json:"asyncExec"`
	SequenceID          int64                   `json:"sequenceId"`
	IsInternal          bool                    `json:"isInternal"`
	DescribeOnly        bool                    `json:"describeOnly"`
	Parameters          map[string]string       `json:"parameters,omitempty"`
	Bindings            map[string]BindingValue `json:"bindings,omitempty"`
	QuerySubmissionTime int64                   `json:"querySubmissionTime"`
}

// BindingValue represents a single parameter binding.
type BindingValue struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// QueryResponse is returned after query execution.
type QueryResponse struct {
	Data    *QueryResponseData `json:"data"`
	Code    string             `json:"code"`
	Message *string            `json:"message"`
	Success bool               `json:"success"`
}

// QueryResponseData contains the query result.
type QueryResponseData struct {
	QueryID           string         `json:"queryId"`
	SQLText           string         `json:"sqlText"`
	QueryResultFormat string         `json:"queryResultFormat"`
	RowType           []RowTypeField `json:"rowtype"`
	RowSet            [][]string     `json:"rowset"`
	Total             int64          `json:"total"`
	Returned          int64          `json:"returned"`
	QueryStatus       string         `json:"queryStatus,omitempty"`
	StatementTypeID   int64          `json:"statementTypeId"`
	FinalDatabaseName string         `json:"finalDatabaseName,omitempty"`
	FinalSchemaName   string         `json:"finalSchemaName,omitempty"`
}

// RowTypeField describes a single column in a result set.
type RowTypeField struct {
	Name       string `json:"name"`
	Database   string `json:"database,omitempty"`
	Schema     string `json:"schema,omitempty"`
	Table      string `json:"table,omitempty"`
	Type       string `json:"type"`
	Scale      *int64 `json:"scale"`
	Precision  *int64 `json:"precision"`
	Length     *int64 `json:"length"`
	Nullable   bool   `json:"nullable"`
	ByteLength *int64 `json:"byteLength,omitempty"`
}

// --- SQL API v2 ---

// V2StatementRequest is the request body for the SQL API v2 endpoint.
type V2StatementRequest struct {
	Statement  string               `json:"statement"`
	Timeout    int                  `json:"timeout,omitempty"`
	Database   string               `json:"database,omitempty"`
	Schema     string               `json:"schema,omitempty"`
	Warehouse  string               `json:"warehouse,omitempty"`
	Role       string               `json:"role,omitempty"`
	Bindings   map[string]V2Binding `json:"bindings,omitempty"`
	Parameters map[string]string    `json:"parameters,omitempty"`
}

// V2Binding represents a single parameter binding in v2 API.
type V2Binding struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// V2StatementResponse is returned by the SQL API v2 endpoint.
type V2StatementResponse struct {
	ResultSetMetaData  *V2ResultSetMetaData `json:"resultSetMetaData,omitempty"`
	Data               [][]string           `json:"data,omitempty"`
	Code               string               `json:"code"`
	StatementHandle    string               `json:"statementHandle"`
	StatementStatusURL string               `json:"statementStatusUrl"`
	SQLState           string               `json:"sqlState"`
	Message            string               `json:"message"`
	CreatedOn          int64                `json:"createdOn"`
}

// V2ResultSetMetaData describes the shape of a v2 result set.
type V2ResultSetMetaData struct {
	NumRows int64       `json:"numRows"`
	Format  string      `json:"format"`
	RowType []V2RowType `json:"rowType"`
}

// V2RowType describes a single column in a v2 result set.
type V2RowType struct {
	Name       string `json:"name"`
	Database   string `json:"database,omitempty"`
	Schema     string `json:"schema,omitempty"`
	Table      string `json:"table,omitempty"`
	Type       string `json:"type"`
	Scale      *int64 `json:"scale,omitempty"`
	Precision  *int64 `json:"precision,omitempty"`
	Length     *int64 `json:"length,omitempty"`
	Nullable   bool   `json:"nullable"`
	ByteLength *int64 `json:"byteLength,omitempty"`
}

// --- Error ---

// ErrorResponse is the standard error envelope.
type ErrorResponse struct {
	Data    interface{} `json:"data"`
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Success bool        `json:"success"`
}
