package extfunc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// FuncArg describes a function parameter.
type FuncArg struct {
	Name string
	Type string
}

// ExternalFunction represents a Snowflake external function.
type ExternalFunction struct {
	Name           string
	Database       string
	Schema         string
	Args           []FuncArg
	ReturnType     string
	APIIntegration string
	URL            string
	Headers        map[string]string
	MaxBatchRows   int
	Compression    string // NONE, AUTO
	CreatedAt      time.Time
}

// APIIntegration stores configuration for an API gateway.
type APIIntegration struct {
	Name        string
	APIProvider string // aws_api_gateway, google_api_gateway, azure_api_management
	APIURL      string
	APIKey      string
	Enabled     bool
	CreatedAt   time.Time
}

// FunctionInfo is the read-only metadata returned by ShowFunctions.
type FunctionInfo struct {
	Name           string
	Database       string
	Schema         string
	ReturnType     string
	APIIntegration string
	CreatedAt      time.Time
}

// IntegrationInfo is the read-only metadata returned by ShowIntegrations.
type IntegrationInfo struct {
	Name        string
	APIProvider string
	APIURL      string
	Enabled     bool
	CreatedAt   time.Time
}

// Manager manages external functions and API integrations.
type Manager struct {
	mu           sync.RWMutex
	functions    map[string]*ExternalFunction // key: DB.SCHEMA.FUNC(types)
	integrations map[string]*APIIntegration
	httpClient   *http.Client
}

// NewManager creates a new external function manager.
func NewManager() *Manager {
	return &Manager{
		functions:    make(map[string]*ExternalFunction),
		integrations: make(map[string]*APIIntegration),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// funcKey returns the map key for a function.
func funcKey(db, schema, name string) string {
	return strings.ToUpper(fmt.Sprintf("%s.%s.%s", db, schema, name))
}

// CreateIntegration creates a new API integration.
func (m *Manager) CreateIntegration(name, provider, url, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name == "" {
		return fmt.Errorf("integration name cannot be empty")
	}
	if _, exists := m.integrations[name]; exists {
		return fmt.Errorf("integration '%s' already exists", name)
	}

	m.integrations[name] = &APIIntegration{
		Name:        name,
		APIProvider: provider,
		APIURL:      url,
		APIKey:      key,
		Enabled:     true,
		CreatedAt:   time.Now(),
	}
	return nil
}

// DropIntegration removes an API integration.
func (m *Manager) DropIntegration(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.integrations[name]; !exists {
		return fmt.Errorf("integration '%s' does not exist", name)
	}
	delete(m.integrations, name)
	return nil
}

// CreateFunction creates a new external function.
func (m *Manager) CreateFunction(db, schema, name string, args []FuncArg, returnType, integration, url string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := funcKey(db, schema, name)
	if _, exists := m.functions[key]; exists {
		return fmt.Errorf("function '%s' already exists", key)
	}

	if _, exists := m.integrations[integration]; !exists {
		return fmt.Errorf("integration '%s' does not exist", integration)
	}

	m.functions[key] = &ExternalFunction{
		Name:           name,
		Database:       db,
		Schema:         schema,
		Args:           args,
		ReturnType:     returnType,
		APIIntegration: integration,
		URL:            url,
		Headers:        make(map[string]string),
		MaxBatchRows:   100,
		Compression:    "NONE",
		CreatedAt:      time.Now(),
	}
	return nil
}

// DropFunction removes an external function.
func (m *Manager) DropFunction(db, schema, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := funcKey(db, schema, name)
	if _, exists := m.functions[key]; !exists {
		return fmt.Errorf("function '%s' does not exist", key)
	}
	delete(m.functions, key)
	return nil
}

// callRequest is the JSON body sent to the external API.
type callRequest struct {
	Data [][]interface{} `json:"data"`
}

// callResponse is the JSON body received from the external API.
type callResponse struct {
	Data [][]interface{} `json:"data"`
}

// Call invokes an external function with the given arguments.
// Each element in args is a row of arguments; returns one result per row.
func (m *Manager) Call(ctx context.Context, db, schema, name string, args [][]interface{}) ([]interface{}, error) {
	m.mu.RLock()
	key := funcKey(db, schema, name)
	fn, exists := m.functions[key]
	if !exists {
		m.mu.RUnlock()
		return nil, fmt.Errorf("function '%s' does not exist", key)
	}

	integration, intExists := m.integrations[fn.APIIntegration]
	if !intExists {
		m.mu.RUnlock()
		return nil, fmt.Errorf("integration '%s' does not exist", fn.APIIntegration)
	}

	// Build the full URL: integration base + function path.
	fullURL := fn.URL
	if fullURL == "" {
		fullURL = integration.APIURL
	}

	headers := make(map[string]string, len(fn.Headers))
	for k, v := range fn.Headers {
		headers[k] = v
	}
	apiKey := integration.APIKey
	m.mu.RUnlock()

	// Prepare request body with row numbers.
	numberedData := make([][]interface{}, len(args))
	for i, row := range args {
		numberedRow := make([]interface{}, 0, len(row)+1)
		numberedRow = append(numberedRow, i)
		numberedRow = append(numberedRow, row...)
		numberedData[i] = numberedRow
	}

	body, err := json.Marshal(callRequest{Data: numberedData})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("sf-custom-api-key", apiKey)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("external function call failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("external function returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result callResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Extract the result value from each row (row format: [row_number, result]).
	values := make([]interface{}, len(result.Data))
	for i, row := range result.Data {
		if len(row) >= 2 {
			values[i] = row[1]
		}
	}
	return values, nil
}

// ShowFunctions returns metadata for functions in the given database and schema.
func (m *Manager) ShowFunctions(db, schema string) []FunctionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []FunctionInfo
	prefix := strings.ToUpper(fmt.Sprintf("%s.%s.", db, schema))
	for key, fn := range m.functions {
		if strings.HasPrefix(key, prefix) {
			result = append(result, FunctionInfo{
				Name:           fn.Name,
				Database:       fn.Database,
				Schema:         fn.Schema,
				ReturnType:     fn.ReturnType,
				APIIntegration: fn.APIIntegration,
				CreatedAt:      fn.CreatedAt,
			})
		}
	}
	return result
}

// ShowIntegrations returns metadata for all API integrations.
func (m *Manager) ShowIntegrations() []IntegrationInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]IntegrationInfo, 0, len(m.integrations))
	for _, intg := range m.integrations {
		result = append(result, IntegrationInfo{
			Name:        intg.Name,
			APIProvider: intg.APIProvider,
			APIURL:      intg.APIURL,
			Enabled:     intg.Enabled,
			CreatedAt:   intg.CreatedAt,
		})
	}
	return result
}
