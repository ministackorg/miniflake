package sharing

import (
	"fmt"
	"sync"
	"time"
)

// ShareType indicates whether a share is outbound (provider) or inbound (consumer).
type ShareType string

const (
	ShareOutbound ShareType = "OUTBOUND"
	ShareInbound  ShareType = "INBOUND"
)

// SharedObject represents a database object included in a share.
type SharedObject struct {
	Type string // TABLE, VIEW, SCHEMA
	Name string // fully qualified name
}

// Share represents a Snowflake data share.
type Share struct {
	Name      string
	Type      ShareType
	Database  string         // the database being shared
	Objects   []SharedObject // tables/views included
	Accounts  []string       // consumer account names
	Comment   string
	CreatedAt time.Time
}

// ShareInfo is the read-only metadata returned by ShowShares.
type ShareInfo struct {
	Name      string
	Type      ShareType
	Database  string
	Comment   string
	Accounts  int
	Objects   int
	CreatedAt time.Time
}

// Manager manages data shares in the emulator.
type Manager struct {
	mu     sync.RWMutex
	shares map[string]*Share
}

// NewManager creates a new sharing manager.
func NewManager() *Manager {
	return &Manager{
		shares: make(map[string]*Share),
	}
}

// CreateShare creates a new outbound share.
func (m *Manager) CreateShare(name, comment string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name == "" {
		return fmt.Errorf("share name cannot be empty")
	}
	if _, exists := m.shares[name]; exists {
		return fmt.Errorf("share '%s' already exists", name)
	}

	m.shares[name] = &Share{
		Name:      name,
		Type:      ShareOutbound,
		Comment:   comment,
		Objects:   []SharedObject{},
		Accounts:  []string{},
		CreatedAt: time.Now(),
	}
	return nil
}

// DropShare removes a share.
func (m *Manager) DropShare(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.shares[name]; !exists {
		return fmt.Errorf("share '%s' does not exist", name)
	}
	delete(m.shares, name)
	return nil
}

// GrantToShare adds an object to a share.
func (m *Manager) GrantToShare(shareName string, objectType, objectName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	share, exists := m.shares[shareName]
	if !exists {
		return fmt.Errorf("share '%s' does not exist", shareName)
	}

	// Check for duplicate.
	for _, obj := range share.Objects {
		if obj.Type == objectType && obj.Name == objectName {
			return fmt.Errorf("object '%s' of type '%s' already granted to share '%s'", objectName, objectType, shareName)
		}
	}

	share.Objects = append(share.Objects, SharedObject{
		Type: objectType,
		Name: objectName,
	})
	return nil
}

// RevokeFromShare removes an object from a share.
func (m *Manager) RevokeFromShare(shareName string, objectType, objectName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	share, exists := m.shares[shareName]
	if !exists {
		return fmt.Errorf("share '%s' does not exist", shareName)
	}

	for i, obj := range share.Objects {
		if obj.Type == objectType && obj.Name == objectName {
			share.Objects = append(share.Objects[:i], share.Objects[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("object '%s' of type '%s' not found in share '%s'", objectName, objectType, shareName)
}

// AlterShareAddAccounts adds consumer accounts to a share.
func (m *Manager) AlterShareAddAccounts(shareName string, accounts []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	share, exists := m.shares[shareName]
	if !exists {
		return fmt.Errorf("share '%s' does not exist", shareName)
	}

	existing := make(map[string]bool, len(share.Accounts))
	for _, a := range share.Accounts {
		existing[a] = true
	}

	for _, a := range accounts {
		if !existing[a] {
			share.Accounts = append(share.Accounts, a)
			existing[a] = true
		}
	}
	return nil
}

// AlterShareRemoveAccounts removes consumer accounts from a share.
func (m *Manager) AlterShareRemoveAccounts(shareName string, accounts []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	share, exists := m.shares[shareName]
	if !exists {
		return fmt.Errorf("share '%s' does not exist", shareName)
	}

	toRemove := make(map[string]bool, len(accounts))
	for _, a := range accounts {
		toRemove[a] = true
	}

	filtered := make([]string, 0, len(share.Accounts))
	for _, a := range share.Accounts {
		if !toRemove[a] {
			filtered = append(filtered, a)
		}
	}
	share.Accounts = filtered
	return nil
}

// ShowShares returns metadata for all shares.
func (m *Manager) ShowShares() []ShareInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]ShareInfo, 0, len(m.shares))
	for _, s := range m.shares {
		result = append(result, ShareInfo{
			Name:      s.Name,
			Type:      s.Type,
			Database:  s.Database,
			Comment:   s.Comment,
			Accounts:  len(s.Accounts),
			Objects:   len(s.Objects),
			CreatedAt: s.CreatedAt,
		})
	}
	return result
}

// DescribeShare returns the full share definition.
func (m *Manager) DescribeShare(name string) (*Share, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	share, exists := m.shares[name]
	if !exists {
		return nil, fmt.Errorf("share '%s' does not exist", name)
	}

	// Return a copy to avoid races.
	cp := *share
	cp.Objects = make([]SharedObject, len(share.Objects))
	copy(cp.Objects, share.Objects)
	cp.Accounts = make([]string, len(share.Accounts))
	copy(cp.Accounts, share.Accounts)
	return &cp, nil
}

// CreateDatabaseFromShare creates a database from an inbound share (consumer side).
func (m *Manager) CreateDatabaseFromShare(shareName, dbName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	share, exists := m.shares[shareName]
	if !exists {
		return fmt.Errorf("share '%s' does not exist", shareName)
	}

	// Create an inbound share entry representing the consumer's mounted database.
	inboundName := dbName + "_from_" + shareName
	if _, exists := m.shares[inboundName]; exists {
		return fmt.Errorf("database '%s' from share '%s' already exists", dbName, shareName)
	}

	m.shares[inboundName] = &Share{
		Name:      inboundName,
		Type:      ShareInbound,
		Database:  dbName,
		Objects:   make([]SharedObject, len(share.Objects)),
		Accounts:  []string{},
		Comment:   fmt.Sprintf("Database created from share '%s'", shareName),
		CreatedAt: time.Now(),
	}
	copy(m.shares[inboundName].Objects, share.Objects)
	return nil
}
