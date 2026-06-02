package network

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Policy represents a Snowflake network policy.
type Policy struct {
	Name          string
	AllowedIPList []string // CIDR notation
	BlockedIPList []string // CIDR notation
	Comment       string
	CreatedAt     time.Time
}

// PolicyInfo is the read-only representation returned by ShowPolicies.
type PolicyInfo struct {
	Name      string
	Comment   string
	CreatedAt time.Time
}

// Manager manages network policies for the account.
type Manager struct {
	mu           sync.RWMutex
	policies     map[string]*Policy
	activePolicy string
}

// NewManager creates a Manager with no policies.
func NewManager() *Manager {
	return &Manager{
		policies: make(map[string]*Policy),
	}
}

// CreatePolicy creates a new network policy.
func (m *Manager) CreatePolicy(name string, allowedIPs, blockedIPs []string, comment string) error {
	if err := validateCIDRList(allowedIPs); err != nil {
		return fmt.Errorf("allowed IP list: %w", err)
	}
	if err := validateCIDRList(blockedIPs); err != nil {
		return fmt.Errorf("blocked IP list: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.policies[name]; exists {
		return fmt.Errorf("network policy %q already exists", name)
	}

	m.policies[name] = &Policy{
		Name:          name,
		AllowedIPList: copyStrings(allowedIPs),
		BlockedIPList: copyStrings(blockedIPs),
		Comment:       comment,
		CreatedAt:     time.Now(),
	}
	return nil
}

// DropPolicy removes a network policy. It cannot drop the active policy.
func (m *Manager) DropPolicy(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.policies[name]; !exists {
		return fmt.Errorf("network policy %q does not exist", name)
	}
	if m.activePolicy == name {
		return fmt.Errorf("cannot drop active network policy %q; unset it first", name)
	}
	delete(m.policies, name)
	return nil
}

// AlterPolicy updates the allowed and blocked IP lists of an existing policy.
func (m *Manager) AlterPolicy(name string, allowedIPs, blockedIPs []string) error {
	if err := validateCIDRList(allowedIPs); err != nil {
		return fmt.Errorf("allowed IP list: %w", err)
	}
	if err := validateCIDRList(blockedIPs); err != nil {
		return fmt.Errorf("blocked IP list: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	p, exists := m.policies[name]
	if !exists {
		return fmt.Errorf("network policy %q does not exist", name)
	}
	p.AllowedIPList = copyStrings(allowedIPs)
	p.BlockedIPList = copyStrings(blockedIPs)
	return nil
}

// SetActivePolicy sets the account-level active network policy.
// Pass an empty string to unset.
func (m *Manager) SetActivePolicy(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name != "" {
		if _, exists := m.policies[name]; !exists {
			return fmt.Errorf("network policy %q does not exist", name)
		}
	}
	m.activePolicy = name
	return nil
}

// CheckAccess checks whether the given client IP is allowed under the active
// network policy. Returns (allowed, reason). If no policy is active, all
// traffic is allowed.
func (m *Manager) CheckAccess(clientIP string) (bool, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.activePolicy == "" {
		return true, "no active network policy"
	}

	p, exists := m.policies[m.activePolicy]
	if !exists {
		return true, "active policy not found"
	}

	ip := net.ParseIP(clientIP)
	if ip == nil {
		return false, fmt.Sprintf("invalid client IP: %s", clientIP)
	}

	// Blocked list takes precedence.
	for _, cidr := range p.BlockedIPList {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return false, fmt.Sprintf("IP %s is in blocked list (%s)", clientIP, cidr)
		}
	}

	// If there is an allowed list, the IP must match at least one entry.
	if len(p.AllowedIPList) > 0 {
		for _, cidr := range p.AllowedIPList {
			_, network, err := net.ParseCIDR(cidr)
			if err != nil {
				continue
			}
			if network.Contains(ip) {
				return true, "allowed"
			}
		}
		return false, fmt.Sprintf("IP %s is not in allowed list", clientIP)
	}

	return true, "allowed"
}

// ShowPolicies returns info about all policies.
func (m *Manager) ShowPolicies() []PolicyInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]PolicyInfo, 0, len(m.policies))
	for _, p := range m.policies {
		result = append(result, PolicyInfo{
			Name:      p.Name,
			Comment:   p.Comment,
			CreatedAt: p.CreatedAt,
		})
	}
	return result
}

// DescribePolicy returns the full policy definition.
func (m *Manager) DescribePolicy(name string) (*Policy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p, exists := m.policies[name]
	if !exists {
		return nil, fmt.Errorf("network policy %q does not exist", name)
	}
	// Return a copy.
	cp := *p
	cp.AllowedIPList = copyStrings(p.AllowedIPList)
	cp.BlockedIPList = copyStrings(p.BlockedIPList)
	return &cp, nil
}

// validateCIDRList checks that every entry is valid CIDR notation.
func validateCIDRList(cidrs []string) error {
	for _, c := range cidrs {
		if _, _, err := net.ParseCIDR(c); err != nil {
			return fmt.Errorf("invalid CIDR %q: %w", c, err)
		}
	}
	return nil
}

func copyStrings(s []string) []string {
	if s == nil {
		return nil
	}
	c := make([]string, len(s))
	copy(c, s)
	return c
}
