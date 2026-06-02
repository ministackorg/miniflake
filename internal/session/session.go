package session

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

const sessionTimeout = 4 * time.Hour

// Session represents an authenticated Snowflake client session.
type Session struct {
	ID           string
	Token        string
	Database     string
	Schema       string
	Warehouse    string
	Role         string
	User         string
	CreatedAt    time.Time
	LastActiveAt time.Time
}

// Manager manages active sessions with concurrent-safe access.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session // keyed by token

	done chan struct{} // signals the expiry goroutine to stop
}

// NewManager creates a Manager and starts the background expiry goroutine.
func NewManager() *Manager {
	m := &Manager{
		sessions: make(map[string]*Session),
		done:     make(chan struct{}),
	}
	go m.expireLoop()
	return m
}

// Stop stops the background expiry goroutine. Call this on shutdown.
func (m *Manager) Stop() {
	close(m.done)
}

// CreateSession creates a new session and returns it. The token is a UUID.
func (m *Manager) CreateSession(user, database, schema, warehouse, role string) *Session {
	now := time.Now()
	s := &Session{
		ID:           generateUUID(),
		Token:        generateUUID(),
		Database:     database,
		Schema:       schema,
		Warehouse:    warehouse,
		Role:         role,
		User:         user,
		CreatedAt:    now,
		LastActiveAt: now,
	}
	m.mu.Lock()
	m.sessions[s.Token] = s
	m.mu.Unlock()
	return s
}

// GetSession returns the session for the given token, or false if not found.
func (m *Manager) GetSession(token string) (*Session, bool) {
	m.mu.RLock()
	s, ok := m.sessions[token]
	m.mu.RUnlock()
	return s, ok
}

// DeleteSession removes a session by token.
func (m *Manager) DeleteSession(token string) {
	m.mu.Lock()
	delete(m.sessions, token)
	m.mu.Unlock()
}

// UpdateActivity updates the LastActiveAt timestamp for the given token.
func (m *Manager) UpdateActivity(token string) {
	m.mu.Lock()
	if s, ok := m.sessions[token]; ok {
		s.LastActiveAt = time.Now()
	}
	m.mu.Unlock()
}

// expireLoop periodically removes sessions inactive for longer than sessionTimeout.
func (m *Manager) expireLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case now := <-ticker.C:
			m.mu.Lock()
			for token, s := range m.sessions {
				if now.Sub(s.LastActiveAt) > sessionTimeout {
					delete(m.sessions, token)
				}
			}
			m.mu.Unlock()
		}
	}
}

// generateUUID produces a v4 UUID string without external dependencies.
func generateUUID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}
