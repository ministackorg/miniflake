package rbac

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Privilege represents a Snowflake privilege.
type Privilege string

const (
	PrivUsage     Privilege = "USAGE"
	PrivSelect    Privilege = "SELECT"
	PrivInsert    Privilege = "INSERT"
	PrivUpdate    Privilege = "UPDATE"
	PrivDelete    Privilege = "DELETE"
	PrivCreate    Privilege = "CREATE"
	PrivDrop      Privilege = "DROP"
	PrivAll       Privilege = "ALL"
	PrivOwnership Privilege = "OWNERSHIP"
)

// ObjectType represents the type of object a privilege applies to.
type ObjectType string

const (
	ObjDatabase  ObjectType = "DATABASE"
	ObjSchema    ObjectType = "SCHEMA"
	ObjTable     ObjectType = "TABLE"
	ObjView      ObjectType = "VIEW"
	ObjWarehouse ObjectType = "WAREHOUSE"
	ObjRole      ObjectType = "ROLE"
	ObjStage     ObjectType = "STAGE"
)

// Grant records a privilege grant.
type Grant struct {
	Privilege  Privilege
	ObjectType ObjectType
	ObjectName string
	GrantedTo  string // role name
	GrantedBy  string
	GrantedAt  time.Time
	WithGrant  bool
}

// Role represents a Snowflake role with its grants and hierarchy.
type Role struct {
	Name        string
	Grants      []Grant
	ParentRoles []string // role hierarchy (roles this role is granted to)
	CreatedAt   time.Time
}

// Engine manages roles, grants, and access checks.
type Engine struct {
	mu        sync.RWMutex
	roles     map[string]*Role
	userRoles map[string][]string // user -> []role mapping
}

// NewEngine creates a new RBAC engine.
func NewEngine() *Engine {
	return &Engine{
		roles:     make(map[string]*Role),
		userRoles: make(map[string][]string),
	}
}

// Init creates default Snowflake roles: ACCOUNTADMIN, SYSADMIN,
// SECURITYADMIN, USERADMIN, PUBLIC.
func (e *Engine) Init() {
	defaults := []string{"ACCOUNTADMIN", "SYSADMIN", "SECURITYADMIN", "USERADMIN", "PUBLIC"}
	for _, name := range defaults {
		_ = e.CreateRole(name)
	}
	// Set up hierarchy: SYSADMIN, SECURITYADMIN -> ACCOUNTADMIN
	_ = e.GrantRoleToRole("SYSADMIN", "ACCOUNTADMIN")
	_ = e.GrantRoleToRole("SECURITYADMIN", "ACCOUNTADMIN")
	_ = e.GrantRoleToRole("USERADMIN", "SECURITYADMIN")
}

// CreateRole creates a new role.
func (e *Engine) CreateRole(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := strings.ToUpper(name)
	if _, exists := e.roles[key]; exists {
		return fmt.Errorf("rbac: role '%s' already exists", key)
	}
	e.roles[key] = &Role{
		Name:      key,
		CreatedAt: time.Now(),
	}
	return nil
}

// DropRole removes a role.
func (e *Engine) DropRole(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := strings.ToUpper(name)
	if _, exists := e.roles[key]; !exists {
		return fmt.Errorf("rbac: role '%s' does not exist", key)
	}
	delete(e.roles, key)

	// Remove from parent references in other roles.
	for _, r := range e.roles {
		filtered := r.ParentRoles[:0]
		for _, p := range r.ParentRoles {
			if p != key {
				filtered = append(filtered, p)
			}
		}
		r.ParentRoles = filtered
	}

	// Remove from user role mappings.
	for user, roles := range e.userRoles {
		filtered := roles[:0]
		for _, r := range roles {
			if r != key {
				filtered = append(filtered, r)
			}
		}
		e.userRoles[user] = filtered
	}
	return nil
}

// GrantPrivilege grants a privilege on an object to a role.
func (e *Engine) GrantPrivilege(privilege Privilege, objType ObjectType, objName, roleName string, withGrant bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	roleKey := strings.ToUpper(roleName)
	role, ok := e.roles[roleKey]
	if !ok {
		return fmt.Errorf("rbac: role '%s' does not exist", roleKey)
	}
	objKey := strings.ToUpper(objName)

	// Check for duplicate.
	for _, g := range role.Grants {
		if g.Privilege == privilege && g.ObjectType == objType && g.ObjectName == objKey {
			return nil // already granted, idempotent
		}
	}

	role.Grants = append(role.Grants, Grant{
		Privilege:  privilege,
		ObjectType: objType,
		ObjectName: objKey,
		GrantedTo:  roleKey,
		GrantedAt:  time.Now(),
		WithGrant:  withGrant,
	})
	return nil
}

// RevokePrivilege removes a privilege on an object from a role.
func (e *Engine) RevokePrivilege(privilege Privilege, objType ObjectType, objName, roleName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	roleKey := strings.ToUpper(roleName)
	role, ok := e.roles[roleKey]
	if !ok {
		return fmt.Errorf("rbac: role '%s' does not exist", roleKey)
	}
	objKey := strings.ToUpper(objName)

	found := false
	filtered := role.Grants[:0]
	for _, g := range role.Grants {
		if g.Privilege == privilege && g.ObjectType == objType && g.ObjectName == objKey {
			found = true
			continue
		}
		filtered = append(filtered, g)
	}
	if !found {
		return fmt.Errorf("rbac: grant not found for role '%s'", roleKey)
	}
	role.Grants = filtered
	return nil
}

// GrantRoleToUser assigns a role to a user.
func (e *Engine) GrantRoleToUser(roleName, userName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	roleKey := strings.ToUpper(roleName)
	if _, ok := e.roles[roleKey]; !ok {
		return fmt.Errorf("rbac: role '%s' does not exist", roleKey)
	}
	userKey := strings.ToUpper(userName)

	// Check for duplicate.
	for _, r := range e.userRoles[userKey] {
		if r == roleKey {
			return nil
		}
	}
	e.userRoles[userKey] = append(e.userRoles[userKey], roleKey)
	return nil
}

// GrantRoleToRole sets up role hierarchy: childRole is granted to parentRole.
func (e *Engine) GrantRoleToRole(childRole, parentRole string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	childKey := strings.ToUpper(childRole)
	parentKey := strings.ToUpper(parentRole)

	child, ok := e.roles[childKey]
	if !ok {
		return fmt.Errorf("rbac: child role '%s' does not exist", childKey)
	}
	if _, ok := e.roles[parentKey]; !ok {
		return fmt.Errorf("rbac: parent role '%s' does not exist", parentKey)
	}

	// Check for duplicate.
	for _, p := range child.ParentRoles {
		if p == parentKey {
			return nil
		}
	}
	child.ParentRoles = append(child.ParentRoles, parentKey)
	return nil
}

// CheckAccess verifies whether a user (via a role) has a specific privilege
// on an object. It traverses the role hierarchy.
func (e *Engine) CheckAccess(userName, roleName string, privilege Privilege, objType ObjectType, objName string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	roleKey := strings.ToUpper(roleName)
	userKey := strings.ToUpper(userName)
	objKey := strings.ToUpper(objName)

	// Verify user has the role (or the role inherits from a role the user has).
	if !e.userHasRoleLocked(userKey, roleKey) {
		return false
	}

	// Check if the role or any of its child roles have the privilege.
	return e.roleHasPrivilegeLocked(roleKey, privilege, objType, objKey, make(map[string]bool))
}

// userHasRoleLocked checks if a user is assigned the given role, directly
// or through hierarchy. Caller must hold mu.
func (e *Engine) userHasRoleLocked(userKey, roleKey string) bool {
	// Direct assignment.
	for _, r := range e.userRoles[userKey] {
		if r == roleKey {
			return true
		}
		// Check if roleKey is a parent of r (user has child, wants parent).
		if e.roleInheritsLocked(roleKey, r, make(map[string]bool)) {
			return true
		}
	}
	return false
}

// roleInheritsLocked checks if parentRole inherits from childRole
// through the hierarchy. A parent inherits from a child if the child
// has the parent in its ParentRoles chain.
func (e *Engine) roleInheritsLocked(targetRole, currentRole string, visited map[string]bool) bool {
	if currentRole == targetRole {
		return true
	}
	if visited[currentRole] {
		return false
	}
	visited[currentRole] = true

	role, ok := e.roles[currentRole]
	if !ok {
		return false
	}
	for _, parent := range role.ParentRoles {
		if e.roleInheritsLocked(targetRole, parent, visited) {
			return true
		}
	}
	return false
}

// roleHasPrivilegeLocked checks if a role or any role it inherits from
// has the specified privilege. In Snowflake, parent roles inherit
// privileges from child roles. So ACCOUNTADMIN inherits from SYSADMIN.
func (e *Engine) roleHasPrivilegeLocked(roleKey string, privilege Privilege, objType ObjectType, objName string, visited map[string]bool) bool {
	if visited[roleKey] {
		return false
	}
	visited[roleKey] = true

	role, ok := e.roles[roleKey]
	if !ok {
		return false
	}

	// Check direct grants on this role.
	for _, g := range role.Grants {
		if (g.Privilege == privilege || g.Privilege == PrivAll) &&
			g.ObjectType == objType &&
			g.ObjectName == objName {
			return true
		}
	}

	// Check child roles (roles that have this role as a parent inherit upward).
	for _, r := range e.roles {
		for _, parent := range r.ParentRoles {
			if parent == roleKey {
				if e.roleHasPrivilegeLocked(r.Name, privilege, objType, objName, visited) {
					return true
				}
			}
		}
	}

	return false
}

// ShowGrants returns all grants for a role.
func (e *Engine) ShowGrants(roleName string) []Grant {
	e.mu.RLock()
	defer e.mu.RUnlock()
	roleKey := strings.ToUpper(roleName)
	role, ok := e.roles[roleKey]
	if !ok {
		return nil
	}
	out := make([]Grant, len(role.Grants))
	copy(out, role.Grants)
	return out
}

// ShowRoles returns all roles.
func (e *Engine) ShowRoles() []Role {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]Role, 0, len(e.roles))
	for _, r := range e.roles {
		result = append(result, *r)
	}
	return result
}
