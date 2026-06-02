package rbac

import (
	"testing"
)

func TestInit(t *testing.T) {
	e := NewEngine()
	e.Init()

	roles := e.ShowRoles()
	if len(roles) != 5 {
		t.Fatalf("expected 5 default roles, got %d", len(roles))
	}

	expected := map[string]bool{
		"ACCOUNTADMIN":  false,
		"SYSADMIN":      false,
		"SECURITYADMIN": false,
		"USERADMIN":     false,
		"PUBLIC":        false,
	}
	for _, r := range roles {
		if _, ok := expected[r.Name]; ok {
			expected[r.Name] = true
		}
	}
	for name, found := range expected {
		if !found {
			t.Errorf("missing default role: %s", name)
		}
	}
}

func TestCreateDropRole(t *testing.T) {
	e := NewEngine()

	if err := e.CreateRole("analyst"); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}

	// Duplicate.
	if err := e.CreateRole("ANALYST"); err == nil {
		t.Fatal("expected error on duplicate role")
	}

	if err := e.DropRole("analyst"); err != nil {
		t.Fatalf("DropRole: %v", err)
	}

	// Drop nonexistent.
	if err := e.DropRole("analyst"); err == nil {
		t.Fatal("expected error dropping nonexistent role")
	}
}

func TestGrantRevoke(t *testing.T) {
	e := NewEngine()
	_ = e.CreateRole("analyst")

	// Grant SELECT on a table.
	err := e.GrantPrivilege(PrivSelect, ObjTable, "mydb.public.users", "analyst", false)
	if err != nil {
		t.Fatalf("GrantPrivilege: %v", err)
	}

	grants := e.ShowGrants("analyst")
	if len(grants) != 1 {
		t.Fatalf("expected 1 grant, got %d", len(grants))
	}
	if grants[0].Privilege != PrivSelect {
		t.Errorf("expected SELECT, got %s", grants[0].Privilege)
	}

	// Idempotent grant.
	err = e.GrantPrivilege(PrivSelect, ObjTable, "mydb.public.users", "analyst", false)
	if err != nil {
		t.Fatalf("idempotent grant should not error: %v", err)
	}
	if len(e.ShowGrants("analyst")) != 1 {
		t.Error("idempotent grant should not duplicate")
	}

	// Revoke.
	err = e.RevokePrivilege(PrivSelect, ObjTable, "mydb.public.users", "analyst")
	if err != nil {
		t.Fatalf("RevokePrivilege: %v", err)
	}
	if len(e.ShowGrants("analyst")) != 0 {
		t.Error("expected 0 grants after revoke")
	}

	// Revoke nonexistent.
	err = e.RevokePrivilege(PrivSelect, ObjTable, "mydb.public.users", "analyst")
	if err == nil {
		t.Fatal("expected error revoking nonexistent grant")
	}

	// Grant to nonexistent role.
	err = e.GrantPrivilege(PrivSelect, ObjTable, "x", "nonexistent", false)
	if err == nil {
		t.Fatal("expected error granting to nonexistent role")
	}
}

func TestCheckAccess(t *testing.T) {
	e := NewEngine()
	_ = e.CreateRole("analyst")
	_ = e.GrantPrivilege(PrivSelect, ObjTable, "mydb.public.users", "analyst", false)
	_ = e.GrantRoleToUser("analyst", "alice")

	// Alice should have SELECT via analyst role.
	if !e.CheckAccess("alice", "analyst", PrivSelect, ObjTable, "mydb.public.users") {
		t.Error("expected access granted")
	}

	// Alice should not have INSERT.
	if e.CheckAccess("alice", "analyst", PrivInsert, ObjTable, "mydb.public.users") {
		t.Error("expected access denied for INSERT")
	}

	// Bob has no roles.
	if e.CheckAccess("bob", "analyst", PrivSelect, ObjTable, "mydb.public.users") {
		t.Error("expected access denied for bob")
	}
}

func TestCheckAccessWithAll(t *testing.T) {
	e := NewEngine()
	_ = e.CreateRole("admin")
	_ = e.GrantPrivilege(PrivAll, ObjTable, "mydb.public.users", "admin", false)
	_ = e.GrantRoleToUser("admin", "alice")

	if !e.CheckAccess("alice", "admin", PrivSelect, ObjTable, "mydb.public.users") {
		t.Error("ALL should grant SELECT")
	}
	if !e.CheckAccess("alice", "admin", PrivInsert, ObjTable, "mydb.public.users") {
		t.Error("ALL should grant INSERT")
	}
}

func TestRoleHierarchy(t *testing.T) {
	e := NewEngine()
	e.Init()

	// Grant SELECT to SYSADMIN.
	_ = e.GrantPrivilege(PrivSelect, ObjTable, "mydb.public.users", "sysadmin", false)

	// Assign ACCOUNTADMIN to alice.
	_ = e.GrantRoleToUser("accountadmin", "alice")

	// ACCOUNTADMIN inherits from SYSADMIN, so alice should have SELECT.
	if !e.CheckAccess("alice", "accountadmin", PrivSelect, ObjTable, "MYDB.PUBLIC.USERS") {
		t.Error("ACCOUNTADMIN should inherit SELECT from SYSADMIN")
	}

	// User with only PUBLIC should not have it.
	_ = e.GrantRoleToUser("public", "bob")
	if e.CheckAccess("bob", "public", PrivSelect, ObjTable, "MYDB.PUBLIC.USERS") {
		t.Error("PUBLIC should not inherit SYSADMIN privileges")
	}
}

func TestGrantRoleToUser(t *testing.T) {
	e := NewEngine()
	_ = e.CreateRole("analyst")

	if err := e.GrantRoleToUser("analyst", "alice"); err != nil {
		t.Fatalf("GrantRoleToUser: %v", err)
	}

	// Idempotent.
	if err := e.GrantRoleToUser("analyst", "alice"); err != nil {
		t.Fatalf("idempotent GrantRoleToUser: %v", err)
	}

	// Nonexistent role.
	if err := e.GrantRoleToUser("nonexistent", "alice"); err == nil {
		t.Fatal("expected error for nonexistent role")
	}
}

func TestGrantRoleToRole(t *testing.T) {
	e := NewEngine()
	_ = e.CreateRole("parent")
	_ = e.CreateRole("child")

	if err := e.GrantRoleToRole("child", "parent"); err != nil {
		t.Fatalf("GrantRoleToRole: %v", err)
	}

	// Nonexistent child.
	if err := e.GrantRoleToRole("nonexistent", "parent"); err == nil {
		t.Fatal("expected error for nonexistent child")
	}

	// Nonexistent parent.
	if err := e.GrantRoleToRole("child", "nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent parent")
	}
}

func TestShowGrantsNonexistent(t *testing.T) {
	e := NewEngine()
	grants := e.ShowGrants("nonexistent")
	if grants != nil {
		t.Errorf("expected nil for nonexistent role, got %v", grants)
	}
}

func TestDropRoleCleansUp(t *testing.T) {
	e := NewEngine()
	_ = e.CreateRole("parent")
	_ = e.CreateRole("child")
	_ = e.GrantRoleToRole("child", "parent")
	_ = e.GrantRoleToUser("child", "alice")

	_ = e.DropRole("child")

	// Parent should no longer reference child.
	// User alice should no longer have child role.
	// These are internal; verify by trying to check access.
	_ = e.GrantPrivilege(PrivSelect, ObjTable, "t", "parent", false)
	_ = e.GrantRoleToUser("parent", "alice")
	// Should work without panics.
	_ = e.CheckAccess("alice", "parent", PrivSelect, ObjTable, "T")
}
