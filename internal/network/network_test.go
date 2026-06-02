package network

import (
	"testing"
)

func TestCreatePolicy(t *testing.T) {
	m := NewManager()

	err := m.CreatePolicy("pol1", []string{"10.0.0.0/8"}, nil, "test policy")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Duplicate.
	err = m.CreatePolicy("pol1", []string{"10.0.0.0/8"}, nil, "dup")
	if err == nil {
		t.Fatal("expected error for duplicate policy")
	}

	// Invalid CIDR.
	err = m.CreatePolicy("pol2", []string{"not-a-cidr"}, nil, "")
	if err == nil {
		t.Fatal("expected error for invalid CIDR")
	}

	// Show.
	policies := m.ShowPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].Name != "pol1" {
		t.Fatalf("expected pol1, got %s", policies[0].Name)
	}

	// Describe.
	p, err := m.DescribePolicy("pol1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(p.AllowedIPList) != 1 || p.AllowedIPList[0] != "10.0.0.0/8" {
		t.Fatalf("unexpected allowed list: %v", p.AllowedIPList)
	}

	// Drop.
	err = m.DropPolicy("pol1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.ShowPolicies()) != 0 {
		t.Fatal("expected 0 policies after drop")
	}
}

func TestCheckAccess(t *testing.T) {
	m := NewManager()

	// No active policy — everything is allowed.
	ok, _ := m.CheckAccess("1.2.3.4")
	if !ok {
		t.Fatal("expected allowed with no active policy")
	}

	// Create and activate policy.
	err := m.CreatePolicy("strict", []string{"192.168.1.0/24"}, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = m.SetActivePolicy("strict")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Allowed IP.
	ok, _ = m.CheckAccess("192.168.1.50")
	if !ok {
		t.Fatal("expected 192.168.1.50 to be allowed")
	}

	// Denied IP.
	ok, reason := m.CheckAccess("10.0.0.1")
	if ok {
		t.Fatal("expected 10.0.0.1 to be denied")
	}
	if reason == "" {
		t.Fatal("expected a reason for denial")
	}
}

func TestCIDRMatching(t *testing.T) {
	m := NewManager()

	err := m.CreatePolicy("cidr_test",
		[]string{"10.0.0.0/8", "172.16.0.0/12"},
		nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = m.SetActivePolicy("cidr_test")

	cases := []struct {
		ip      string
		allowed bool
	}{
		{"10.1.2.3", true},
		{"10.255.255.255", true},
		{"172.16.0.1", true},
		{"172.31.255.254", true},
		{"172.32.0.1", false},
		{"192.168.1.1", false},
	}

	for _, tc := range cases {
		ok, _ := m.CheckAccess(tc.ip)
		if ok != tc.allowed {
			t.Errorf("IP %s: expected allowed=%v, got %v", tc.ip, tc.allowed, ok)
		}
	}
}

func TestBlockedIP(t *testing.T) {
	m := NewManager()

	// Allow the /16 but block a specific /24 within it.
	err := m.CreatePolicy("mixed",
		[]string{"10.0.0.0/16"},
		[]string{"10.0.1.0/24"},
		"block a subnet")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = m.SetActivePolicy("mixed")

	// 10.0.0.5 is allowed (in /16, not in blocked /24).
	ok, _ := m.CheckAccess("10.0.0.5")
	if !ok {
		t.Fatal("expected 10.0.0.5 to be allowed")
	}

	// 10.0.1.100 is blocked.
	ok, reason := m.CheckAccess("10.0.1.100")
	if ok {
		t.Fatal("expected 10.0.1.100 to be blocked")
	}
	t.Logf("blocked reason: %s", reason)

	// Cannot drop active policy.
	err = m.DropPolicy("mixed")
	if err == nil {
		t.Fatal("expected error dropping active policy")
	}
}
