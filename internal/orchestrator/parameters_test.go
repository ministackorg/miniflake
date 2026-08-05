package orchestrator

import (
	"testing"
)

func TestHandleShowParameters(t *testing.T) {
	orch, _, cleanup := testOrchestrator(t)
	defer cleanup()

	result, handled, err := orch.handleShowParameters("", "SESSION")
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("expected handled")
	}
	wantCols := []string{"key", "value", "default", "level", "description", "type"}
	if len(result.Columns) != len(wantCols) {
		t.Fatalf("columns=%v", result.Columns)
	}
	for i, c := range wantCols {
		if result.Columns[i] != c {
			t.Errorf("col[%d]=%q want %q", i, result.Columns[i], c)
		}
	}
	if len(result.Rows) < 10 {
		t.Fatalf("expected a useful default catalog, got %d rows", len(result.Rows))
	}

	foundTZ := false
	for _, row := range result.Rows {
		key, _ := row[0].(string)
		if key == "TIMEZONE" {
			foundTZ = true
			if row[1] != "America/Los_Angeles" {
				t.Errorf("TIMEZONE value=%v", row[1])
			}
			if row[2] != "America/Los_Angeles" {
				t.Errorf("TIMEZONE default=%v", row[2])
			}
			if row[5] != "STRING" {
				t.Errorf("TIMEZONE type=%v want STRING", row[5])
			}
		}
		if key == "STATEMENT_TIMEOUT_IN_SECONDS" {
			if row[1] != "172800" || row[2] != "172800" {
				t.Errorf("STATEMENT_TIMEOUT_IN_SECONDS value/default=%v/%v want 172800", row[1], row[2])
			}
		}
	}
	if !foundTZ {
		t.Error("TIMEZONE missing from SHOW PARAMETERS")
	}
}

func TestHandleShowParametersLike(t *testing.T) {
	orch, _, cleanup := testOrchestrator(t)
	defer cleanup()

	result, _, err := orch.handleShowParameters("%TIME%", "SESSION")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("expected rows for LIKE pattern containing TIME")
	}
	for _, row := range result.Rows {
		key := row[0].(string)
		if !matchLike("%TIME%", key) {
			t.Errorf("row %q does not match LIKE %%TIME%%", key)
		}
	}
}

func TestHandleShowParametersInAccount(t *testing.T) {
	orch, _, cleanup := testOrchestrator(t)
	defer cleanup()

	sessionRows, _, err := orch.handleShowParameters("", "SESSION")
	if err != nil {
		t.Fatal(err)
	}
	accountRows, _, err := orch.handleShowParameters("", "ACCOUNT")
	if err != nil {
		t.Fatal(err)
	}
	if len(accountRows.Rows) <= len(sessionRows.Rows) {
		t.Fatalf("IN ACCOUNT should include account params; session=%d account=%d",
			len(sessionRows.Rows), len(accountRows.Rows))
	}
}

func TestMatchLike(t *testing.T) {
	cases := []struct {
		pat, val string
		want     bool
	}{
		{"TIMEZONE", "TIMEZONE", true},
		{"timezone", "TIMEZONE", true},
		{"TIME%", "TIMEZONE", true},
		{"%ZONE", "TIMEZONE", true},
		{"%TIME%", "TIMESTAMP_OUTPUT_FORMAT", true},
		{"AUTOCOMMIT", "TIMEZONE", false},
		{"T_MEZONE", "TIMEZONE", true},
	}
	for _, c := range cases {
		if got := matchLike(c.pat, c.val); got != c.want {
			t.Errorf("matchLike(%q,%q)=%v want %v", c.pat, c.val, got, c.want)
		}
	}
}
