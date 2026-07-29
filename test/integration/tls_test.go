//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"database/sql"
	"net/http"
	"testing"

	sf "github.com/snowflakedb/gosnowflake"
)

// TestTLSConnectionHTTPS drives the real gosnowflake driver over HTTPS against
// the same port the plain-HTTP tests use, exercising the auto-detect TLS
// listener end-to-end: TLS handshake + login + a query.
//
// The transport uses InsecureSkipVerify because trusting the emulator's
// self-signed certificate is the user's responsibility (documented: import
// <data-dir>/miniflake-cert.pem into your trust store). This test's job is the
// transport round-trip; cert correctness/compliance is asserted separately by
// the unit test in internal/server.
func TestTLSConnectionHTTPS(t *testing.T) {
	cfg := &sf.Config{
		Account:   "miniflake",
		User:      "test",
		Password:  "test",
		Host:      "127.0.0.1",
		Port:      testPort,
		Protocol:  "https",
		Database:  "TESTDB",
		Schema:    "PUBLIC",
		Warehouse: "COMPUTE_WH",
		Transporter: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed emulator cert
		},
	}
	// Use the Config object (not a DSN string): Transporter is a runtime
	// http.RoundTripper and can't be serialized into a DSN, so sf.DSN() would
	// silently drop it and fall back to the default transport.
	db := sql.OpenDB(sf.NewConnector(sf.SnowflakeDriver{}, *cfg))
	defer db.Close()

	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&n); err != nil {
		t.Fatalf("SELECT 1 over https failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("SELECT 1 over https = %d, want 1", n)
	}
}
