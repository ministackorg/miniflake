package server

import (
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

// TestGenerateSelfSignedCert guards the properties strict TLS clients (and the
// Apple/macOS system verifier that Go and .NET use) require of the emulator's
// self-signed certificate. A 10-year cert previously shipped here was rejected
// by gosnowflake on macOS as "not standards compliant" — this test is the
// portable, driver-free guard against that regression.
func TestGenerateSelfSignedCert(t *testing.T) {
	certPEM, keyPEM, err := generateSelfSignedCert([]string{"localhost", "127.0.0.1", "::1"})
	if err != nil {
		t.Fatalf("generateSelfSignedCert: %v", err)
	}
	if len(keyPEM) == 0 {
		t.Fatal("empty private key PEM")
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("certificate is not valid PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	// Apple platforms reject TLS server certificates whose validity exceeds
	// 398 days.
	if span := cert.NotAfter.Sub(cert.NotBefore); span > 398*24*time.Hour {
		t.Errorf("validity span %v exceeds the 398-day maximum", span)
	}
	if !cert.IsCA {
		t.Error("certificate is not a CA; it must self-anchor to be trusted from a store")
	}

	dns := map[string]bool{}
	for _, d := range cert.DNSNames {
		dns[d] = true
	}
	if !dns["localhost"] {
		t.Errorf("missing localhost DNS SAN; got DNS=%v", cert.DNSNames)
	}
	if len(cert.IPAddresses) == 0 {
		t.Error("missing IP SANs (127.0.0.1 / ::1)")
	}

	serverAuth := false
	for _, eku := range cert.ExtKeyUsage {
		if eku == x509.ExtKeyUsageServerAuth {
			serverAuth = true
		}
	}
	if !serverAuth {
		t.Error("missing ExtKeyUsageServerAuth")
	}
}
