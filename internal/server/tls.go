package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// tlsHandshakeRecord is the first byte of a TLS ClientHello (TLS record
// content-type 22, "handshake"). A plain HTTP request begins with an ASCII
// method verb (G, P, ...), so the first byte cleanly discriminates the two
// protocols on a shared port.
const tlsHandshakeRecord = 0x16

// tlsPeekTimeout bounds how long we wait for a new connection's first byte
// before dropping it, so a stalled client can't wedge the accept loop.
const tlsPeekTimeout = 10 * time.Second

// EnableTLS lets the server accept TLS connections alongside plain HTTP on the
// same port (see autoListener). Snowflake drivers that mandate HTTPS — notably
// the .NET connector, which has no plain-HTTP mode — can then connect, while
// gosnowflake/Python/JDBC clients using protocol=http keep working unchanged.
//
// If certFile and keyFile are both set, that key pair is used. Otherwise a
// self-signed certificate is generated once and persisted under dataDir
// (miniflake-cert.pem / miniflake-key.pem) so it stays stable across restarts
// and strict clients only need to trust it once.
func (s *Server) EnableTLS(certFile, keyFile, dataDir string) error {
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("load tls key pair: %w", err)
		}
		s.setTLSCert(cert, certFile)
		return nil
	}

	certPath := filepath.Join(dataDir, "miniflake-cert.pem")
	keyPath := filepath.Join(dataDir, "miniflake-key.pem")

	if fileExists(certPath) && fileExists(keyPath) {
		if cert, err := tls.LoadX509KeyPair(certPath, keyPath); err == nil && !certExpired(cert) {
			s.setTLSCert(cert, certPath)
			return nil
		}
		// Fall through and regenerate if the persisted pair is unreadable or
		// has expired.
	}

	hosts := []string{"localhost", "127.0.0.1", "::1"}
	if s.host != "" && s.host != "0.0.0.0" && s.host != "::" {
		hosts = append(hosts, s.host)
	}
	certPEM, keyPEM, err := generateSelfSignedCert(hosts)
	if err != nil {
		return fmt.Errorf("generate self-signed cert: %w", err)
	}
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("parse generated cert: %w", err)
	}
	s.setTLSCert(cert, certPath)
	return nil
}

func (s *Server) setTLSCert(cert tls.Certificate, path string) {
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12, // Snowflake drivers require TLS 1.2+.
	}
	s.certPath = path
}

// CertPath returns the path of the TLS certificate in use, or "" when TLS is
// disabled. Strict clients (e.g. the Snowflake .NET connector) import this file
// into their trust store to accept the emulator's self-signed certificate.
func (s *Server) CertPath() string { return s.certPath }

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// certExpired reports whether the leaf of a loaded key pair is unparseable or
// past its NotAfter, so EnableTLS can regenerate a fresh persisted certificate.
func certExpired(cert tls.Certificate) bool {
	if len(cert.Certificate) == 0 {
		return true
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return true
	}
	return time.Now().After(leaf.NotAfter)
}

// generateSelfSignedCert builds a 10-year, RSA-2048 self-signed server
// certificate valid for the given hosts (DNS names and IP literals). RSA-2048
// is chosen for the broadest driver/runtime compatibility (Go, Java/JDBC,
// .NET, Python).
func generateSelfSignedCert(hosts []string) (certPEM, keyPEM []byte, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"MiniFlake"}, CommonName: "miniflake"},
		NotBefore:    time.Now().Add(-time.Hour), // tolerate small clock skew
		// Apple platforms (macOS/iOS system verifiers, which Go and .NET use)
		// reject TLS server certificates whose validity exceeds 398 days, so
		// keep it just under that. EnableTLS regenerates the persisted cert once
		// it expires.
		NotAfter:    time.Now().AddDate(0, 0, 397),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		// Self-signed and also its own trust anchor, so importing this one file
		// into a trust store (or RootCAs pool) validates the server cert.
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	return certPEM, keyPEM, nil
}

// autoListener serves TLS and plain HTTP on one port. It peeks the first byte
// of each accepted connection: a TLS ClientHello begins with 0x16, so those
// connections are wrapped with tls.Server; everything else is handed to the
// HTTP server as-is. A failed peek drops only that connection rather than
// stopping the accept loop, so a single bad client can't take the listener
// down.
type autoListener struct {
	net.Listener
	tlsConfig *tls.Config
}

func (l *autoListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		_ = conn.SetReadDeadline(time.Now().Add(tlsPeekTimeout))
		first := make([]byte, 1)
		n, rerr := io.ReadFull(conn, first)
		_ = conn.SetReadDeadline(time.Time{})
		if rerr != nil || n == 0 {
			conn.Close()
			continue
		}
		pc := &peekedConn{Conn: conn, prefix: first[:1]}
		if first[0] == tlsHandshakeRecord {
			return tls.Server(pc, l.tlsConfig), nil
		}
		return pc, nil
	}
}

// peekedConn replays the byte consumed while sniffing the protocol, then defers
// to the underlying connection for all subsequent reads.
type peekedConn struct {
	net.Conn
	prefix []byte
}

func (c *peekedConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}
