package setup

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"software.sslmate.com/src/go-pkcs12"
)

func readCertPEM(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return parseCertPEM(t, data)
}

func parseCertPEM(t *testing.T, data []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("not a PEM certificate: %q", data)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != want {
		t.Errorf("%s mode = %o, want %o", filepath.Base(path), got, want)
	}
}

func TestGenerateSelfSignedCert(t *testing.T) {
	tests := []struct {
		domain   string
		wantDNS  []string
		wantIPs  []string
		verifyAs string
	}{
		{"bifract.example.com", []string{"bifract.example.com", "localhost"}, []string{"127.0.0.1"}, "bifract.example.com"},
		{"localhost", []string{"localhost"}, []string{"127.0.0.1"}, "localhost"},
		{"10.1.2.3", []string{"localhost"}, []string{"10.1.2.3", "127.0.0.1"}, "10.1.2.3"},
		// The loopback is not listed twice when it is the domain.
		{"127.0.0.1", []string{"localhost"}, []string{"127.0.0.1"}, "127.0.0.1"},
	}
	for _, tt := range tests {
		t.Run(tt.domain, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "certs")
			if err := GenerateSelfSignedCert(tt.domain, dir); err != nil {
				t.Fatal(err)
			}
			cert := readCertPEM(t, filepath.Join(dir, "cert.pem"))

			if strings.Join(cert.DNSNames, ",") != strings.Join(tt.wantDNS, ",") {
				t.Errorf("DNS SANs = %v, want %v", cert.DNSNames, tt.wantDNS)
			}
			var ips []string
			for _, ip := range cert.IPAddresses {
				ips = append(ips, ip.String())
			}
			if strings.Join(ips, ",") != strings.Join(tt.wantIPs, ",") {
				t.Errorf("IP SANs = %v, want %v", ips, tt.wantIPs)
			}

			// It must actually serve TLS for the domain: self-signed, server auth.
			pool := x509.NewCertPool()
			pool.AddCert(cert)
			if _, err := cert.Verify(x509.VerifyOptions{
				DNSName:   tt.verifyAs,
				Roots:     pool,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}); err != nil {
				t.Errorf("certificate does not verify for %s: %v", tt.verifyAs, err)
			}
			if cert.NotAfter.Before(time.Now().Add(9 * 365 * 24 * time.Hour)) {
				t.Errorf("expires %s; a self-signed install cert is meant to last ~10 years", cert.NotAfter)
			}

			keyData, err := os.ReadFile(filepath.Join(dir, "key.pem"))
			if err != nil {
				t.Fatal(err)
			}
			block, _ := pem.Decode(keyData)
			key, err := x509.ParseECPrivateKey(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			if !key.PublicKey.Equal(cert.PublicKey) {
				t.Error("key.pem does not match cert.pem")
			}
			assertMode(t, filepath.Join(dir, "key.pem"), 0600)
		})
	}
}

// Upgrades call this on every run; replacing a cert the browser already trusts,
// or an operator's own, would be a regression.
func TestGenerateSelfSignedCertKeepsExisting(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"cert.pem", "key.pem"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("existing "+f), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := GenerateSelfSignedCert("example.com", dir); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"cert.pem", "key.pem"} {
		if b, _ := os.ReadFile(filepath.Join(dir, f)); string(b) != "existing "+f {
			t.Errorf("%s was replaced", f)
		}
	}

	// Only one half present: regenerate both rather than pairing a new key with an old cert.
	if err := os.Remove(filepath.Join(dir, "key.pem")); err != nil {
		t.Fatal(err)
	}
	if err := GenerateSelfSignedCert("example.com", dir); err != nil {
		t.Fatal(err)
	}
	readCertPEM(t, filepath.Join(dir, "cert.pem"))
}

func TestGenerateClientCA(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "client-ca")
	if err := GenerateClientCA(dir); err != nil {
		t.Fatal(err)
	}
	ca := readCertPEM(t, filepath.Join(dir, "ca.pem"))
	if !ca.IsCA || !ca.BasicConstraintsValid {
		t.Error("client CA is not marked as a CA")
	}
	if ca.MaxPathLen != 0 || !ca.MaxPathLenZero {
		t.Error("client CA may sign intermediates; it should only sign leaf client certs")
	}
	if ca.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Error("client CA lacks the cert-sign key usage")
	}
	assertMode(t, filepath.Join(dir, "ca-key.pem"), 0600)
	assertMode(t, dir, 0700)

	before, _ := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err := GenerateClientCA(dir); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if string(before) != string(after) {
		t.Error("a second call replaced the CA, invalidating every client cert it signed")
	}
}

// decodeClientP12 opens a bundle and verifies the leaf chains to the CA for
// client authentication, which is what Caddy checks.
func decodeClientP12(t *testing.T, p12 []byte, password string, caPEM []byte) *x509.Certificate {
	t.Helper()
	key, leaf, chain, err := pkcs12.DecodeChain(p12, password)
	if err != nil {
		t.Fatalf("decode .p12: %v", err)
	}
	ca := parseCertPEM(t, caPEM)
	if len(chain) != 1 || !chain[0].Equal(ca) {
		t.Errorf("bundle chain = %d certs, want exactly the CA", len(chain))
	}
	ecKey, ok := key.(*ecdsa.PrivateKey)
	if !ok || !ecKey.PublicKey.Equal(leaf.PublicKey) {
		t.Error("bundled key does not match the bundled certificate")
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Errorf("client cert does not verify against the CA for client auth: %v", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err == nil {
		t.Error("client cert is also valid for server auth")
	}
	if leaf.IsCA {
		t.Error("client cert is a CA")
	}
	return leaf
}

func TestGenerateClientCertBytes(t *testing.T) {
	caPEM, keyPEM, err := GenerateClientCAPEM()
	if err != nil {
		t.Fatal(err)
	}
	p12, err := GenerateClientCertBytes([]byte(caPEM), []byte(keyPEM), "analyst@example.com", "s3cret!")
	if err != nil {
		t.Fatal(err)
	}
	leaf := decodeClientP12(t, p12, "s3cret!", []byte(caPEM))
	if leaf.Subject.CommonName != "analyst@example.com" {
		t.Errorf("CN = %q", leaf.Subject.CommonName)
	}
	if d := time.Until(leaf.NotAfter); d < 364*24*time.Hour || d > 366*24*time.Hour {
		t.Errorf("client cert valid for %s, want one year", d)
	}
	if _, _, _, err := pkcs12.DecodeChain(p12, "wrong"); err == nil {
		t.Error("bundle opened with the wrong password")
	}

	t.Run("rejects bad CA material", func(t *testing.T) {
		_, otherKey, err := GenerateClientCAPEM()
		if err != nil {
			t.Fatal(err)
		}
		cases := map[string][2]string{
			"cert not PEM":   {"garbage", keyPEM},
			"key not PEM":    {caPEM, "garbage"},
			"key not EC":     {caPEM, "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n"},
			"cert not x509":  {"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n", keyPEM},
			"key of another": {caPEM, otherKey},
		}
		for name, c := range cases {
			if _, err := GenerateClientCertBytes([]byte(c[0]), []byte(c[1]), "x", "pw"); err == nil {
				t.Errorf("%s: accepted", name)
			}
		}
	})
}

func TestGenerateClientCertFromDisk(t *testing.T) {
	caDir := filepath.Join(t.TempDir(), "client-ca")
	if err := GenerateClientCA(caDir); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "nested", "admin.p12")
	if err := GenerateClientCert(caDir, "admin", "pw123", out); err != nil {
		t.Fatal(err)
	}
	p12, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, _ := os.ReadFile(filepath.Join(caDir, "ca.pem"))
	decodeClientP12(t, p12, "pw123", caPEM)
	assertMode(t, out, 0600)

	if err := GenerateClientCert(t.TempDir(), "admin", "pw", out); err == nil {
		t.Error("generated a client cert without a CA")
	}
}

func TestRunGenClientCert(t *testing.T) {
	t.Run("docker layout", func(t *testing.T) {
		dir := t.TempDir()
		if err := GenerateClientCA(filepath.Join(dir, "caddy", "client-ca")); err != nil {
			t.Fatal(err)
		}
		var err error
		captureStdout(t, func() { err = RunGenClientCert(dir, "bob", "pw") })
		if err != nil {
			t.Fatal(err)
		}
		if !fileExists(filepath.Join(dir, "caddy", "client-ca", "bob.p12")) {
			t.Error("bundle not written next to the docker CA")
		}
	})
	t.Run("k8s layout", func(t *testing.T) {
		dir := t.TempDir()
		if err := GenerateClientCA(filepath.Join(dir, "client-ca")); err != nil {
			t.Fatal(err)
		}
		var err error
		captureStdout(t, func() { err = RunGenClientCert(dir, "bob", "pw") })
		if err != nil {
			t.Fatal(err)
		}
		if !fileExists(filepath.Join(dir, "client-ca", "bob.p12")) {
			t.Error("bundle not written next to the k8s CA")
		}
	})
	t.Run("no CA", func(t *testing.T) {
		var err error
		captureStdout(t, func() { err = RunGenClientCert(t.TempDir(), "bob", "pw") })
		if err == nil || !strings.Contains(err.Error(), "client CA not found") {
			t.Errorf("err = %v", err)
		}
	})
}

func TestCopyCertsToDir(t *testing.T) {
	src := t.TempDir()
	certSrc, keySrc := filepath.Join(src, "c.pem"), filepath.Join(src, "k.pem")
	os.WriteFile(certSrc, []byte("CERT"), 0644)
	os.WriteFile(keySrc, []byte("KEY"), 0644)

	dst := filepath.Join(t.TempDir(), "certs")
	if err := copyCertsToDir(certSrc, keySrc, dst); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "cert.pem")); string(b) != "CERT" {
		t.Errorf("cert.pem = %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "key.pem")); string(b) != "KEY" {
		t.Errorf("key.pem = %q", b)
	}
	// The source key may be world-readable; the copy Caddy mounts must not be.
	assertMode(t, filepath.Join(dst, "key.pem"), 0600)

	if err := copyCertsToDir(filepath.Join(src, "missing.pem"), keySrc, dst); err == nil {
		t.Error("copied a missing certificate")
	}
	if err := copyCertsToDir(certSrc, filepath.Join(src, "missing.pem"), dst); err == nil {
		t.Error("copied a missing key")
	}
}
