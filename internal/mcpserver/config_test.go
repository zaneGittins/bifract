package mcpserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// env sets exactly the variables a case names, so one test's settings cannot leak
// into the next and the defaults are what an unset variable really produces.
func env(t *testing.T, values map[string]string) {
	t.Helper()
	for _, name := range []string{
		"BIFRACT_URL", "BIFRACT_API_KEY", "BIFRACT_CA_CERT", "BIFRACT_CLIENT_CERT",
		"BIFRACT_CLIENT_KEY", "BIFRACT_VERIFY_SSL", "BIFRACT_TIMEOUT",
		"BIFRACT_FRACTAL_ID", "BIFRACT_PRISM_ID",
	} {
		t.Setenv(name, values[name])
	}
}

func validEnv(extra map[string]string) map[string]string {
	values := map[string]string{
		"BIFRACT_URL":     "https://bifract.test",
		"BIFRACT_API_KEY": "bifract_key",
	}
	for k, v := range extra {
		values[k] = v
	}
	return values
}

func TestConfigDefaults(t *testing.T) {
	env(t, validEnv(map[string]string{"BIFRACT_URL": "https://bifract.test/"}))
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.URL != "https://bifract.test" {
		t.Errorf("URL = %q, the trailing slash was not trimmed", cfg.URL)
	}
	if cfg.APIBase() != "https://bifract.test/api/v1" {
		t.Errorf("APIBase = %q", cfg.APIBase())
	}
	if cfg.Timeout != defaultTimeout {
		t.Errorf("Timeout = %v, want %v", cfg.Timeout, defaultTimeout)
	}
	if cfg.TLS.InsecureSkipVerify {
		t.Error("verification is off by default")
	}
	if cfg.TLS.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %#x, want TLS 1.2", cfg.TLS.MinVersion)
	}
}

// A misconfigured client is usually missing more than one thing, and a server
// that dies on the first is a slow way to find that out.
func TestEveryConfigProblemIsReportedAtOnce(t *testing.T) {
	env(t, map[string]string{"BIFRACT_TIMEOUT": "nope"})
	_, err := LoadConfig()
	if err == nil {
		t.Fatal("an empty environment is not a usable configuration")
	}
	for _, want := range []string{"BIFRACT_URL", "BIFRACT_API_KEY", "BIFRACT_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestTheURLSchemeIsRequired(t *testing.T) {
	env(t, validEnv(map[string]string{"BIFRACT_URL": "bifract.test"}))
	_, err := LoadConfig()
	if err == nil || !strings.Contains(err.Error(), "http://") {
		t.Fatalf("got %v, want a complaint about the scheme", err)
	}
}

func TestVerificationCanBeDisabled(t *testing.T) {
	for _, value := range []string{"false", "FALSE", "0", "no", "off"} {
		env(t, validEnv(map[string]string{"BIFRACT_VERIFY_SSL": value}))
		cfg, err := LoadConfig()
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.TLS.InsecureSkipVerify {
			t.Errorf("BIFRACT_VERIFY_SSL=%s did not disable verification", value)
		}
	}
	env(t, validEnv(map[string]string{"BIFRACT_VERIFY_SSL": "true"}))
	cfg, _ := LoadConfig()
	if cfg.TLS.InsecureSkipVerify {
		t.Error("BIFRACT_VERIFY_SSL=true disabled verification")
	}
}

// A CA bundle is an instruction to verify, so it must win over a stale
// BIFRACT_VERIFY_SSL=false rather than being silently combined with it.
func TestACABundleOverridesTheVerifyFlag(t *testing.T) {
	bundle, _, _ := writeCert(t)
	env(t, validEnv(map[string]string{
		"BIFRACT_CA_CERT":    bundle,
		"BIFRACT_VERIFY_SSL": "false",
	}))
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLS.InsecureSkipVerify {
		t.Error("a configured CA bundle left verification disabled")
	}
	if cfg.TLS.RootCAs == nil {
		t.Error("the bundle was not loaded into the trust store")
	}
}

func TestAnUnusableCABundleIsReported(t *testing.T) {
	for _, tc := range []struct{ name, path, want string }{
		{"missing", filepath.Join(t.TempDir(), "absent.pem"), "cannot be read"},
		{"not a certificate", writeFile(t, "junk.pem", "not a certificate"), "no certificates"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env(t, validEnv(map[string]string{"BIFRACT_CA_CERT": tc.path}))
			_, err := LoadConfig()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want a message mentioning %q", err, tc.want)
			}
		})
	}
}

func TestClientCertificates(t *testing.T) {
	certPath, keyPath, combinedPath := writeCert(t)

	t.Run("pair", func(t *testing.T) {
		env(t, validEnv(map[string]string{
			"BIFRACT_CLIENT_CERT": certPath,
			"BIFRACT_CLIENT_KEY":  keyPath,
		}))
		cfg, err := LoadConfig()
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.TLS.Certificates) != 1 {
			t.Error("the client certificate was not loaded")
		}
	})

	// A combined PEM carries its own key, so the key path is optional.
	t.Run("combined PEM", func(t *testing.T) {
		env(t, validEnv(map[string]string{"BIFRACT_CLIENT_CERT": combinedPath}))
		cfg, err := LoadConfig()
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.TLS.Certificates) != 1 {
			t.Error("the combined certificate was not loaded")
		}
	})

	t.Run("certificate without its key", func(t *testing.T) {
		env(t, validEnv(map[string]string{"BIFRACT_CLIENT_CERT": certPath}))
		_, err := LoadConfig()
		if err == nil || !strings.Contains(err.Error(), "BIFRACT_CLIENT_KEY") {
			t.Fatalf("got %v, want a message naming the missing key", err)
		}
	})
}

func TestTimeout(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
		fails bool
	}{
		{"", defaultTimeout, false},
		{"120", 120 * time.Second, false},
		{"0.5", 500 * time.Millisecond, false},
		{"0", defaultTimeout, true},
		{"-5", defaultTimeout, true},
		{"soon", defaultTimeout, true},
	} {
		env(t, validEnv(map[string]string{"BIFRACT_TIMEOUT": tc.value}))
		cfg, err := LoadConfig()
		if tc.fails {
			if err == nil {
				t.Errorf("BIFRACT_TIMEOUT=%q was accepted", tc.value)
			}
			continue
		}
		if err != nil {
			t.Fatalf("BIFRACT_TIMEOUT=%q: %v", tc.value, err)
		}
		if cfg.Timeout != tc.want {
			t.Errorf("BIFRACT_TIMEOUT=%q gave %v, want %v", tc.value, cfg.Timeout, tc.want)
		}
	}
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeCert generates a throwaway self-signed pair, so the tests carry no
// certificate that can expire out from under them.
func writeCert(t *testing.T) (certPath, keyPath, combinedPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "bifract-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	return write("cert.pem", certPEM), write("key.pem", keyPEM), write("combined.pem", certPEM+keyPEM)
}

// A tenant-admin key (bifract_admin_...) belongs to no fractal and names the one
// it means per request, so the session has to be told which.
func TestScopeIsBuiltForAKeyThatCarriesNone(t *testing.T) {
	env(t, validEnv(map[string]string{"BIFRACT_FRACTAL_ID": "588f9ff8-4fe9-484a-9ca8-2ee77260e0b8"}))
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scope != "fractal:588f9ff8-4fe9-484a-9ca8-2ee77260e0b8" {
		t.Errorf("Scope = %q", cfg.Scope)
	}
	if cfg.FractalScope() != "588f9ff8-4fe9-484a-9ca8-2ee77260e0b8" {
		t.Errorf("FractalScope = %q", cfg.FractalScope())
	}

	env(t, validEnv(map[string]string{"BIFRACT_PRISM_ID": "p-1"}))
	cfg, err = LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scope != "prism:p-1" {
		t.Errorf("Scope = %q", cfg.Scope)
	}
	// A prism spans several fractals, so it is not one.
	if cfg.FractalScope() != "" {
		t.Errorf("a prism scope reported a fractal: %q", cfg.FractalScope())
	}
}

func TestNoScopeIsSentWhenTheKeyCarriesItsOwn(t *testing.T) {
	env(t, validEnv(nil))
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scope != "" {
		t.Errorf("Scope = %q, want empty so the key's own scope stands", cfg.Scope)
	}
}

// The server answers a malformed scope with a 400 on every call, which is a
// confusing way to learn about a typo in a config file.
func TestAnUnusableScopeIsReported(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values map[string]string
		want   string
	}{
		{"both", map[string]string{"BIFRACT_FRACTAL_ID": "f-1", "BIFRACT_PRISM_ID": "p-1"}, "not both"},
		{"not an id", map[string]string{"BIFRACT_FRACTAL_ID": "fractal one!"}, "not an id"},
		{"too long", map[string]string{"BIFRACT_PRISM_ID": strings.Repeat("a", 37)}, "not an id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env(t, validEnv(tc.values))
			_, err := LoadConfig()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want a message mentioning %q", err, tc.want)
			}
		})
	}
}
