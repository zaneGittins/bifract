package storage

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
)

// TestTLSDisabledYieldsNilConfig is the regression proof for docker and k8s.
// Both render plaintext connections on port 9000, and the driver must receive a
// nil TLS config for those, not a disabled one: a non-nil *tls.Config makes
// clickhouse-go negotiate TLS and the handshake fails against a plaintext port.
func TestTLSDisabledYieldsNilConfig(t *testing.T) {
	cfg, err := TLSConfig{}.tlsConfig()
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if cfg != nil {
		t.Fatalf("tlsConfig = %+v, want nil for the zero value", cfg)
	}

	// Fields set but Enabled false must still yield nil, so a half-filled config
	// cannot silently turn a plaintext deployment into a TLS one.
	cfg, err = TLSConfig{ServerName: "example", CACertPath: "/nonexistent"}.tlsConfig()
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if cfg != nil {
		t.Fatalf("tlsConfig = %+v, want nil when Enabled is false", cfg)
	}
}

func TestTLSEnabled(t *testing.T) {
	cfg, err := TLSConfig{Enabled: true, ServerName: "ch.example.com"}.tlsConfig()
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if cfg == nil {
		t.Fatal("tlsConfig = nil, want a config")
	}
	if cfg.ServerName != "ch.example.com" {
		t.Errorf("ServerName = %q, want ch.example.com", cfg.ServerName)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %d, want TLS 1.2", cfg.MinVersion)
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = true, want false unless explicitly set")
	}
	if cfg.RootCAs != nil {
		t.Error("RootCAs set, want system roots when no CA path is given")
	}
}

func TestTLSCACertErrors(t *testing.T) {
	if _, err := (TLSConfig{Enabled: true, CACertPath: "/nonexistent/ca.pem"}).tlsConfig(); err == nil {
		t.Error("missing CA file accepted, want an error")
	}

	junk := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(junk, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (TLSConfig{Enabled: true, CACertPath: junk}).tlsConfig(); err == nil {
		t.Error("unparseable CA file accepted, want an error")
	}
}

func TestHostAddrs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hosts []string
		port  int
		want  []string
	}{
		{"appends the port", []string{"ch"}, 9000, []string{"ch:9000"}},
		{"keeps an explicit port", []string{"ch:9440"}, 9000, []string{"ch:9440"}},
		{"trims whitespace", []string{" a ", "b "}, 9000, []string{"a:9000", "b:9000"}},
		{"drops empties", []string{"a", "", "  "}, 9000, []string{"a:9000"}},
		{"mixed", []string{"a", "b:9001"}, 9000, []string{"a:9000", "b:9001"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := HostAddrs(tc.hosts, tc.port)
			if len(got) != len(tc.want) {
				t.Fatalf("HostAddrs = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("HostAddrs = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestSingleNodeOptionsDefaults pins what docker installs get: plaintext, the
// default query pool, and a control-plane role.
func TestSingleNodeOptionsDefaults(t *testing.T) {
	opts := SingleNodeOptions("clickhouse", 9000, "logs", "default", "pw")
	if len(opts.Conn.Addrs) != 1 || opts.Conn.Addrs[0] != "clickhouse:9000" {
		t.Errorf("Addrs = %v, want [clickhouse:9000]", opts.Conn.Addrs)
	}
	if opts.Conn.TLS.Enabled {
		t.Error("TLS enabled by default, want plaintext")
	}
	if opts.Topo.Kind != DeploymentSingleNode {
		t.Errorf("Kind = %q, want %q", opts.Topo.Kind, DeploymentSingleNode)
	}
	if opts.Role != RoleControlPlane {
		t.Errorf("Role = %v, want RoleControlPlane", opts.Role)
	}
	if opts.Conn.Pool != DefaultQueryPoolConfig() {
		t.Errorf("Pool = %+v, want the default query pool", opts.Conn.Pool)
	}
}
