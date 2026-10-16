package setup

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestClickHouseTargetNormalize(t *testing.T) {
	tests := []struct {
		name string
		in   ClickHouseTarget
		want ClickHouseTarget
	}{
		{
			// A reconfigure must not carry stale external settings into a bundled
			// install, whose ClickHouse is plain TCP on the compose network.
			name: "bundled is pinned to the shipped manifests",
			in:   ClickHouseTarget{Deployment: "cloud", Secure: true, CACertPath: "/ca.pem", FanoutCluster: "f", Port: 9440},
			want: ClickHouseTarget{Database: "logs", User: "default", Port: 9000},
		},
		{
			name: "external single node",
			in:   ClickHouseTarget{Backend: CHBackendExternal, Host: "ch"},
			want: ClickHouseTarget{Backend: CHBackendExternal, Deployment: "single-node", Host: "ch", Port: 9000, Database: "logs", User: "default"},
		},
		{
			name: "hosts and a cluster mean self-managed",
			in:   ClickHouseTarget{Backend: CHBackendExternal, Hosts: "a,b", Cluster: "c"},
			want: ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cluster", Hosts: "a,b", Cluster: "c", Port: 9000, Database: "logs", User: "default"},
		},
		{
			name: "cloud forces TLS and its port",
			in:   ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cloud", Host: "x.clickhouse.cloud"},
			want: ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cloud", Host: "x.clickhouse.cloud", Port: 9440, Secure: true, Database: "logs", User: "default"},
		},
		{
			name: "an explicit port wins",
			in:   ClickHouseTarget{Backend: CHBackendExternal, Host: "ch", Port: 19000, Secure: true, Database: "db", User: "u"},
			want: ClickHouseTarget{Backend: CHBackendExternal, Deployment: "single-node", Host: "ch", Port: 19000, Secure: true, Database: "db", User: "u"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in
			got.Normalize()
			if got != tt.want {
				t.Errorf("Normalize() = %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

// PersistEnv and TargetFromEnv are how reconfigure and upgrade remember an
// external ClickHouse. Losing any field would silently point the install back at
// a bundled server it does not have, or at the wrong port or TLS mode.
func TestClickHouseTargetPersistRoundTrip(t *testing.T) {
	targets := []ClickHouseTarget{
		{Backend: CHBackendExternal, Host: "ch.internal", Port: 9000},
		{Backend: CHBackendExternal, Deployment: "cloud", Host: "abc.clickhouse.cloud"},
		{Backend: CHBackendExternal, Hosts: "a:9000,b:9000", Cluster: "logs_cluster", FanoutCluster: "all",
			Database: "bifract", User: "admin", Secure: true, CACertPath: "/etc/ca.pem", Port: 9440},
	}
	for _, in := range targets {
		in.Normalize()
		env := map[string]string{}
		for _, v := range in.PersistEnv() {
			env[v.Name] = v.Value
		}
		if env["BIFRACT_CH_BACKEND"] != "external" {
			t.Errorf("%+v: no backend marker persisted", in)
		}
		if got := TargetFromEnv(env); got != in {
			t.Errorf("round trip:\n got  %+v\n want %+v", got, in)
		}
	}

	var bundled ClickHouseTarget
	if got := bundled.PersistEnv(); got != nil {
		t.Errorf("bundled target persisted %v; its shape lives in the compose file", got)
	}
	// An install from before the field existed has no marker: bundled.
	got := TargetFromEnv(map[string]string{"CLICKHOUSE_HOST": "clickhouse", "CLICKHOUSE_PORT": "9000"})
	if !got.Bundled() || got.Host != "" {
		t.Errorf("unmarked env read as %+v, want the bundled target", got)
	}
}

func TestClickHouseTargetSummary(t *testing.T) {
	tests := []struct {
		in   ClickHouseTarget
		want string
	}{
		{ClickHouseTarget{}, "bundled (managed by this installer)"},
		{ClickHouseTarget{Backend: CHBackendExternal, Deployment: "single-node", Host: "ch", Port: 9000}, "single-node ch:9000"},
		{ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cloud", Host: "x.cloud", Port: 9440, Secure: true}, "cloud x.cloud:9440 (TLS)"},
		{ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cluster", Host: "ignored", Hosts: "a,b", Port: 9000}, "cluster a,b:9000"},
	}
	for _, tt := range tests {
		if got := tt.in.Summary(); got != tt.want {
			t.Errorf("Summary(%+v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestTLSServerNameFor(t *testing.T) {
	var target ClickHouseTarget
	for in, want := range map[string]string{
		"abc.clickhouse.cloud":      "abc.clickhouse.cloud",
		"abc.clickhouse.cloud:9440": "abc.clickhouse.cloud",
		"10.0.0.1:9440":             "10.0.0.1",
	} {
		if got := target.TLSServerNameFor(in); got != want {
			t.Errorf("TLSServerNameFor(%q) = %q, want %q", in, got, want)
		}
	}
}

// loopbackTarget points an external target at a listener in this process.
func loopbackTarget(t *testing.T, addr string, secure bool) ClickHouseTarget {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	return ClickHouseTarget{Backend: CHBackendExternal, Host: host, Port: p, Secure: secure}
}

// The wizard's reachability probe, against listeners in this process only.
func TestCheckClickHouseReachable(t *testing.T) {
	if err := CheckClickHouseReachable(ClickHouseTarget{}); err != nil {
		t.Errorf("bundled target probed: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener: %v", err)
	}
	open := ln.Addr().String()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	t.Cleanup(func() { ln.Close() })

	if err := CheckClickHouseReachable(loopbackTarget(t, open, false)); err != nil {
		t.Errorf("open port reported unreachable: %v", err)
	}

	// The first of several hosts is the one probed.
	multi := loopbackTarget(t, open, false)
	multi.Host, multi.Hosts = "", open+",192.0.2.1:9000"
	if err := CheckClickHouseReachable(multi); err != nil {
		t.Errorf("hosts list probe: %v", err)
	}

	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := closed.Addr().String()
	closed.Close()
	if err := CheckClickHouseReachable(loopbackTarget(t, closedAddr, false)); err == nil ||
		!strings.Contains(err.Error(), "cannot reach ClickHouse") {
		t.Errorf("closed port: %v", err)
	}

	// TLS is verified, not just negotiated: a certificate nobody vouches for is
	// exactly the mistake (or interception) this probe exists to surface.
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	secure := loopbackTarget(t, srv.Listener.Addr().String(), true)
	if err := CheckClickHouseReachable(secure); err == nil || !strings.Contains(err.Error(), "TLS handshake") {
		t.Errorf("untrusted certificate accepted: %v", err)
	}
	// A TLS target on a plain-TCP port fails rather than passing on the dial alone.
	if err := CheckClickHouseReachable(loopbackTarget(t, open, true)); err == nil {
		t.Error("TLS probe succeeded against a plain TCP port")
	}
}
