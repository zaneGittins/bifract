package setup

import (
	"crypto/tls"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"
)

type PrereqResult struct {
	DockerOK   bool
	ComposeOK  bool
	DockerVer  string
	ComposeVer string
	Errors     []string
}

func CheckPrereqs() PrereqResult {
	r := PrereqResult{}

	if out, err := exec.Command("docker", "--version").Output(); err == nil {
		r.DockerOK = true
		r.DockerVer = strings.TrimSpace(string(out))
	} else {
		r.Errors = append(r.Errors, "Docker not found. Install: https://docs.docker.com/get-docker/")
	}

	if out, err := exec.Command("docker", "compose", "version").Output(); err == nil {
		r.ComposeOK = true
		r.ComposeVer = strings.TrimSpace(string(out))
	} else {
		r.Errors = append(r.Errors, "Docker Compose not found. Install: https://docs.docker.com/compose/install/")
	}

	return r
}

func (r PrereqResult) OK() bool {
	return r.DockerOK && r.ComposeOK
}

func (r PrereqResult) Summary() string {
	var lines []string
	if r.DockerOK {
		lines = append(lines, fmt.Sprintf("  Docker:  %s", r.DockerVer))
	}
	if r.ComposeOK {
		lines = append(lines, fmt.Sprintf("  Compose: %s", r.ComposeVer))
	}
	for _, e := range r.Errors {
		lines = append(lines, fmt.Sprintf("  [!] %s", e))
	}
	return strings.Join(lines, "\n")
}

// CheckClickHouseReachable dials external ClickHouse before any files are
// written, so a wrong host, a closed port or a TLS mismatch is reported while the
// operator is still in the wizard rather than after the containers come up.
//
// This is a reachability check, not an authentication one: the credentials are
// the app's to prove, and the endpoint being wrong is the mistake worth catching
// here. No-op for a bundled ClickHouse, which does not exist yet at this point.
func CheckClickHouseReachable(t ClickHouseTarget) error {
	if t.Bundled() {
		return nil
	}
	t.Normalize()

	host := t.Host
	if t.Hosts != "" {
		if hosts := splitCSV(t.Hosts); len(hosts) > 0 {
			host = hosts[0]
		}
	}
	if host == "" {
		return fmt.Errorf("no ClickHouse host configured")
	}
	addr := host
	if !strings.Contains(addr, ":") {
		addr = fmt.Sprintf("%s:%d", host, t.Port)
	}

	const timeout = 5 * time.Second
	if !t.Secure {
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			return fmt.Errorf("cannot reach ClickHouse at %s: %w", addr, err)
		}
		return conn.Close()
	}

	serverName := t.TLSServerNameFor(host)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	})
	if err != nil {
		return fmt.Errorf("TLS handshake with ClickHouse at %s failed: %w", addr, err)
	}
	return conn.Close()
}
