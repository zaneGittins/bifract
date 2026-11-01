package setup

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// checkHostPorts is swapped in tests so they do not depend on the host's listeners.
var checkHostPorts = checkPortsFree

// composeHostPorts lists the TCP host addresses ("ip:port", ip empty for all
// interfaces) that the services in a compose file publish.
func composeHostPorts(compose string) ([]string, error) {
	var doc struct {
		Services map[string]struct {
			Ports []yaml.Node `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(compose), &doc); err != nil {
		return nil, fmt.Errorf("parse docker-compose: %w", err)
	}
	var addrs []string
	names := make([]string, 0, len(doc.Services))
	for name := range doc.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, n := range doc.Services[name].Ports {
			if addr := publishedAddr(n); addr != "" {
				addrs = append(addrs, addr)
			}
		}
	}
	return addrs, nil
}

// publishedAddr reads one compose port entry, short ("ip:host:container/proto")
// or long form, returning "" when it publishes no fixed TCP host port.
func publishedAddr(n yaml.Node) string {
	if n.Kind == yaml.MappingNode {
		var p struct {
			HostIP    string `yaml:"host_ip"`
			Published string `yaml:"published"`
			Protocol  string `yaml:"protocol"`
		}
		if n.Decode(&p) != nil || p.Published == "" || (p.Protocol != "" && p.Protocol != "tcp") {
			return ""
		}
		return net.JoinHostPort(p.HostIP, p.Published)
	}
	spec, proto, _ := strings.Cut(n.Value, "/")
	if proto != "" && proto != "tcp" {
		return ""
	}
	parts := strings.Split(spec, ":")
	switch len(parts) {
	case 2:
		return net.JoinHostPort("", parts[0])
	case 3:
		return net.JoinHostPort(parts[0], parts[1])
	}
	return ""
}

// checkPortsFree reports every address something is already listening on.
func checkPortsFree(addrs []string) error {
	var busy []string
	for _, addr := range addrs {
		if portInUse(addr) {
			_, port, _ := net.SplitHostPort(addr)
			busy = append(busy, port)
		}
	}
	switch len(busy) {
	case 0:
		return nil
	case 1:
		return fmt.Errorf("port %s is already in use", busy[0])
	}
	return fmt.Errorf("ports %s are already in use", strings.Join(busy, ", "))
}

// portInUse tries to bind addr. Without the privilege to bind a low port it falls
// back to dialing loopback, so a non-root installer still sees a running server.
func portInUse(addr string) bool {
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		ln.Close()
		return false
	}
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	host, port, _ := net.SplitHostPort(addr)
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	conn, dialErr := net.DialTimeout("tcp", net.JoinHostPort(host, port), time.Second)
	if dialErr != nil {
		return false
	}
	conn.Close()
	return true
}
