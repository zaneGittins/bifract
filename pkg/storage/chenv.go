package storage

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ClickHouseEnv is the single definition of the CLICKHOUSE_* environment
// contract. The server, the ingest tier and the archiver all read it through
// here, so a variable cannot mean one thing in one binary and something else in
// another.
//
// A misconfigured storage backend is not a degraded mode: every error returned
// from this file is fatal at startup, names the offending variable, and says
// what to do about it.
type ClickHouseEnv struct {
	Deployment DeploymentKind

	Host      string
	Port      int
	Hosts     []string
	WriteHost string

	Database string
	User     string
	Password string

	Cluster       string
	FanoutCluster string

	Secure        bool
	CACertPath    string
	TLSServerName string
	TLSInsecure   bool
}

// Default ports for the ClickHouse native protocol.
const (
	defaultNativePort       = 9000
	defaultNativeSecurePort = 9440
)

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// envBool reports the value and whether the variable was set, so a default that
// depends on the deployment can tell "unset" from "explicitly false".
func envBool(key string) (val, ok bool) {
	v, present := os.LookupEnv(key)
	if !present || v == "" {
		return false, false
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true, true
	default:
		return false, true
	}
}

func envInt(key string) (val int, ok bool, err error) {
	v, present := os.LookupEnv(key)
	if !present || v == "" {
		return 0, false, nil
	}
	n, convErr := strconv.Atoi(strings.TrimSpace(v))
	if convErr != nil {
		return 0, true, fmt.Errorf("%s=%q is not a number", key, v)
	}
	return n, true, nil
}

// splitHosts parses a comma-separated host list, dropping empty entries.
func splitHosts(v string) []string {
	var out []string
	for _, h := range strings.Split(v, ",") {
		if h = strings.TrimSpace(h); h != "" {
			out = append(out, h)
		}
	}
	return out
}

// ClickHouseEnvFromOS reads and validates the ClickHouse configuration.
func ClickHouseEnvFromOS() (ClickHouseEnv, error) {
	e := ClickHouseEnv{
		Host:          envStr("CLICKHOUSE_HOST", "localhost"),
		Hosts:         splitHosts(os.Getenv("CLICKHOUSE_HOSTS")),
		WriteHost:     envStr("CLICKHOUSE_WRITE_HOST", ""),
		Database:      envStr("CLICKHOUSE_DB", "logs"),
		User:          envStr("CLICKHOUSE_USER", "default"),
		Password:      os.Getenv("CLICKHOUSE_PASSWORD"),
		Cluster:       envStr("CLICKHOUSE_CLUSTER", ""),
		FanoutCluster: envStr("CLICKHOUSE_FANOUT_CLUSTER", ""),
		CACertPath:    envStr("CLICKHOUSE_CA_CERT", ""),
		TLSServerName: envStr("CLICKHOUSE_TLS_SERVER_NAME", ""),
	}

	kind, err := resolveDeployment(os.Getenv("CLICKHOUSE_DEPLOYMENT"), e.Hosts, e.Cluster)
	if err != nil {
		return ClickHouseEnv{}, err
	}
	e.Deployment = kind

	// Cloud is TLS-only, so secure defaults on there. Everywhere else the default
	// is plaintext, which is what a bundled ClickHouse on the same network uses.
	secure, secureSet := envBool("CLICKHOUSE_SECURE")
	e.Secure = secure
	if !secureSet {
		e.Secure = kind == DeploymentCloud
	}
	e.TLSInsecure, _ = envBool("CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY")

	// The port default follows the transport, so a Cloud service needs only
	// CLICKHOUSE_DEPLOYMENT, CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD.
	port, portSet, err := envInt("CLICKHOUSE_PORT")
	if err != nil {
		return ClickHouseEnv{}, err
	}
	e.Port = port
	if !portSet {
		e.Port = defaultNativePort
		if e.Secure {
			e.Port = defaultNativeSecurePort
		}
	}

	if err := e.validate(); err != nil {
		return ClickHouseEnv{}, err
	}
	return e, nil
}

// resolveDeployment picks the deployment kind, deriving it from the cluster
// variables when CLICKHOUSE_DEPLOYMENT is unset so deployments predating that
// variable keep working.
//
// The kind is never inferred from a cluster name alone. ClickHouse Cloud exposes
// a cluster named "default" for its table functions, and reading that as a
// self-managed cluster name would wrongly enable Distributed tables, engine
// rewriting and ON CLUSTER against a server that has none of them.
func resolveDeployment(declared string, hosts []string, cluster string) (DeploymentKind, error) {
	if declared != "" {
		k := DeploymentKind(strings.ToLower(strings.TrimSpace(declared)))
		switch k {
		case DeploymentSingleNode, DeploymentSelfManagedCluster, DeploymentCloud:
			return k, nil
		}
		return "", fmt.Errorf("CLICKHOUSE_DEPLOYMENT=%q is not valid: want %s, %s or %s",
			declared, DeploymentSingleNode, DeploymentSelfManagedCluster, DeploymentCloud)
	}

	switch {
	case len(hosts) == 0 && cluster == "":
		return DeploymentSingleNode, nil
	case len(hosts) > 0 && cluster != "":
		return DeploymentSelfManagedCluster, nil
	case cluster == "":
		return "", fmt.Errorf("CLICKHOUSE_HOSTS is set but CLICKHOUSE_CLUSTER is not: " +
			"a sharded deployment needs both, and half of one silently runs as a single node against one shard")
	default:
		return "", fmt.Errorf("CLICKHOUSE_CLUSTER is set but CLICKHOUSE_HOSTS is not: " +
			"a sharded deployment needs both, and half of one silently runs as a single node against one shard")
	}
}

func (e ClickHouseEnv) validate() error {
	switch e.Deployment {
	case DeploymentSingleNode:
		if len(e.Hosts) > 0 || e.Cluster != "" {
			return fmt.Errorf("CLICKHOUSE_DEPLOYMENT=%s does not accept CLICKHOUSE_HOSTS or CLICKHOUSE_CLUSTER; use %s for a sharded deployment",
				DeploymentSingleNode, DeploymentSelfManagedCluster)
		}
	case DeploymentSelfManagedCluster:
		if len(e.Hosts) == 0 {
			return fmt.Errorf("CLICKHOUSE_DEPLOYMENT=%s requires CLICKHOUSE_HOSTS", DeploymentSelfManagedCluster)
		}
		if e.Cluster == "" {
			return fmt.Errorf("CLICKHOUSE_DEPLOYMENT=%s requires CLICKHOUSE_CLUSTER", DeploymentSelfManagedCluster)
		}
	case DeploymentCloud:
		// Cloud is one endpoint that replicates internally. There is nothing to
		// shard, no DDL cluster and no write load balancer in front of it.
		for _, v := range []struct{ name, val string }{
			{"CLICKHOUSE_HOSTS", strings.Join(e.Hosts, ",")},
			{"CLICKHOUSE_CLUSTER", e.Cluster},
			{"CLICKHOUSE_WRITE_HOST", e.WriteHost},
		} {
			if v.val != "" {
				return fmt.Errorf("CLICKHOUSE_DEPLOYMENT=%s does not accept %s: Cloud is a single endpoint that replicates internally",
					DeploymentCloud, v.name)
			}
		}
		if !e.Secure {
			return fmt.Errorf("CLICKHOUSE_DEPLOYMENT=%s requires CLICKHOUSE_SECURE=true: ClickHouse Cloud accepts TLS connections only", DeploymentCloud)
		}
		if e.TLSInsecure {
			return fmt.Errorf("CLICKHOUSE_DEPLOYMENT=%s does not accept CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY: Cloud presents a publicly verifiable certificate", DeploymentCloud)
		}
	}

	if e.Port <= 0 || e.Port > 65535 {
		return fmt.Errorf("CLICKHOUSE_PORT=%d is out of range", e.Port)
	}
	if !e.Secure && (e.CACertPath != "" || e.TLSServerName != "" || e.TLSInsecure) {
		return fmt.Errorf("CLICKHOUSE_CA_CERT, CLICKHOUSE_TLS_SERVER_NAME and CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY require CLICKHOUSE_SECURE=true")
	}

	// Surfaces a bad cluster name here, at config parse, rather than at the first
	// query that interpolates it.
	_, err := e.Topology()
	return err
}

// Topology derives the deployment topology this configuration describes.
func (e ClickHouseEnv) Topology() (Topology, error) {
	return DeriveTopology(TopologySpec{
		Kind:          e.Deployment,
		Cluster:       e.Cluster,
		FanoutCluster: e.FanoutCluster,
	})
}

func (e ClickHouseEnv) tls() TLSConfig {
	return TLSConfig{
		Enabled:            e.Secure,
		ServerName:         e.TLSServerName,
		CACertPath:         e.CACertPath,
		InsecureSkipVerify: e.TLSInsecure,
	}
}

// readHosts is every address the deployment can be read from: all shards for a
// sharded deployment, the single endpoint otherwise.
func (e ClickHouseEnv) readHosts() []string {
	if len(e.Hosts) > 0 {
		return e.Hosts
	}
	return []string{e.Host}
}

func (e ClickHouseEnv) connOptions(hosts []string, pool ClickHousePoolConfig) ConnOptions {
	return ConnOptions{
		Addrs:    HostAddrs(hosts, e.Port),
		Database: e.Database,
		User:     e.User,
		Password: e.Password,
		TLS:      e.tls(),
		Pool:     pool,
	}
}

// ClientOptions builds a client covering every address, which is what schema
// work and all-node backpressure need.
func (e ClickHouseEnv) ClientOptions(pool ClickHousePoolConfig, role ClientRole) (ClientOptions, error) {
	topo, err := e.Topology()
	if err != nil {
		return ClientOptions{}, err
	}
	return ClientOptions{Conn: e.connOptions(e.readHosts(), pool), Topo: topo, Role: role}, nil
}

// IngestOptions is ClientOptions routed through CLICKHOUSE_WRITE_HOST when one
// is set, so k8s spreads insert connections across shards through a load
// balancer while the read client keeps every shard address.
func (e ClickHouseEnv) IngestOptions(pool ClickHousePoolConfig, role ClientRole) (ClientOptions, error) {
	topo, err := e.Topology()
	if err != nil {
		return ClientOptions{}, err
	}
	hosts := e.readHosts()
	if e.WriteHost != "" {
		hosts = []string{e.WriteHost}
	}
	return ClientOptions{Conn: e.connOptions(hosts, pool), Topo: topo, Role: role}, nil
}

// String is an operator-facing one-line summary for startup logs. It never
// includes the password.
func (e ClickHouseEnv) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s db=%s user=%s", e.Deployment, strings.Join(HostAddrs(e.readHosts(), e.Port), ","), e.Database, e.User)
	if e.WriteHost != "" {
		fmt.Fprintf(&b, " write_host=%s", e.WriteHost)
	}
	if e.Secure {
		b.WriteString(" tls")
		if e.TLSInsecure {
			b.WriteString("(INSECURE: certificate verification disabled)")
		}
	}
	return b.String()
}
