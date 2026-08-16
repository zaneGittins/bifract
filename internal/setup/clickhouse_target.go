package setup

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"bifract/pkg/storage"
)

// CHBackend selects where ClickHouse comes from.
//
// The bundled value is deliberately the ZERO value. Every lifecycle path that
// rebuilds a SetupConfig from an existing install (reconfigure, upgrade) leaves
// unknown fields at their zero value, so an install that predates this type must
// keep rendering the bundled ClickHouse it already has.
type CHBackend string

const (
	CHBackendBundled  CHBackend = ""
	CHBackendExternal CHBackend = "external"
)

// ClickHouseTarget is where this install's ClickHouse lives. It is shared by the
// docker and k8s wizards, which are otherwise entirely separate programs: the
// value object, its validation and its environment output are the parts worth
// sharing, and each wizard keeps its own input plumbing.
type ClickHouseTarget struct {
	Backend CHBackend

	// Deployment is the storage.DeploymentKind this target is. Only meaningful
	// when external; a bundled target's shape is decided by the manifests.
	Deployment string

	Host          string
	Port          int
	Hosts         string // comma-separated, external sharded clusters only
	Cluster       string
	FanoutCluster string
	Database      string
	User          string

	Secure     bool
	CACertPath string
}

// Bundled reports whether the installer renders and owns a ClickHouse.
func (t ClickHouseTarget) Bundled() bool { return t.Backend != CHBackendExternal }

// Normalize fills defaults. An empty Backend means bundled, and a bundled target
// is pinned to the values the shipped manifests use.
func (t *ClickHouseTarget) Normalize() {
	if t.Backend != CHBackendExternal {
		t.Backend = CHBackendBundled
	}
	if t.Database == "" {
		t.Database = "logs"
	}
	if t.User == "" {
		t.User = "default"
	}
	if t.Bundled() {
		// The bundled ClickHouse is reached in-network by service name and is
		// never TLS; the wizard does not offer these, so pin them rather than
		// letting a stale value survive a reconfigure.
		t.Deployment = ""
		t.Secure = false
		t.CACertPath = ""
		t.FanoutCluster = ""
		t.Port = 9000
		return
	}
	if t.Deployment == "" {
		t.Deployment = string(storage.DeploymentSingleNode)
		if t.Hosts != "" && t.Cluster != "" {
			t.Deployment = string(storage.DeploymentSelfManagedCluster)
		}
	}
	if t.Deployment == string(storage.DeploymentCloud) {
		t.Secure = true
	}
	if t.Port == 0 {
		t.Port = 9000
		if t.Secure {
			t.Port = 9440
		}
	}
}

// Validate checks an external target before any files are written, so a typo is
// caught at install time rather than at the app's first connection. It runs the
// same rules the server applies at startup, from the same package, so the
// installer cannot accept a configuration the server will reject.
func (t ClickHouseTarget) Validate() error {
	if t.Bundled() {
		return nil
	}
	if t.Host == "" && t.Hosts == "" {
		return fmt.Errorf("external ClickHouse needs a host")
	}
	if t.Port <= 0 || t.Port > 65535 {
		return fmt.Errorf("port %d is out of range", t.Port)
	}
	env := storage.ClickHouseEnv{
		Deployment:    storage.DeploymentKind(t.Deployment),
		Host:          t.Host,
		Port:          t.Port,
		Hosts:         splitCSV(t.Hosts),
		Database:      t.Database,
		User:          t.User,
		Cluster:       t.Cluster,
		FanoutCluster: t.FanoutCluster,
		Secure:        t.Secure,
		CACertPath:    t.CACertPath,
	}
	if _, err := env.Topology(); err != nil {
		return err
	}
	return nil
}

// EnvVar is one rendered environment entry.
type EnvVar struct {
	Name  string
	Value string
}

// EnvVars is the SINGLE definition of the CLICKHOUSE_* environment the installer
// emits, for both docker and k8s. It must stay in lockstep with
// storage.ClickHouseEnvFromOS, which is what reads it back;
// TestDockerRenderedEnvParsesToSingleNode and TestK8sRenderedEnvParsesToCluster
// are what hold the two together.
//
// The password is deliberately absent: it comes from .env or a secretKeyRef, not
// from a rendered literal.
func (t ClickHouseTarget) EnvVars(o CHEnvOptions) []EnvVar {
	t.Normalize()

	user := t.User
	if o.UserOverride != "" {
		user = o.UserOverride
	}

	out := []EnvVar{}
	add := func(name, value string) {
		if value != "" {
			out = append(out, EnvVar{Name: name, Value: value})
		}
	}

	if t.Bundled() {
		// The manifests decide the bundled shape: docker reaches one service by
		// name, k8s reaches the operator's pod FQDNs through a cluster.
		add("CLICKHOUSE_HOST", o.BundledHost)
		add("CLICKHOUSE_HOSTS", o.BundledHosts)
		add("CLICKHOUSE_WRITE_HOST", o.BundledWriteHost)
		add("CLICKHOUSE_PORT", "9000")
		add("CLICKHOUSE_DB", t.Database)
		add("CLICKHOUSE_USER", user)
		add("CLICKHOUSE_CLUSTER", o.BundledCluster)
		return out
	}

	add("CLICKHOUSE_DEPLOYMENT", t.Deployment)
	add("CLICKHOUSE_HOST", t.Host)
	add("CLICKHOUSE_HOSTS", t.Hosts)
	add("CLICKHOUSE_PORT", strconv.Itoa(t.Port))
	add("CLICKHOUSE_DB", t.Database)
	add("CLICKHOUSE_USER", user)
	add("CLICKHOUSE_CLUSTER", t.Cluster)
	add("CLICKHOUSE_FANOUT_CLUSTER", t.FanoutCluster)
	if t.Secure {
		add("CLICKHOUSE_SECURE", "true")
		add("CLICKHOUSE_CA_CERT", t.CACertPath)
	}
	return out
}

// CHEnvOptions carries what only the caller knows: how the bundled ClickHouse is
// addressed on this platform, and which identity this particular service uses.
type CHEnvOptions struct {
	BundledHost      string
	BundledHosts     string
	BundledCluster   string
	BundledWriteHost string
	// UserOverride replaces the target user. The ingest tier connects as its own
	// least-privilege identity rather than the admin one.
	UserOverride string
}

// Summary is a one-line operator-facing description for the wizard's confirm step.
func (t ClickHouseTarget) Summary() string {
	if t.Bundled() {
		return "bundled (managed by this installer)"
	}
	endpoint := t.Host
	if t.Hosts != "" {
		endpoint = t.Hosts
	}
	s := fmt.Sprintf("%s %s:%d", t.Deployment, endpoint, t.Port)
	if t.Secure {
		s += " (TLS)"
	}
	return s
}

func splitCSV(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// dockerBundledCHHost is the compose service name the bundled ClickHouse is
// reachable at, and the only address the docker path ever uses.
const dockerBundledCHHost = "clickhouse"

// CHBundled reports whether docker-compose.yml renders its own ClickHouse.
// Template method: drives the service, its depends_on entries and its volume.
func (c *SetupConfig) CHBundled() bool { return c.CH.Bundled() }

// CHEnvApp is the ClickHouse environment for services that connect with the
// admin identity: the app tier and both archiver services.
func (c *SetupConfig) CHEnvApp() []EnvVar {
	return c.CH.EnvVars(CHEnvOptions{BundledHost: dockerBundledCHHost})
}

// CHEnvIngest is the same, for the least-privilege ingest tier.
func (c *SetupConfig) CHEnvIngest() []EnvVar {
	return c.CH.EnvVars(CHEnvOptions{
		BundledHost:  dockerBundledCHHost,
		UserOverride: storage.IngestCHUser,
	})
}

// Env var names used to persist an external target in .env, so reconfigure and
// upgrade recover it instead of silently reverting the install to a bundled
// ClickHouse it no longer has.
const (
	envCHBackend    = "BIFRACT_CH_BACKEND"
	envCHDeployment = "CLICKHOUSE_DEPLOYMENT"
	envCHHost       = "CLICKHOUSE_HOST"
	envCHHosts      = "CLICKHOUSE_HOSTS"
	envCHPort       = "CLICKHOUSE_PORT"
	envCHDatabase   = "CLICKHOUSE_DB"
	envCHUser       = "CLICKHOUSE_USER"
	envCHCluster    = "CLICKHOUSE_CLUSTER"
	envCHFanout     = "CLICKHOUSE_FANOUT_CLUSTER"
	envCHSecure     = "CLICKHOUSE_SECURE"
	envCHCACert     = "CLICKHOUSE_CA_CERT"
)

// PersistEnv renders the target as .env lines. Empty for a bundled target: its
// shape lives in the compose file, and writing it twice invites drift.
func (t ClickHouseTarget) PersistEnv() []EnvVar {
	if t.Bundled() {
		return nil
	}
	t.Normalize()
	out := []EnvVar{{Name: envCHBackend, Value: string(CHBackendExternal)}}
	for _, v := range []EnvVar{
		{envCHDeployment, t.Deployment},
		{envCHHost, t.Host},
		{envCHHosts, t.Hosts},
		{envCHPort, strconv.Itoa(t.Port)},
		{envCHDatabase, t.Database},
		{envCHUser, t.User},
		{envCHCluster, t.Cluster},
		{envCHFanout, t.FanoutCluster},
		{envCHCACert, t.CACertPath},
	} {
		if v.Value != "" {
			out = append(out, v)
		}
	}
	if t.Secure {
		out = append(out, EnvVar{Name: envCHSecure, Value: "true"})
	}
	return out
}

// TargetFromEnv recovers a persisted target. A file with no backend marker
// describes a bundled install, which is the zero value.
func TargetFromEnv(env map[string]string) ClickHouseTarget {
	var t ClickHouseTarget
	if env[envCHBackend] != string(CHBackendExternal) {
		t.Normalize()
		return t
	}
	t.Backend = CHBackendExternal
	t.Deployment = env[envCHDeployment]
	t.Host = env[envCHHost]
	t.Hosts = env[envCHHosts]
	t.Database = env[envCHDatabase]
	t.User = env[envCHUser]
	t.Cluster = env[envCHCluster]
	t.FanoutCluster = env[envCHFanout]
	t.CACertPath = env[envCHCACert]
	t.Secure = env[envCHSecure] == "true"
	if p, err := strconv.Atoi(env[envCHPort]); err == nil {
		t.Port = p
	}
	t.Normalize()
	return t
}

// TLSServerNameFor is the name to verify the certificate against: the address
// host unless the operator overrode it. Stripping any port keeps SNI valid when
// the host entry carries one.
func (t ClickHouseTarget) TLSServerNameFor(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return host
}
