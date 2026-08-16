package setup

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"bifract/pkg/storage"

	"gopkg.in/yaml.v3"
)

// The installer renders the CLICKHOUSE_* environment; the server parses it with
// storage.ClickHouseEnvFromOS. Nothing else connects those two sides, so a
// rename, a dropped variable, or a changed default on either side would only
// surface at runtime on a real deployment. These tests render the real
// templates, feed the result through the real parser, and assert the topology
// that comes out is the one that deployment is supposed to have.

func composeSetupConfig(t *testing.T) *SetupConfig {
	t.Helper()
	return &SetupConfig{
		InstallDir:               t.TempDir(),
		Domain:                   "bifract.example.com",
		ImageTag:                 "v0.0.3",
		PostgresPassword:         "pgpw",
		IngestPostgresPassword:   "ingestpgpw",
		ClickHousePassword:       "chpw",
		IngestClickHousePassword: "ingestchpw",
		LiteLLMMasterKey:         "llmkey",
		AdminPasswordHash:        "hash",
		PasswordPepper:           "pepper",
		FeedEncryptionKey:        strings.Repeat("a", 64),
		BackupEncryptionKey:      strings.Repeat("b", 64),
	}
}

// composeServiceEnv extracts one compose service's environment map, resolving
// ${VAR} references against the values the installer writes to .env.
func composeServiceEnv(t *testing.T, rendered, service string, envFile map[string]string) map[string]string {
	t.Helper()
	var doc struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(rendered), &doc); err != nil {
		t.Fatalf("parse docker-compose: %v", err)
	}
	svc, ok := doc.Services[service]
	if !ok {
		t.Fatalf("service %q not found in rendered compose", service)
	}
	out := map[string]string{}
	for k, v := range svc.Environment {
		if strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") {
			v = envFile[strings.TrimSuffix(strings.TrimPrefix(v, "${"), "}")]
		}
		out[k] = v
	}
	return out
}

// k8sContainerEnv extracts a deployment's first container env. Values sourced
// from a secretKeyRef have no literal, which is correct: the contract never
// requires a password to be present.
func k8sContainerEnv(t *testing.T, rendered string) map[string]string {
	t.Helper()
	var doc struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Env []struct {
							Name  string `yaml:"name"`
							Value string `yaml:"value"`
						} `yaml:"env"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	for _, d := range strings.Split(rendered, "\n---") {
		if err := yaml.Unmarshal([]byte(d), &doc); err != nil {
			t.Fatalf("parse manifest: %v", err)
		}
		if len(doc.Spec.Template.Spec.Containers) > 0 {
			out := map[string]string{}
			for _, e := range doc.Spec.Template.Spec.Containers[0].Env {
				out[e.Name] = e.Value
			}
			return out
		}
	}
	t.Fatal("no container with env found in rendered manifest")
	return nil
}

// applyCHEnv sets exactly the CLICKHOUSE_* variables the deployment renders,
// clearing the rest so the assertion reflects the manifest and nothing else.
func applyCHEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, k := range []string{
		"CLICKHOUSE_DEPLOYMENT", "CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_DB",
		"CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_HOSTS", "CLICKHOUSE_CLUSTER",
		"CLICKHOUSE_WRITE_HOST", "CLICKHOUSE_FANOUT_CLUSTER", "CLICKHOUSE_SECURE",
		"CLICKHOUSE_CA_CERT", "CLICKHOUSE_TLS_SERVER_NAME", "CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY",
	} {
		t.Setenv(k, "")
	}
	for k, v := range env {
		if strings.HasPrefix(k, "CLICKHOUSE_") {
			t.Setenv(k, v)
		}
	}
}

func TestDockerRenderedEnvParsesToSingleNode(t *testing.T) {
	cfg := composeSetupConfig(t)
	rendered, err := RenderDockerCompose(cfg)
	if err != nil {
		t.Fatalf("render docker-compose: %v", err)
	}
	envFile := map[string]string{
		"CLICKHOUSE_PASSWORD":                cfg.ClickHousePassword,
		"BIFRACT_INGEST_CLICKHOUSE_PASSWORD": cfg.IngestClickHousePassword,
	}

	for _, svc := range []struct{ name, wantUser, wantPassword string }{
		{"bifract", "default", cfg.ClickHousePassword},
		{"bifract-ingest", storage.IngestCHUser, cfg.IngestClickHousePassword},
	} {
		t.Run(svc.name, func(t *testing.T) {
			env := composeServiceEnv(t, rendered, svc.name, envFile)
			applyCHEnv(t, env)

			e, err := storage.ClickHouseEnvFromOS()
			if err != nil {
				t.Fatalf("ClickHouseEnvFromOS: %v", err)
			}
			if e.Deployment != storage.DeploymentSingleNode {
				t.Errorf("Deployment = %q, want %q", e.Deployment, storage.DeploymentSingleNode)
			}
			if e.Host != "clickhouse" || e.Port != 9000 {
				t.Errorf("endpoint = %s:%d, want clickhouse:9000", e.Host, e.Port)
			}
			if e.Database != "logs" {
				t.Errorf("Database = %q, want logs", e.Database)
			}
			if e.User != svc.wantUser {
				t.Errorf("User = %q, want %q", e.User, svc.wantUser)
			}
			if e.Password != svc.wantPassword {
				t.Errorf("Password did not resolve from .env")
			}
			if e.Secure {
				t.Error("Secure = true, want plaintext for a bundled ClickHouse")
			}

			topo, err := e.Topology()
			if err != nil {
				t.Fatalf("Topology: %v", err)
			}
			if topo.DistributedTables || topo.ReplicatedEngines || topo.PerNodeAdmin || topo.ShardRouting {
				t.Errorf("topology = %+v, want the plain single-node shape", topo)
			}
			if topo.DDLCluster != "" || topo.FanoutCluster != "" {
				t.Errorf("cluster names = %q/%q, want both empty", topo.DDLCluster, topo.FanoutCluster)
			}
		})
	}
}

// renderK8sInstall runs the real writeK8sManifests path into a temp dir and
// returns it, so these assertions cover what an operator actually gets rather
// than a hand-assembled template fixture.
func renderK8sInstall(t *testing.T, shards int, ch ClickHouseTarget) string {
	t.Helper()
	dir := t.TempDir()
	cfg := freshK8sConfig(sizeProfiles[0], dir)
	cfg.CHShards = shards
	cfg.CH = ch
	for _, sub := range []string{"clickhouse", "postgres", "bifract", "caddy", "litellm"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeK8sManifests(cfg); err != nil {
		t.Fatalf("writeK8sManifests: %v", err)
	}
	return dir
}

func readRendered(t *testing.T, dir, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(b)
}

func TestK8sRenderedEnvParsesToCluster(t *testing.T) {
	for _, tc := range []struct {
		name          string
		manifest      string
		shards        int
		wantUser      string
		wantWriteHost string
	}{
		{"app single shard", "bifract/deployment.yaml", 1, "default", ""},
		{"app multi shard", "bifract/deployment.yaml", 3, "default", "bifract-ch-clickhouse-lb"},
		{"ingest multi shard", "bifract/ingest-deployment.yaml", 3, storage.IngestCHUser, "bifract-ch-clickhouse-lb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := renderK8sInstall(t, tc.shards, ClickHouseTarget{})
			applyCHEnv(t, k8sContainerEnv(t, readRendered(t, dir, tc.manifest)))

			e, err := storage.ClickHouseEnvFromOS()
			if err != nil {
				t.Fatalf("ClickHouseEnvFromOS: %v", err)
			}
			if e.Deployment != storage.DeploymentSelfManagedCluster {
				t.Errorf("Deployment = %q, want %q", e.Deployment, storage.DeploymentSelfManagedCluster)
			}
			if len(e.Hosts) != tc.shards {
				t.Errorf("Hosts = %v, want %d shard address(es)", e.Hosts, tc.shards)
			}
			if e.WriteHost != tc.wantWriteHost {
				t.Errorf("WriteHost = %q, want %q", e.WriteHost, tc.wantWriteHost)
			}
			if e.User != tc.wantUser {
				t.Errorf("User = %q, want %q", e.User, tc.wantUser)
			}
			if e.Secure {
				t.Error("Secure = true, want plaintext for an in-cluster ClickHouse")
			}

			topo, err := e.Topology()
			if err != nil {
				t.Fatalf("Topology: %v", err)
			}
			// Every one of these drove a separate branch before the refactor and
			// all of them hung off a single non-empty cluster name.
			if !topo.DistributedTables || !topo.ReplicatedEngines || !topo.PerNodeAdmin || !topo.ShardRouting {
				t.Errorf("topology = %+v, want the full sharded shape", topo)
			}
			if topo.DDLCluster != "default" || topo.FanoutCluster != "default" {
				t.Errorf("cluster names = %q/%q, want both \"default\"", topo.DDLCluster, topo.FanoutCluster)
			}
			if topo.ManagedStorage {
				t.Error("ManagedStorage = true, want false for self-managed ClickHouse")
			}

			// The write LB must route inserts without narrowing the read client,
			// which still needs every shard for schema sync and backpressure.
			read, err := e.ClientOptions(storage.DefaultQueryPoolConfig(), storage.RoleControlPlane)
			if err != nil {
				t.Fatalf("ClientOptions: %v", err)
			}
			if len(read.Conn.Addrs) != tc.shards {
				t.Errorf("read addrs = %v, want %d", read.Conn.Addrs, tc.shards)
			}
			write, err := e.IngestOptions(storage.DefaultIngestPoolConfig(), storage.RoleIngest)
			if err != nil {
				t.Fatalf("IngestOptions: %v", err)
			}
			wantWrite := tc.shards
			if tc.wantWriteHost != "" {
				wantWrite = 1
			}
			if len(write.Conn.Addrs) != wantWrite {
				t.Errorf("write addrs = %v, want %d", write.Conn.Addrs, wantWrite)
			}
		})
	}
}

// An external target must render env the server parses back to exactly what the
// operator asked for, and must not render any manifest for a ClickHouse this
// installer does not own.
func TestK8sExternalClickHouse(t *testing.T) {
	target := ClickHouseTarget{
		Backend:    CHBackendExternal,
		Deployment: string(storage.DeploymentCloud),
		Host:       "abc.clickhouse.cloud",
	}
	target.Normalize()
	dir := renderK8sInstall(t, 3, target)

	applyCHEnv(t, k8sContainerEnv(t, readRendered(t, dir, "bifract/deployment.yaml")))
	e, err := storage.ClickHouseEnvFromOS()
	if err != nil {
		t.Fatalf("ClickHouseEnvFromOS: %v", err)
	}
	if e.Deployment != storage.DeploymentCloud {
		t.Errorf("Deployment = %q, want %q", e.Deployment, storage.DeploymentCloud)
	}
	if e.Host != "abc.clickhouse.cloud" || e.Port != 9440 {
		t.Errorf("endpoint = %s:%d, want abc.clickhouse.cloud:9440", e.Host, e.Port)
	}
	if !e.Secure {
		t.Error("Secure = false, want TLS for a managed service")
	}
	// The shard count must not leak into an external target: it describes a
	// ClickHouse this installer is no longer rendering.
	if len(e.Hosts) != 0 || e.WriteHost != "" {
		t.Errorf("Hosts = %v, WriteHost = %q, want neither for a single managed endpoint", e.Hosts, e.WriteHost)
	}
	topo, err := e.Topology()
	if err != nil {
		t.Fatalf("Topology: %v", err)
	}
	if topo.DistributedTables || topo.PerNodeAdmin || !topo.ManagedStorage {
		t.Errorf("topology = %+v, want the managed single-endpoint shape", topo)
	}

	// No ClickHouse workload manifests, and nothing in kustomization pointing at
	// a file we did not write (which is what kubectl kustomize would reject).
	for _, rel := range []string{"clickhouse/clickhouse-installation.yaml", "clickhouse/lb-service.yaml"} {
		if _, err := os.Stat(filepath.Join(dir, rel)); !os.IsNotExist(err) {
			t.Errorf("%s was rendered for an external ClickHouse", rel)
		}
	}
	kustomization := readRendered(t, dir, "kustomization.yaml")
	for _, ref := range []string{"clickhouse/clickhouse-installation.yaml", "clickhouse/lb-service.yaml"} {
		if strings.Contains(kustomization, ref) {
			t.Errorf("kustomization still lists %s", ref)
		}
	}
	// All policies here are Ingress-only, so an external ClickHouse needs none.
	policies := readRendered(t, dir, "network-policies.yaml")
	if strings.Contains(policies, "allow-clickhouse-from-bifract") || strings.Contains(policies, "allow-keeper-from-clickhouse") {
		t.Error("in-cluster ClickHouse NetworkPolicies rendered for an external ClickHouse")
	}
}

// An external ClickHouse must produce a compose file docker itself accepts, with
// no ClickHouse service, no depends_on pointing at one, and no orphan volume.
// This is what catches a conditional that leaves the YAML structurally broken.
func TestDockerComposeExternalClickHouse(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	cfg := composeSetupConfig(t)
	cfg.CH = ClickHouseTarget{
		Backend:    CHBackendExternal,
		Deployment: string(storage.DeploymentCloud),
		Host:       "abc.clickhouse.cloud",
	}
	cfg.CH.Normalize()

	rendered, err := RenderDockerCompose(cfg)
	if err != nil {
		t.Fatalf("render docker-compose: %v", err)
	}
	if strings.Contains(rendered, "container_name: bifract-clickhouse") {
		t.Error("a ClickHouse service was rendered for an external target")
	}
	if strings.Contains(rendered, "clickhouse-data") {
		t.Error("the ClickHouse data volume was rendered for an external target")
	}

	path := filepath.Join(t.TempDir(), "docker-compose.yml")
	if err := os.WriteFile(path, []byte(rendered), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("docker", "compose", "-f", path, "config").CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose config rejected the external render: %v\n%s", err, out)
	}

	// And the env it renders must parse back to the managed topology.
	applyCHEnv(t, composeServiceEnv(t, rendered, "bifract", map[string]string{}))
	e, err := storage.ClickHouseEnvFromOS()
	if err != nil {
		t.Fatalf("ClickHouseEnvFromOS: %v", err)
	}
	if e.Deployment != storage.DeploymentCloud || !e.Secure || e.Port != 9440 {
		t.Errorf("env = %+v, want a secure Cloud endpoint on 9440", e)
	}
}

// kubectl kustomize is the definitive check that no resources: entry points at a
// file the conditional rendering did not write. A dangling entry is invisible in
// the rendered YAML and only fails at apply time.
func TestKustomizeBuildsWithExternalClickHouse(t *testing.T) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not available")
	}
	target := ClickHouseTarget{
		Backend:    CHBackendExternal,
		Deployment: string(storage.DeploymentCloud),
		Host:       "abc.clickhouse.cloud",
	}
	target.Normalize()
	// Three shards deliberately: the shard count must not resurrect the load
	// balancer entry for a ClickHouse this installer no longer renders.
	dir := renderK8sInstall(t, 3, target)

	out, err := exec.Command("kubectl", "kustomize", dir).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl kustomize failed for an external ClickHouse: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "kind: Deployment") {
		t.Error("kustomize output has no Deployment")
	}
	if strings.Contains(string(out), "kind: ClickHouseCluster") {
		t.Error("a ClickHouseCluster was built for an external ClickHouse")
	}
}

// An operator-supplied ClickHouse password must survive GeneratePasswords. The
// bundled path generates one because the installer creates that server; an
// external server already owns its credential, and overwriting it would write a
// password that authenticates nowhere.
func TestGeneratePasswordsPreservesExternalClickHousePassword(t *testing.T) {
	external := &SetupConfig{CH: ClickHouseTarget{Backend: CHBackendExternal, Host: "ch.example.com"}}
	external.ClickHousePassword = "the-operators-real-password"
	if err := external.GeneratePasswords(); err != nil {
		t.Fatalf("GeneratePasswords: %v", err)
	}
	if external.ClickHousePassword != "the-operators-real-password" {
		t.Errorf("ClickHousePassword = %q, want the operator's value", external.ClickHousePassword)
	}
	// The ingest identity is still ours to mint: the app creates that user on
	// whichever server it connects to.
	if external.IngestClickHousePassword == "" {
		t.Error("IngestClickHousePassword was not generated")
	}

	bundled := &SetupConfig{}
	if err := bundled.GeneratePasswords(); err != nil {
		t.Fatalf("GeneratePasswords: %v", err)
	}
	if len(bundled.ClickHousePassword) < 20 {
		t.Errorf("bundled ClickHousePassword = %q, want a generated value", bundled.ClickHousePassword)
	}
}
