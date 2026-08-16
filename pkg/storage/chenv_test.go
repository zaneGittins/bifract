package storage

import "testing"

// setEnv applies a CLICKHOUSE_* set for one test, clearing every variable the
// contract reads first so a stray value from the developer's shell cannot make
// a case pass or fail spuriously.
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for _, k := range []string{
		"CLICKHOUSE_DEPLOYMENT", "CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_DB",
		"CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_HOSTS", "CLICKHOUSE_CLUSTER",
		"CLICKHOUSE_WRITE_HOST", "CLICKHOUSE_FANOUT_CLUSTER", "CLICKHOUSE_SECURE",
		"CLICKHOUSE_CA_CERT", "CLICKHOUSE_TLS_SERVER_NAME", "CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY",
	} {
		t.Setenv(k, "")
	}
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

// TestClickHouseEnvDefaultsMatchLegacy pins the defaults every existing docker
// install relies on. These were read individually in main.go before the contract
// existed; changing any of them silently repoints a running deployment.
func TestClickHouseEnvDefaultsMatchLegacy(t *testing.T) {
	setEnv(t, nil)
	e, err := ClickHouseEnvFromOS()
	if err != nil {
		t.Fatalf("ClickHouseEnvFromOS: %v", err)
	}
	for _, tc := range []struct{ name, got, want string }{
		{"Host", e.Host, "localhost"},
		{"Database", e.Database, "logs"},
		{"User", e.User, "default"},
		{"Password", e.Password, ""},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if e.Port != defaultNativePort {
		t.Errorf("Port = %d, want %d", e.Port, defaultNativePort)
	}
	if e.Secure {
		t.Error("Secure = true, want plaintext by default")
	}
	if e.Deployment != DeploymentSingleNode {
		t.Errorf("Deployment = %q, want %q", e.Deployment, DeploymentSingleNode)
	}
}

func TestClickHouseEnvDeploymentDerivation(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want DeploymentKind
	}{
		{"neither set is single node", nil, DeploymentSingleNode},
		{"both set is a cluster",
			map[string]string{"CLICKHOUSE_HOSTS": "a,b", "CLICKHOUSE_CLUSTER": "bifract"},
			DeploymentSelfManagedCluster},
		{"explicit kind wins",
			map[string]string{"CLICKHOUSE_DEPLOYMENT": "cloud", "CLICKHOUSE_HOST": "x.clickhouse.cloud"},
			DeploymentCloud},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			e, err := ClickHouseEnvFromOS()
			if err != nil {
				t.Fatalf("ClickHouseEnvFromOS: %v", err)
			}
			if e.Deployment != tc.want {
				t.Errorf("Deployment = %q, want %q", e.Deployment, tc.want)
			}
		})
	}
}

// TestClickHouseEnvRejects covers the validation matrix. The half-a-cluster cases
// are a genuine bug fix: they used to fall through silently to single-node, which
// runs the whole app against one shard of a sharded cluster.
func TestClickHouseEnvRejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"hosts without cluster", map[string]string{"CLICKHOUSE_HOSTS": "a,b"}},
		{"cluster without hosts", map[string]string{"CLICKHOUSE_CLUSTER": "bifract"}},
		{"unknown deployment", map[string]string{"CLICKHOUSE_DEPLOYMENT": "warehouse"}},
		{"single node with hosts", map[string]string{
			"CLICKHOUSE_DEPLOYMENT": "single-node", "CLICKHOUSE_HOSTS": "a,b"}},
		{"cluster kind without hosts", map[string]string{
			"CLICKHOUSE_DEPLOYMENT": "cluster", "CLICKHOUSE_CLUSTER": "bifract"}},
		{"cloud with a cluster name", map[string]string{
			"CLICKHOUSE_DEPLOYMENT": "cloud", "CLICKHOUSE_CLUSTER": "default"}},
		{"cloud with hosts", map[string]string{
			"CLICKHOUSE_DEPLOYMENT": "cloud", "CLICKHOUSE_HOSTS": "a,b"}},
		{"cloud with a write host", map[string]string{
			"CLICKHOUSE_DEPLOYMENT": "cloud", "CLICKHOUSE_WRITE_HOST": "lb"}},
		{"cloud with TLS off", map[string]string{
			"CLICKHOUSE_DEPLOYMENT": "cloud", "CLICKHOUSE_SECURE": "false"}},
		{"cloud skipping verification", map[string]string{
			"CLICKHOUSE_DEPLOYMENT": "cloud", "CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY": "true"}},
		{"TLS options without TLS", map[string]string{"CLICKHOUSE_CA_CERT": "/ca.pem"}},
		{"non-numeric port", map[string]string{"CLICKHOUSE_PORT": "nine thousand"}},
		{"out of range port", map[string]string{"CLICKHOUSE_PORT": "70000"}},
		{"bad cluster name", map[string]string{
			"CLICKHOUSE_HOSTS": "a,b", "CLICKHOUSE_CLUSTER": "a' OR '1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			if _, err := ClickHouseEnvFromOS(); err == nil {
				t.Fatal("accepted, want an error")
			}
		})
	}
}

// TestClickHouseEnvCloudNeedsThreeVariables is the ergonomic claim behind the
// secure-dependent port default.
func TestClickHouseEnvCloudNeedsThreeVariables(t *testing.T) {
	setEnv(t, map[string]string{
		"CLICKHOUSE_DEPLOYMENT": "cloud",
		"CLICKHOUSE_HOST":       "abc.clickhouse.cloud",
		"CLICKHOUSE_PASSWORD":   "pw",
	})
	e, err := ClickHouseEnvFromOS()
	if err != nil {
		t.Fatalf("ClickHouseEnvFromOS: %v", err)
	}
	if !e.Secure {
		t.Error("Secure = false, want true by default on Cloud")
	}
	if e.Port != defaultNativeSecurePort {
		t.Errorf("Port = %d, want %d when secure", e.Port, defaultNativeSecurePort)
	}
	topo, err := e.Topology()
	if err != nil {
		t.Fatalf("Topology: %v", err)
	}
	if topo.FanoutCluster != cloudFanoutCluster || topo.DistributedTables {
		t.Errorf("topology = %+v, want Cloud fanout with no Distributed tables", topo)
	}
	if e.String() == "" || !contains(e.String(), "tls") {
		t.Errorf("String() = %q, want it to report tls", e.String())
	}
}

// TestIngestOptionsRouteThroughWriteHost covers the k8s write load balancer: the
// read client must keep every shard address while writes go to the LB.
func TestIngestOptionsRouteThroughWriteHost(t *testing.T) {
	setEnv(t, map[string]string{
		"CLICKHOUSE_HOSTS":      "sh1,sh2",
		"CLICKHOUSE_CLUSTER":    "bifract",
		"CLICKHOUSE_WRITE_HOST": "ch-lb",
	})
	e, err := ClickHouseEnvFromOS()
	if err != nil {
		t.Fatalf("ClickHouseEnvFromOS: %v", err)
	}
	read, err := e.ClientOptions(DefaultQueryPoolConfig(), RoleControlPlane)
	if err != nil {
		t.Fatalf("ClientOptions: %v", err)
	}
	if len(read.Conn.Addrs) != 2 {
		t.Errorf("read addrs = %v, want both shards", read.Conn.Addrs)
	}
	write, err := e.IngestOptions(DefaultIngestPoolConfig(), RoleIngest)
	if err != nil {
		t.Fatalf("IngestOptions: %v", err)
	}
	if len(write.Conn.Addrs) != 1 || write.Conn.Addrs[0] != "ch-lb:9000" {
		t.Errorf("write addrs = %v, want [ch-lb:9000]", write.Conn.Addrs)
	}
	if write.Role != RoleIngest {
		t.Error("ingest client must never carry the control-plane role")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
