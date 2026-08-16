package storage

import "testing"

// TestDeriveTopologyMatchesLegacyIsCluster is the behaviour-preservation proof
// for the topology refactor. Before the refactor a single string field drove
// every branch: Cluster != "" meant Distributed tables, Replicated engines,
// ON CLUSTER, per-node admin, shard routing and fanout, all at once. For the two
// pre-existing deployment kinds every derived field must still equal exactly
// that, or a docker/k8s install changes behaviour.
func TestDeriveTopologyMatchesLegacyIsCluster(t *testing.T) {
	for _, tc := range []struct {
		name    string
		spec    TopologySpec
		cluster string // the legacy Cluster field value this config used to carry
	}{
		{"single node, kind derived", TopologySpec{}, ""},
		{"single node, kind explicit", TopologySpec{Kind: DeploymentSingleNode}, ""},
		{"cluster, kind derived", TopologySpec{Cluster: "default"}, "default"},
		{"cluster, kind explicit", TopologySpec{Kind: DeploymentSelfManagedCluster, Cluster: "bifract"}, "bifract"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DeriveTopology(tc.spec)
			if err != nil {
				t.Fatalf("DeriveTopology: %v", err)
			}
			legacyIsCluster := tc.cluster != ""

			for _, f := range []struct {
				name string
				got  bool
			}{
				{"DistributedTables", got.DistributedTables},
				{"ReplicatedEngines", got.ReplicatedEngines},
				{"PerNodeAdmin", got.PerNodeAdmin},
				{"ShardRouting", got.ShardRouting},
			} {
				if f.got != legacyIsCluster {
					t.Errorf("%s = %v, want %v (legacy IsCluster)", f.name, f.got, legacyIsCluster)
				}
			}
			if got.ManagedStorage {
				t.Error("ManagedStorage = true, want false for a self-managed deployment")
			}
			if got.DDLCluster != tc.cluster {
				t.Errorf("DDLCluster = %q, want %q (legacy Cluster)", got.DDLCluster, tc.cluster)
			}
			if got.FanoutCluster != tc.cluster {
				t.Errorf("FanoutCluster = %q, want %q (legacy Cluster)", got.FanoutCluster, tc.cluster)
			}
		})
	}
}

func TestDeriveTopologyCloud(t *testing.T) {
	got, err := DeriveTopology(TopologySpec{Kind: DeploymentCloud})
	if err != nil {
		t.Fatalf("DeriveTopology: %v", err)
	}
	if got.DistributedTables || got.ReplicatedEngines || got.PerNodeAdmin || got.ShardRouting {
		t.Errorf("Cloud must use the single-endpoint path, got %+v", got)
	}
	if !got.ManagedStorage {
		t.Error("ManagedStorage = false, want true")
	}
	if got.DDLCluster != "" {
		t.Errorf("DDLCluster = %q, want empty: Cloud replicates DDL without ON CLUSTER", got.DDLCluster)
	}
	if got.FanoutCluster != cloudFanoutCluster {
		t.Errorf("FanoutCluster = %q, want %q", got.FanoutCluster, cloudFanoutCluster)
	}
	if !got.fanoutDefaulted {
		t.Error("fanoutDefaulted = false: a value we supplied must stay narrowable")
	}
}

func TestDeriveTopologyOperatorFanoutIsNotNarrowable(t *testing.T) {
	got, err := DeriveTopology(TopologySpec{Kind: DeploymentCloud, FanoutCluster: "all_groups.default"})
	if err != nil {
		t.Fatalf("DeriveTopology: %v", err)
	}
	if got.FanoutCluster != "all_groups.default" {
		t.Errorf("FanoutCluster = %q, want the operator's value", got.FanoutCluster)
	}
	if got.fanoutDefaulted {
		t.Error("fanoutDefaulted = true: an operator-declared value must never be narrowed")
	}
}

func TestDeriveTopologyRejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec TopologySpec
	}{
		{"cluster kind without a name", TopologySpec{Kind: DeploymentSelfManagedCluster}},
		{"single node with a cluster name", TopologySpec{Kind: DeploymentSingleNode, Cluster: "c"}},
		{"cloud with a cluster name", TopologySpec{Kind: DeploymentCloud, Cluster: "default"}},
		{"unknown kind", TopologySpec{Kind: "warehouse"}},
		{"dotted DDL cluster", TopologySpec{Kind: DeploymentSelfManagedCluster, Cluster: "all_groups.default"}},
		{"injection in DDL cluster", TopologySpec{Kind: DeploymentSelfManagedCluster, Cluster: "a' OR '1"}},
		{"two dots in fanout cluster", TopologySpec{Kind: DeploymentCloud, FanoutCluster: "a.b.c"}},
		{"injection in fanout cluster", TopologySpec{Kind: DeploymentCloud, FanoutCluster: "a' OR '1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DeriveTopology(tc.spec); err == nil {
				t.Fatal("DeriveTopology succeeded, want an error")
			}
		})
	}
}

func TestTopologySQLFragments(t *testing.T) {
	single, err := DeriveTopology(TopologySpec{})
	if err != nil {
		t.Fatalf("DeriveTopology: %v", err)
	}
	cluster, err := DeriveTopology(TopologySpec{Cluster: "bifract"})
	if err != nil {
		t.Fatalf("DeriveTopology: %v", err)
	}

	for _, tc := range []struct{ name, got, want string }{
		{"single OnClusterSQL", single.OnClusterSQL(), ""},
		{"single fanout", single.FanoutSystemTable("system.parts"), "system.parts"},
		{"single fanout args", single.FanoutSystemTableArgs("system", "distribution_queue"), "system.distribution_queue"},
		{"single shard table", single.ShardSystemTable("system.parts"), "system.parts"},
		{"single table", single.Table("logs"), "logs"},

		{"cluster OnClusterSQL", cluster.OnClusterSQL(), " ON CLUSTER 'bifract'"},
		{"cluster fanout", cluster.FanoutSystemTable("system.parts"), "clusterAllReplicas('bifract', system.parts)"},
		{"cluster fanout args", cluster.FanoutSystemTableArgs("system", "distribution_queue"), "clusterAllReplicas('bifract', system, distribution_queue)"},
		{"cluster shard table", cluster.ShardSystemTable("system.parts"), "cluster('bifract', system.parts)"},
		{"cluster table", cluster.Table("logs"), "logs_distributed"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}
