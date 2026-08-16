package storage

import (
	"fmt"
	"regexp"
	"strings"
)

// DeploymentKind is declared by the operator, never inferred from the presence
// of some other value. Inferring it is unsafe: a Cloud service exposes a cluster
// named "default" for table functions, and treating that as a self-managed
// cluster name would wrongly enable Distributed tables, engine rewriting and
// ON CLUSTER against a server that has none of them.
type DeploymentKind string

const (
	DeploymentSingleNode         DeploymentKind = "single-node"
	DeploymentSelfManagedCluster DeploymentKind = "cluster"
	DeploymentCloud              DeploymentKind = "cloud"
)

// cloudFanoutCluster is the cluster name ClickHouse Cloud exposes in
// system.clusters for the current service. Overridable per deployment (e.g. to
// a warehouse-wide all_groups.<service> scope).
const cloudFanoutCluster = "default"

// Topology answers the questions the code actually asks about a deployment.
// Every field is a pure function of configuration: resolving a Topology cannot
// fail at runtime and cannot change once built. Facts that only a live server
// can answer belong in Capabilities, not here.
//
// Fanout and ON CLUSTER are expressed as the presence of a name rather than a
// bool plus a name, so "enabled with no name" is unrepresentable.
type Topology struct {
	Kind DeploymentKind

	// DistributedTables: reads and writes go through the *_distributed tables.
	DistributedTables bool
	// ReplicatedEngines: init SQL is rewritten to Replicated* with a Keeper path.
	ReplicatedEngines bool
	// PerNodeAdmin: schema, DDL and metrics work iterates Addrs() opening a
	// direct connection per node, rather than using the load-balanced pool.
	PerNodeAdmin bool
	// ShardRouting: system.clusters shard_num lookups and shard-direct connections.
	ShardRouting bool
	// ManagedStorage: the server owns replication, merges and storage tiers, so
	// the app must never issue storage-policy DDL or SYSTEM STOP MERGES.
	ManagedStorage bool

	// DDLCluster names the cluster for ON CLUSTER. Empty means never emit it.
	DDLCluster string
	// FanoutCluster names the cluster for cluster(), clusterAllReplicas() and the
	// *Cluster table functions. Empty means never fan out.
	FanoutCluster string

	// fanoutDefaulted records that FanoutCluster came from us rather than the
	// operator. Only a defaulted value may be narrowed by ResolveTopology.
	fanoutDefaulted bool
}

// TopologySpec is the configuration input to DeriveTopology.
type TopologySpec struct {
	// Kind may be empty, in which case it is derived from Cluster for backwards
	// compatibility with deployments predating CLICKHOUSE_DEPLOYMENT.
	Kind          DeploymentKind
	Cluster       string
	FanoutCluster string
}

// validDDLClusterName matches names usable in ON CLUSTER and Distributed().
// Self-managed cluster names are plain identifiers.
var validDDLClusterName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// validFanoutClusterName additionally permits one dot, for the warehouse-scoped
// form (all_groups.<service>) that cluster()/clusterAllReplicas() accept. This
// is deliberately not a general relaxation of the DDL grammar.
var validFanoutClusterName = regexp.MustCompile(`^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)?$`)

// DeriveTopology builds a Topology from configuration. It is pure and total:
// every error is a configuration error the operator can fix.
func DeriveTopology(spec TopologySpec) (Topology, error) {
	kind := spec.Kind
	if kind == "" {
		kind = DeploymentSingleNode
		if spec.Cluster != "" {
			kind = DeploymentSelfManagedCluster
		}
	}

	t := Topology{Kind: kind}

	switch kind {
	case DeploymentSingleNode:
		if spec.Cluster != "" {
			return Topology{}, fmt.Errorf("deployment %q does not accept a cluster name (got %q)", kind, spec.Cluster)
		}
	case DeploymentSelfManagedCluster:
		if spec.Cluster == "" {
			return Topology{}, fmt.Errorf("deployment %q requires a cluster name", kind)
		}
		t.DistributedTables = true
		t.ReplicatedEngines = true
		t.PerNodeAdmin = true
		t.ShardRouting = true
		t.DDLCluster = spec.Cluster
		t.FanoutCluster = spec.Cluster
	case DeploymentCloud:
		if spec.Cluster != "" {
			return Topology{}, fmt.Errorf("deployment %q does not accept a cluster name (got %q): Cloud replicates without a Distributed layer", kind, spec.Cluster)
		}
		t.ManagedStorage = true
		t.FanoutCluster = cloudFanoutCluster
		t.fanoutDefaulted = true
	default:
		return Topology{}, fmt.Errorf("unknown deployment %q: want %s, %s or %s",
			kind, DeploymentSingleNode, DeploymentSelfManagedCluster, DeploymentCloud)
	}

	if spec.FanoutCluster != "" {
		t.FanoutCluster = spec.FanoutCluster
		t.fanoutDefaulted = false
	}

	if t.DDLCluster != "" && !validDDLClusterName.MatchString(t.DDLCluster) {
		return Topology{}, fmt.Errorf("invalid cluster name %q: must be alphanumeric, hyphens, or underscores only", t.DDLCluster)
	}
	if t.FanoutCluster != "" && !validFanoutClusterName.MatchString(t.FanoutCluster) {
		return Topology{}, fmt.Errorf("invalid fanout cluster name %q: must be alphanumeric, hyphens or underscores, with at most one dot", t.FanoutCluster)
	}

	return t, nil
}

// OnClusterSQL returns the ON CLUSTER clause for DDL, or an empty string when
// the deployment has no DDL cluster.
func (t Topology) OnClusterSQL() string {
	if t.DDLCluster == "" {
		return ""
	}
	return " ON CLUSTER '" + EscCHStr(t.DDLCluster) + "'"
}

// FanoutSystemTable returns the every-replica form of a system table, e.g.
// clusterAllReplicas('default', system.parts). Returns the bare table name when
// the deployment does not fan out.
func (t Topology) FanoutSystemTable(table string) string {
	if t.FanoutCluster == "" {
		return table
	}
	return fmt.Sprintf("clusterAllReplicas('%s', %s)", EscCHStr(t.FanoutCluster), table)
}

// FanoutSystemTableArgs returns the three-argument every-replica form that some
// system tables require, e.g. clusterAllReplicas('c', system, distribution_queue).
// db and table are identifiers, not string literals.
func (t Topology) FanoutSystemTableArgs(db, table string) string {
	if t.FanoutCluster == "" {
		return db + "." + table
	}
	return fmt.Sprintf("clusterAllReplicas('%s', %s, %s)", EscCHStr(t.FanoutCluster), db, table)
}

// ShardSystemTable returns the one-replica-per-shard form, cluster('c', system.X).
// Callers measuring stored bytes want this rather than every replica: asking all
// replicas would multiply identical rows by the replication factor.
func (t Topology) ShardSystemTable(table string) string {
	if t.FanoutCluster == "" {
		return table
	}
	return fmt.Sprintf("cluster('%s', %s)", EscCHStr(t.FanoutCluster), table)
}

// Table returns the read/write name for a base table, adding the _distributed
// suffix when reads and writes fan out across shards.
func (t Topology) Table(base string) string {
	if t.DistributedTables {
		return base + "_distributed"
	}
	return base
}

// String returns a one-line operator-facing summary for startup logs.
func (t Topology) String() string {
	var b strings.Builder
	b.WriteString(string(t.Kind))
	if t.DDLCluster != "" {
		fmt.Fprintf(&b, " ddl_cluster=%s", t.DDLCluster)
	}
	if t.FanoutCluster != "" {
		fmt.Fprintf(&b, " fanout_cluster=%s", t.FanoutCluster)
	}
	if t.DistributedTables {
		b.WriteString(" distributed_tables")
	}
	if t.ManagedStorage {
		b.WriteString(" managed_storage")
	}
	return b.String()
}
