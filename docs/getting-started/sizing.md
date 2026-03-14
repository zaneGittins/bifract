# Kubernetes Sizing Guide

When deploying Bifract on Kubernetes, choosing the right resource profile ensures stable performance under your expected workload. The `--install-k8s` wizard includes a resource profile selector with five presets. This guide explains each profile and helps you pick the right one.

## Resource Profiles

### X-Small

**Use case:** Development, staging, or light production workloads.

**Recommended cluster:** 3 nodes, 8 vCPU / 16GB RAM each.

| Component | CPU Request / Limit | Memory Request / Limit |
|---|---|---|
| ClickHouse (per replica) | 1 / 4 | 4Gi / 16Gi |
| ClickHouse Keeper | 250m / 1 | 512Mi / 1Gi |
| Bifract | 500m / 2 | 512Mi / 2Gi |
| PostgreSQL | 250m / 2 | 512Mi / 2Gi |
| Caddy | 100m / 1 | 128Mi / 512Mi |
| LiteLLM | 100m / 500m | 256Mi / 512Mi |

**Default shards/replicas:** 1 shard, 2 replicas.

### Small

**Use case:** Light production workloads, up to approximately 1 TB/day of raw log ingest.

**Recommended cluster:** 3 nodes, 8 vCPU / 32GB RAM each.

| Component | CPU Request / Limit | Memory Request / Limit |
|---|---|---|
| ClickHouse (per replica) | 2 / 4 | 6Gi / 20Gi |
| ClickHouse Keeper | 250m / 1 | 512Mi / 1Gi |
| Bifract | 500m / 2 | 512Mi / 2Gi |
| PostgreSQL | 500m / 2 | 1Gi / 2Gi |
| Caddy | 100m / 1 | 128Mi / 512Mi |
| LiteLLM | 100m / 500m | 256Mi / 512Mi |

**Default shards/replicas:** 1 shard, 2 replicas.

### Medium

**Use case:** Production workloads, approximately 1-2 TB/day of raw log ingest.

**Recommended cluster:** 3 nodes, 16 vCPU / 32GB RAM each.

| Component | CPU Request / Limit | Memory Request / Limit |
|---|---|---|
| ClickHouse (per replica) | 2 / 8 | 8Gi / 24Gi |
| ClickHouse Keeper | 500m / 2 | 1Gi / 2Gi |
| Bifract | 1 / 4 | 1Gi / 4Gi |
| PostgreSQL | 500m / 4 | 1Gi / 4Gi |
| Caddy | 250m / 2 | 256Mi / 1Gi |
| LiteLLM | 100m / 500m | 256Mi / 512Mi |

**Default shards/replicas:** 2 shards, 2 replicas (4 ClickHouse pods). Sharding begins at this tier to distribute write load and parallelize queries across nodes.

### Large

**Use case:** High-volume production, approximately 2-10 TB/day of raw log ingest.

**Recommended cluster:** 3 nodes, 32 vCPU / 64GB RAM each.

| Component | CPU Request / Limit | Memory Request / Limit |
|---|---|---|
| ClickHouse (per replica) | 4 / 16 | 16Gi / 48Gi |
| ClickHouse Keeper | 500m / 2 | 1Gi / 2Gi |
| Bifract | 2 / 8 | 2Gi / 8Gi |
| PostgreSQL | 1 / 4 | 2Gi / 8Gi |
| Caddy | 500m / 2 | 512Mi / 1Gi |
| LiteLLM | 250m / 1 | 512Mi / 1Gi |

**Default shards/replicas:** 3 shards, 2 replicas (6 ClickHouse pods).

### X-Large

**Use case:** Very high-volume production, 10+ TB/day of raw log ingest.

**Recommended cluster:** 6 nodes, 32 vCPU / 64GB RAM each.

| Component | CPU Request / Limit | Memory Request / Limit |
|---|---|---|
| ClickHouse (per replica) | 8 / 32 | 32Gi / 96Gi |
| ClickHouse Keeper | 1 / 2 | 2Gi / 4Gi |
| Bifract | 4 / 16 | 4Gi / 16Gi |
| PostgreSQL | 2 / 8 | 4Gi / 16Gi |
| Caddy | 1 / 4 | 1Gi / 2Gi |
| LiteLLM | 500m / 2 | 1Gi / 2Gi |

**Default shards/replicas:** 6 shards, 2 replicas (12 ClickHouse pods).

## How Sharding Works

ClickHouse shards distribute data horizontally across multiple nodes. Each shard holds a subset of the data, and queries fan out across all shards in parallel. Replicas within a shard provide high availability.

- **X-Small and Small** use a single shard. All data lives on every replica, keeping things simple.
- **Medium** introduces 2 shards. Write load is split across shards and queries run in parallel.
- **Large** uses 3 shards for higher throughput.
- **X-Large** uses 6 shards, mapping well to 6-node clusters for data locality.

The wizard pre-fills shard and replica counts based on the selected profile, but you can adjust them before generating manifests.

## Choosing a Profile

The ingest estimates assume a mix of concurrent queries, active alerts, and steady ingest. A cluster handling only ingest with no queries could sustain higher volume, but sizing for realistic production use is the right approach.

Key factors that affect capacity beyond raw ingest volume:

- **Number of concurrent queries** - Search and dashboard queries compete with ingest for CPU and memory.
- **Alert count and frequency** - Each alert evaluation runs a query against ClickHouse.
- **Query complexity** - Aggregations and wide time ranges use more resources than simple filters.
- **Log size and cardinality** - Larger logs and high-cardinality fields increase storage and memory pressure.

When in doubt, start with the profile that matches your node sizes and monitor ClickHouse resource usage. You can always adjust resource limits in the generated manifests without re-running the wizard.