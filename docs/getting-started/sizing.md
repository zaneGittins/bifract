# Kubernetes Sizing Guide

The `--install-k8s` wizard includes six resource profiles. Choose the one that matches your expected daily ingest volume.

## Resource Profiles

| Profile | Ingest | Nodes | CH Shards x Replicas | CH CPU (per pod) | CH Memory (per pod) |
|---|---|---|---|---|---|
| Dev | ~1-10 GB/day | 3x 4vCPU/8GB | 1x2 | 2 / 3 | 4Gi / 5Gi |
| X-Small | ~10-50 GB/day | 3x 8vCPU/16GB | 1x2 | 6 / 8 | 8Gi / 12Gi |
| Small | ~50-200 GB/day | 3x 16vCPU/32GB | 1x2 | 10 / 12 | 12Gi / 24Gi |
| Medium | ~200-500 GB/day | 3x 24vCPU/48GB | 2x2 | 8 / 12 | 12Gi / 24Gi |
| Large | ~500 GB-2 TB/day | 3x 32vCPU/96GB | 3x2 | 8 / 16 | 16Gi / 32Gi |
| X-Large | ~2-10 TB/day | 6x 32vCPU/96GB | 6x2 | 8 / 16 | 16Gi / 32Gi |

All CPU/memory values shown as request / limit. Medium and above per-pod resources decrease as sharding distributes the work across more pods. Full resource details for all components (Bifract, PostgreSQL, Caddy, LiteLLM, Keeper) are defined in the generated manifests.

## How Sharding Works

ClickHouse shards distribute data horizontally. Each shard holds a subset of the data, and queries fan out across all shards in parallel. Replicas within a shard provide high availability.

Dev through Small use a single shard. Medium introduces 2 shards to distribute write load. Large and X-Large add more shards for higher throughput. The wizard pre-fills shard and replica counts based on the selected profile, but you can adjust them before generating manifests.