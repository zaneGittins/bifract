# Kubernetes Sizing Guide

The `--install-k8s` wizard includes six resource profiles. Choose the one that matches your expected daily ingest volume.

Bifract on Kubernetes scales by **sharding** ClickHouse. Each shard is a **single replica** — Bifract does not use ClickHouse replication for durability or disaster recovery. Instead, every log is copied to the [Apache Iceberg archive](../administration/backup-restore.md) on object storage, which is the source of truth for DR. This keeps the cluster lean: ClickHouse shards for query and ingest throughput, Iceberg for durability.

## Resource Profiles

| Profile | Ingest | Shards | Node (per shard) | CH CPU (req/lim) | CH Memory (req/lim) |
|---|---|---|---|---|---|
| Dev | ~1-10 GB/day | 1 | 4 vCPU / 8GB | 2 / 3 | 4Gi / 5Gi |
| X-Small | ~10-50 GB/day | 1 | 8 vCPU / 16GB | 6 / 8 | 8Gi / 12Gi |
| Small | ~50-200 GB/day | 1 | 16 vCPU / 32GB | 10 / 12 | 12Gi / 24Gi |
| Medium | ~200-500 GB/day | 2 | 24 vCPU / 48GB | 12 / 20 | 20Gi / 40Gi |
| Large | ~500 GB-2 TB/day | 3 | 32 vCPU / 64GB | 16 / 28 | 28Gi / 56Gi |
| X-Large | ~2-10 TB/day | 6 | 32 vCPU / 64GB | 16 / 28 | 28Gi / 56Gi |

All CPU/memory values shown as request / limit. The **Node (per shard)** column is the worker node that hosts a ClickHouse shard pod; the ClickHouse container is sized to take the bulk of it. The recommended baseline for **500 GB/day** is **3 shards at 32 vCPU / 64GB per node**. Full resource details for all components (Bifract, PostgreSQL, Caddy, LiteLLM, Keeper) are defined in the generated manifests.

## Storage

ClickHouse is I/O-bound on both ingest and query, so the shard nodes need fast storage. Size the volume for your retention, and make sure the disk meets this performance floor:

| Requirement | Minimum | Recommended (Large / X-Large) |
|---|---|---|
| Random-read IOPS (per node) | >= 5,000 provisioned | 50,000+ (NVMe) |
| Read latency (per node) | single-digit millisecond (< 10 ms) | sub-millisecond (< 1 ms) |

## How Sharding Works

ClickHouse shards distribute data horizontally. Each shard holds a subset of the data, and queries fan out across all shards in parallel. Dev through Small use a single shard. Medium introduces 2 shards to distribute write and query load; Large and X-Large add more shards for higher throughput.

Durability does not come from ClickHouse. A ClickHouse node loss is recovered by re-provisioning the shard and restoring from the Iceberg archive. See [Backup & Restore](../administration/backup-restore.md) for how the archive and point-in-time recovery work.