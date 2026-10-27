# Kubernetes Sizing Guide

Pick a resource profile in `bifract --install-k8s` (or `--size` on `--upgrade-k8s` and `--reconfigure-k8s`) to match your daily ingest. The profile sets CPU and memory for every component, the default ClickHouse shard count, and ingest concurrency.

Bifract scales by sharding ClickHouse. Each shard is a single replica; the durable copy of every log is the [Apache Iceberg archive](../administration/backup-restore.md#iceberg-archive) on object storage.

!!! warning "Durability is not the same as fast recovery"
    Replaying archived data into ClickHouse costs roughly what ingesting it did. At Large or X-Large, restoring months of history takes days to weeks and may not fit shard disks sized for a shorter window. Size the hot store for the window you need online and query older data in place with Recall. See [Recovery time](../administration/backup-restore.md#recovery-time).

## Resource Profiles

| Profile | Ingest | Shards | Node (per shard) | CH CPU (req/lim) | CH Memory (req/lim) |
|---|---|---|---|---|---|
| Dev | ~1-10 GB/day | 1 | 4 vCPU / 8GB | 2 / 3 | 4Gi / 5Gi |
| X-Small | ~10-50 GB/day | 1 | 8 vCPU / 16GB | 6 / 8 | 8Gi / 12Gi |
| Small | ~50-200 GB/day | 1 | 16 vCPU / 32GB | 10 / 12 | 12Gi / 24Gi |
| Medium | ~200-500 GB/day | 2 | 24 vCPU / 48GB | 12 / 20 | 20Gi / 40Gi |
| Large | ~500 GB-2 TB/day | 3 | 32 vCPU / 64GB | 16 / 28 | 28Gi / 56Gi |
| X-Large | ~2-10 TB/day | 6 | 32 vCPU / 64GB | 16 / 28 | 28Gi / 56Gi |

**Node (per shard)** is the worker node that hosts one ClickHouse shard pod; ClickHouse takes most of it. The reference point is **500 GB/day on 3 shards of 32 vCPU / 64GB**.

## Ingest concurrency

Each profile sets `BIFRACT_INGEST_WORKERS` (concurrent ClickHouse inserts per ingest pod) and `BIFRACT_INGEST_QUEUE_SIZE` to match the ClickHouse CPU it provisions. Inserts are CPU-bound, so too few workers leaves ClickHouse idle and caps ingest below what the cluster can take.

| Profile | Workers | Queue size |
|---|---|---|
| Dev | 4 | 100 |
| X-Small | 8 | 200 |
| Small | 12 | 300 |
| Medium | 24 | 500 |
| Large | 32 | 1000 |
| X-Large | 48 | 2000 |

See [Ingestion tuning](../ingestion/tuning.md) for what these settings do.

## Storage

ClickHouse is IO-bound for both ingest and queries. Size each shard volume for your retention, on disks that meet this floor:

| Requirement | Minimum | Recommended (Large / X-Large) |
|---|---|---|
| Random-read IOPS (per node) | 5,000 provisioned | 50,000+ (NVMe) |
| Read latency (per node) | under 10 ms | under 1 ms |

## Ingress throughput

All traffic enters through the `caddy` Service, which provisions a cloud load balancer on managed clusters. Size it for your peak ingest rate.

=== "DigitalOcean"
    ```bash
    kubectl -n bifract annotate svc caddy \
      service.beta.kubernetes.io/do-loadbalancer-size-unit="4" --overwrite
    ```

=== "AWS"
    Use an NLB rather than the default Classic Load Balancer:
    ```bash
    kubectl -n bifract annotate svc caddy \
      service.beta.kubernetes.io/aws-load-balancer-type="nlb" --overwrite
    ```

=== "Azure / GKE"
    Both scale with load and normally need no annotation. Confirm against your target rate.

=== "Bare metal"
    With MetalLB or similar, the limit is the ingress node's NIC and the Caddy pod itself.
