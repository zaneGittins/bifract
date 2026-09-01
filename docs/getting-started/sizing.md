# Kubernetes Sizing Guide

The `--install-k8s` wizard includes six resource profiles. Choose the one that matches your expected daily ingest volume.

Bifract on Kubernetes scales by **sharding** ClickHouse. Each shard is a **single replica**: Bifract does not use ClickHouse replication for durability. Every log is copied to the [Apache Iceberg archive](../administration/backup-restore.md) on object storage instead, which is the durable copy of record. ClickHouse shards for query and ingest throughput, Iceberg for durability.

!!! warning "Durability is not the same as fast recovery"
    Replaying archived data into ClickHouse costs roughly what ingesting it cost, so at Large or X-Large restoring months of history takes days to weeks and may exceed shard disks sized for a shorter hot window. Size the hot store for the window you need online, and query the rest in place with Recall. See [Recovery time](../administration/backup-restore.md#recovery-time).

## Resource Profiles

| Profile | Ingest | Shards | Node (per shard) | CH CPU (req/lim) | CH Memory (req/lim) |
|---|---|---|---|---|---|
| Dev | ~1-10 GB/day | 1 | 4 vCPU / 8GB | 2 / 3 | 4Gi / 5Gi |
| X-Small | ~10-50 GB/day | 1 | 8 vCPU / 16GB | 6 / 8 | 8Gi / 12Gi |
| Small | ~50-200 GB/day | 1 | 16 vCPU / 32GB | 10 / 12 | 12Gi / 24Gi |
| Medium | ~200-500 GB/day | 2 | 24 vCPU / 48GB | 12 / 20 | 20Gi / 40Gi |
| Large | ~500 GB-2 TB/day | 3 | 32 vCPU / 64GB | 16 / 28 | 28Gi / 56Gi |
| X-Large | ~2-10 TB/day | 6 | 32 vCPU / 64GB | 16 / 28 | 28Gi / 56Gi |

All CPU/memory values are request / limit. **Node (per shard)** is the worker node hosting a ClickHouse shard pod; the ClickHouse container takes the bulk of it. The recommended baseline for **500 GB/day** is **3 shards at 32 vCPU / 64GB per node**.

## Storage

ClickHouse is IO bound on both ingest and query, so the shard nodes need fast storage. Size the volume for your retention, and make sure the disk meets this performance floor:

| Requirement | Minimum | Recommended (Large / X-Large) |
|---|---|---|
| Random-read IOPS (per node) | >= 5,000 provisioned | 50,000+ (NVMe) |
| Read latency (per node) | single-digit millisecond (< 10 ms) | sub-millisecond (< 1 ms) |

## Ingress Throughput

Everything reaches Bifract through the `caddy` Service, which on a managed cluster provisions a
cloud load balancer. Size the load balancer for your target ingest rate.

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
    Both scale automatically with load; no annotation is normally required. Confirm the
    behaviour matches your target rate before assuming it.

=== "Bare metal"
    With MetalLB or similar there is no managed load balancer, so the ceiling is your
    ingress node's NIC and the Caddy pod itself.