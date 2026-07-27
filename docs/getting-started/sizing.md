# Kubernetes Sizing Guide

The `--install-k8s` wizard includes six resource profiles. Choose the one that matches your expected daily ingest volume.

Bifract on Kubernetes scales by **sharding** ClickHouse. Each shard is a **single replica** — Bifract does not use ClickHouse replication for durability or disaster recovery. Instead, every log is copied to the [Apache Iceberg archive](../administration/backup-restore.md) on object storage, which is the durable copy of record. This keeps the cluster lean: ClickHouse shards for query and ingest throughput, Iceberg for durability.

!!! warning "Durability is not the same as fast recovery"
    The archive guarantees your log data survives a ClickHouse loss. It does **not** mean a large hot store can be rebuilt quickly. Replaying archived data into ClickHouse is bounded by MergeTree merge throughput, so it costs roughly what ingesting the same data cost in the first place. At Large or X-Large, restoring months of history takes **days to weeks** and may exceed the shard disks you sized for a shorter hot window. Plan recovery as "restore the operational window you need, and query the rest in place with [Recall](../administration/backup-restore.md#restore--reconcile)". See [Disaster Recovery](../administration/backup-restore.md#disaster-recovery).

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

## Ingress Throughput

Everything reaches Bifract through the `caddy` Service, which on a managed Kubernetes cluster
provisions a cloud load balancer. **That load balancer is sized by your cloud provider's
default, not by the Bifract profile you chose**, and at high ingest rates it is a common
throughput ceiling.

This failure mode is difficult to diagnose because it is invisible from inside the cluster.
Every component reports healthy, CPU sits idle, ClickHouse shows no insert delays, and the
median request latency looks fine. Only the tail moves: p50 stays flat while p95 and p99 climb
into seconds, throughput plateaus below target, and client connection errors accumulate.

Measured on a 3-shard Large cluster targeting 500 GB/day, the only change being the load
balancer size:

| | Default size | Scaled up |
|---|---|---|
| Sustained ingest | 7,867 events/sec | **9,867 events/sec** |
| p95 | 9,430 ms | **196 ms** |
| p99 | 19,433 ms | **1,527 ms** |
| Client connection errors | accumulating | **none** |

Size the load balancer for your target ingest rate. The annotation is provider-specific, so it
is not set in the generated manifests:

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

!!! note "Caddy does not scale horizontally"
    Caddy runs a single replica with a ReadWriteOnce volume. Combined with
    `externalTrafficPolicy: Local`, all ingress traffic lands on one node. This is sufficient
    at the rates above once the load balancer is sized correctly, but it is a structural
    ceiling worth knowing before you push well past Large.

## Ingest Concurrency

`BIFRACT_INGEST_WORKERS` on the ingest tier controls how many inserts run against ClickHouse
concurrently. ClickHouse inserts are **CPU-bound building skip indexes**, not disk-bound
(measured: 684 ms CPU versus 12 ms disk per insert), so throughput scales with concurrent
inserts until the shard CPUs are saturated.

Each profile sets its own default (Dev 4, X-Small 8, Small 12, Medium 24, Large 32, X-Large 48),
sized against that profile's shard CPU. Raise it only if you have moved off the profile default
or your event shape is unusually cheap to index. Going from 8 to 32 on a 3-shard Large cluster
moved sustained ingest from 6,800 to 8,966 events/sec with no other change:

```bash
kubectl -n bifract set env statefulset/bifract-ingest BIFRACT_INGEST_WORKERS=32
```

If ClickHouse CPU is well below its limit while ingest throughput is below target, this is the
first thing to raise.

## How Sharding Works

ClickHouse shards distribute data horizontally. Each shard holds a subset of the data, and queries fan out across all shards in parallel. Dev through Small use a single shard. Medium introduces 2 shards to distribute write and query load; Large and X-Large add more shards for higher throughput.

Durability does not come from ClickHouse. A ClickHouse node loss is recovered by re-provisioning the shard and replaying from the Iceberg archive. Size that expectation by how much data you actually need back in the hot store rather than by total history: replay speed is governed by merge throughput, not by how fast object storage can be read. See [Backup & Restore](../administration/backup-restore.md) for how the archive and recovery work.