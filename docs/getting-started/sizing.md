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

### Capacity for everything else

The shard nodes hold ClickHouse and little else. The app tier, the ingest tier and its
archiver sidecar, archive maintenance, PostgreSQL, Caddy and LiteLLM need their own capacity.

| Profile | Non-ClickHouse CPU (req/lim) | Non-ClickHouse memory (req/lim) |
|---|---|---|
| Dev | 3 / 8 | 5Gi / 10Gi |
| X-Small | 3 / 10 | 5Gi / 14Gi |
| Small | 4 / 14 | 10Gi / 20Gi |
| Medium | 5 / 18 | 12Gi / 30Gi |
| Large | 9 / 24 | 18Gi / 44Gi |
| X-Large | 14 / 40 | 28Gi / 84Gi |

Requests decide whether pods schedule; limits are the burst ceiling. Size a node pool for at
least the request column with room to spare: a cluster provisioned only for the shard nodes
cannot place these pods. Figures are for a single ingest replica; add the ingest pod's own
figures again for each additional replica.

Two components dominate. The **ingest pod** carries both the ingest container and the archiver
sidecar, making it the largest single pod outside ClickHouse (Large: 5Gi requested, up to 16Gi;
X-Large: 9Gi up to 32Gi). Its memory is driven by `ARCHIVE_MAX_PENDING_BYTES`, the buffer the
archiver accumulates before rolling Parquet, so raising the roll thresholds raises this pod.
**Archive maintenance** is sized separately because compaction decodes Parquet row groups into
Arrow, and its memory limit also sets compaction's parallelism.

Full per-component resources are in the generated manifests.

## Storage

ClickHouse is I/O-bound on both ingest and query, so the shard nodes need fast storage. Size the volume for your retention, and make sure the disk meets this performance floor:

| Requirement | Minimum | Recommended (Large / X-Large) |
|---|---|---|
| Random-read IOPS (per node) | >= 5,000 provisioned | 50,000+ (NVMe) |
| Read latency (per node) | single-digit millisecond (< 10 ms) | sub-millisecond (< 1 ms) |

## Ingress Throughput

Everything reaches Bifract through the `caddy` Service, which on a managed cluster provisions a
cloud load balancer. **That load balancer is sized by your cloud provider's default, not by the
Bifract profile you chose**, and at high ingest rates it is a common throughput ceiling.

It is hard to diagnose because it is invisible from inside the cluster. Every component reports
healthy, CPU sits idle, ClickHouse shows no insert delays, and median latency looks fine. Only
the tail moves: p50 stays flat while p95 and p99 climb into seconds, throughput plateaus below
target, and client connection errors accumulate.

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
    `externalTrafficPolicy: Local`, all ingress traffic lands on one node. That is sufficient at
    the rates above once the load balancer is sized correctly, but it is a structural ceiling
    past Large.

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

Each shard holds a subset of the data, and queries fan out across all shards in parallel. Dev
through Small use a single shard. Medium introduces 2 shards to distribute write and query load;
Large and X-Large add more for higher throughput.

A ClickHouse node loss is recovered by re-provisioning the shard and replaying from the Iceberg
archive. See [Backup & Archive](../administration/backup-restore.md) for how the archive and
recovery work.
