# Support Bundle

When something goes wrong, `scripts/collect-logs.sh` gathers everything needed to diagnose it into a single archive you can send to support. It takes no arguments and detects whether you are running docker compose or Kubernetes.

```bash
./scripts/collect-logs.sh
```

The result is `bifract-support-<hostname>-<timestamp>.zip` in the current directory.

To inspect the contents yourself instead of producing an archive, write to a directory:

```bash
./scripts/collect-logs.sh --dir bifract-support
```

## Options

| Option | Description |
|--------|-------------|
| `--dir <path>` | Write to a new directory instead of an archive. |
| `--docker` / `--k8s` | Force the platform instead of auto-detecting. |
| `--namespace <ns>` | Kubernetes namespace. Default `bifract`. |
| `--tail <n>` | Log lines collected per container. Default 3000. |
| `--quick` | Skip the 8 second second-sample used to tell stuck merges from moving ones. |

Run it as a user that can reach the platform: a member of the `docker` group for compose installs, or with a kubeconfig that can `exec` into pods for Kubernetes.

## What it collects

| Area | Contents |
|------|----------|
| `host/` | OS, CPU, memory, disk, load, OOM-killer evidence, listening ports. |
| `docker/` or `kubernetes/` | Daemon or cluster inventory, container inspect or pod describes, events, manifests, PVCs, network policies, operator logs. |
| `bifract/` | Version banner, image tags and digests, reverse proxy config. |
| `logs/` | Tail of every container log, plus an `errors/` excerpt per component and the previous container instance for crash loops. |
| `clickhouse/nodes/<node>/` | Version, cluster topology, disks and storage policies, non-default settings, metrics, active merges, mutations, part moves, distribution and DDL queues, part counts, detached parts, table DDL, applied migrations, error counters, failed and slow queries, server log. |
| `clickhouse/consistency/` | Cross-node analysis: local vs Distributed schema, schema across shards, migration number per node, merges moving or stuck. |
| `postgres/` | Version, migrations, runtime settings, table sizes, object counts, alert state, archive state, current activity and locks. |

`SUMMARY.txt` at the top of the bundle is a one-page triage view: version, workload health, migration numbers, schema mismatches, stuck merges, and which component logged the most errors. `README.txt` describes the full layout. `collection-errors.txt` lists anything the run could not collect.

## Size and safety

The collector only reads. It never restarts, deletes, or reconfigures anything, and every ClickHouse query it runs is capped at 45 seconds and reads rollup or system tables rather than scanning `logs`.

Output is capped so it stays small enough to email and cannot fill the disk: 4 MB per captured command, 6 MB and 3000 lines per container log, 60 log targets. A typical single-node bundle is a few hundred kilobytes compressed. The run aborts if less than 1 GB is free on the target filesystem.

## Redaction

Values whose key names indicate a secret (passwords, tokens, API and access keys, credentials, bearer tokens, credentials embedded in URLs) are replaced with `***REDACTED***`. Kubernetes secret values are never read at all; only secret and key names are listed.

The bundle does still contain operational data: container log lines, query text truncated to 300 characters, table and column names, fractal ids, and object counts.