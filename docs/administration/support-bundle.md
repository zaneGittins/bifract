# Support Bundle

`scripts/collect-logs.sh` (in the Bifract repository) gathers what support needs to diagnose a problem into one archive. It detects Docker Compose or Kubernetes on its own and only reads; it never restarts, deletes or changes anything.

## Collect a bundle

1. Run it on the host as a user in the `docker` group (Compose), or with a kubeconfig that can `exec` into pods (Kubernetes):

    ```bash
    ./scripts/collect-logs.sh
    ```

2. Send the resulting `bifract-support-<hostname>-<timestamp>.zip` to support. Without `zip`, it writes a `.tar.gz`.

To look through the contents yourself, write a directory instead: `./scripts/collect-logs.sh --dir bifract-support`. Start with `SUMMARY.txt`.

| Option | Description |
|--------|-------------|
| `--dir <path>` | Write a directory instead of an archive |
| `--docker` / `--k8s` | Force the platform |
| `--namespace <ns>` | Kubernetes namespace. Default `bifract` |
| `--tail <n>` | Log lines per container. Default 3000 |
| `--quick` | Skip the 8 second second sample that tells stuck merges from moving ones |

## What it collects

| Folder | Contents |
|--------|----------|
| `host/` | OS, CPU, memory, disk, load, OOM-killer evidence, listening ports |
| `docker/` or `kubernetes/` | Container or pod inventory and describes, events, manifests, PVCs, network policies, operator logs |
| `bifract/` | Version, image tags and digests, reverse proxy config |
| `logs/` | Tail of every container log, an `errors/` excerpt per component, and the previous instance of crash-looping containers |
| `clickhouse/nodes/<node>/` | Version, topology, disks, settings, metrics, merges, mutations, queues, parts, table DDL, migrations, errors, failed and slow queries, server log |
| `clickhouse/consistency/` | Cross-node checks: schema drift, migration numbers, stuck merges |
| `postgres/` | Version, migrations, settings, table sizes, object counts, alert and archive state, activity and locks |

`SUMMARY.txt` is a one-page triage view, `README.txt` describes the layout, and `collection-errors.txt` lists anything that could not be collected.

## Size and privacy

- Output is capped at 4 MB per command, 6 MB per container log and 60 log targets. A single-node bundle is usually a few hundred KB.
- ClickHouse queries time out after 45 seconds and read system tables, never `logs`.
- The run stops if the target filesystem has less than 1 GB free.
- Values under secret-looking keys (passwords, tokens, API and access keys, bearer tokens, credentials in URLs) become `***REDACTED***`. Kubernetes secret values are never read.

!!! note
    The bundle still holds operational data: container log lines, query text (first 300 characters), table and column names, fractal IDs and object counts. Review it before sending if that matters.
