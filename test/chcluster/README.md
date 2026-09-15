# Two-shard ClickHouse for cluster schema tests

Local stand-in for a multi-shard deployment, used by `pkg/storage/clusterinit_manual_test.go`
(build tag `chcluster`). It covers the per-shard branches of `Initialize` that a single-node
docker install never reaches: fresh provisioning, repair of a shard with no schema, and
stamping a shard whose schema is current but unrecorded.

Ports 19001/19002 and project name `chtest` keep it clear of the main compose stack.

```bash
docker compose -p chtest -f test/chcluster/docker-compose.yml up -d
go test -tags chcluster ./pkg/storage/ -run TestClusterInit -v
docker compose -p chtest -f test/chcluster/docker-compose.yml down -v
```

Each test drops and recreates the `logs` database. Re-creating Replicated tables over
Keeper paths that a previous run left behind takes far longer than a genuine fresh install
(tens of seconds per shard versus a couple), so treat the timings as meaningless. `down -v`
between runs restores the fast path.

## Shard credentials

`cluster.xml` gives each replica a user and password. The nodes require one, so without
them no Distributed read works at all: every cross-shard path, dictionaries included, fails
with "Authentication failed" from the remote node rather than anything describing the cause.
In k8s the ClickHouse operator generates `remote_servers` and supplies these itself.
