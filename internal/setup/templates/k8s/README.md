# Kubernetes manifests

**This directory is the only place Kubernetes manifests live.** They are Go templates
rendered by `bifract --install-k8s` into the user's output directory, which is then applied
with `kubectl apply -k`. There is no second copy to keep in sync.

A `deploy/k8s/` tree of static manifests used to sit alongside these and was deleted, for
two reasons worth remembering before anyone recreates it:

- It rotted. It was referenced by nothing (no embed, no docs, no CI), so nothing caught the
  drift. By the end it was missing the ClickHouse load-balancer Service, which meant
  multi-shard could not work from it at all, and its ingest workload had diverged into a
  Deployment with an `emptyDir` spool while the template here is a StatefulSet with a
  per-pod PVC.
- It could not have been correct. These templates are conditional on shard count, mTLS,
  resource profile, domain, and image tag. Any static copy is a snapshot of one arbitrary
  configuration, so "keeping it in sync" is not a thing that can be done.

Fixes to a manifest belong here and nowhere else. `internal/setup/k8s_yaml_valid_test.go`
renders every template and parses the result, so a mis-indented block or an unresolved
field fails at `go test` rather than at `kubectl apply`.
