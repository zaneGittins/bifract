#!/usr/bin/env bash
# Drops the logs database on every ClickHouse shard and recreates it using
# the local db/init-clickhouse.sql (which includes GRANULARITY 1 on the
# inverted index). Clears all log data, stuck merges, mutations, and the
# replication queue. Fractal configs (Postgres) are not affected.
#
# Run from the repo root: ./scripts/reset-clickhouse-schema.sh
# Requires: kubectl configured for the bifract cluster.

set -euo pipefail

NAMESPACE="bifract"
SECRET_NAME="bifract-secrets"
CH_USER="default"
INIT_SQL="db/init-clickhouse.sql"

# ── pre-flight ────────────────────────────────────────────────────────────────
if [[ ! -f "${INIT_SQL}" ]]; then
  echo "ERROR: ${INIT_SQL} not found. Run this script from the repo root." >&2
  exit 1
fi

echo "WARNING: This will DROP the logs database on all ClickHouse shards."
echo "         All ingested log data will be permanently deleted."
echo "         Fractal configs, secrets, and Kubernetes resources are NOT affected."
echo ""
read -r -p "Type 'yes' to continue: " confirm
if [[ "${confirm}" != "yes" ]]; then
  echo "Aborted."
  exit 0
fi
echo ""

# ── auth ──────────────────────────────────────────────────────────────────────
echo "Fetching ClickHouse password from secret ${SECRET_NAME}..."
CH_PASS=$(kubectl get secret -n "${NAMESPACE}" "${SECRET_NAME}" \
  -o jsonpath='{.data.CLICKHOUSE_PASSWORD}' | base64 -d)

if [[ -z "${CH_PASS}" ]]; then
  echo "ERROR: Could not retrieve CLICKHOUSE_PASSWORD from secret ${SECRET_NAME}" >&2
  exit 1
fi

# ── pod discovery ─────────────────────────────────────────────────────────────
PODS=$(kubectl get pods -n "${NAMESPACE}" \
  -l "app.kubernetes.io/name=clickhouse-server" \
  --no-headers -o custom-columns=":metadata.name" 2>/dev/null || true)

if [[ -z "${PODS}" ]]; then
  PODS=$(kubectl get pods -n "${NAMESPACE}" --no-headers \
    -o custom-columns=":metadata.name" | grep "^bifract-ch" || true)
fi

if [[ -z "${PODS}" ]]; then
  echo "ERROR: No ClickHouse pods found in namespace ${NAMESPACE}" >&2
  exit 1
fi

POD_COUNT=$(echo "${PODS}" | wc -l | tr -d ' ')
echo "Found ${POD_COUNT} pod(s): $(echo "${PODS}" | tr '\n' ' ')"
echo ""

# ── helper ────────────────────────────────────────────────────────────────────
run_sql() {
  local pod="$1"
  local sql="$2"
  kubectl exec -n "${NAMESPACE}" "${pod}" -- \
    clickhouse-client --user="${CH_USER}" --password="${CH_PASS}" \
    --query="${sql}"
}

# ── drop + recreate on each pod ───────────────────────────────────────────────
for POD in ${PODS}; do
  echo "=== ${POD} ==="

  echo "  Dropping logs database (clears all data, mutations, merges, replication queue)..."
  # ClickHouse refuses to drop tables >50 GB without a force flag file.
  kubectl exec -n "${NAMESPACE}" "${POD}" -- \
    sh -c 'touch /var/lib/clickhouse/flags/force_drop_table && chmod 666 /var/lib/clickhouse/flags/force_drop_table'
  run_sql "${POD}" "DROP DATABASE IF EXISTS logs"
  echo "  Dropped."

  echo "  Recreating schema from ${INIT_SQL}..."
  kubectl exec -i -n "${NAMESPACE}" "${POD}" -- \
    clickhouse-client --user="${CH_USER}" --password="${CH_PASS}" \
    < "${INIT_SQL}"
  echo "  Schema created."
  echo ""
done

# ── verify ────────────────────────────────────────────────────────────────────
echo "Verifying index definition on each pod:"
for POD in ${PODS}; do
  echo "  ${POD}:"
  run_sql "${POD}" \
    "SELECT name, type, expr, granularity FROM system.data_skipping_indices WHERE database='logs' AND table='logs'" \
    | sed 's/^/    /'
done

echo ""
echo "Done. Restarting bifract deployment so containers reinitialize against the new schema..."
kubectl rollout restart deployment -n "${NAMESPACE}" bifract
kubectl rollout status deployment -n "${NAMESPACE}" bifract
echo "Rollout complete."
