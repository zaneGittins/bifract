#!/usr/bin/env bash
# Drops the logs database on every ClickHouse node and recreates it using the
# local db/init-clickhouse.sql (which includes GRANULARITY 1 on the inverted
# index). Clears all log data, stuck merges, mutations, and the replication
# queue. Fractal configs (Postgres) are not affected.
#
# Run from the repo root.
#
# Modes:
#   --k8s            Target a Kubernetes cluster (default if kubectl finds pods).
#   --docker         Target the local docker compose stack.
#   (auto)           If neither flag is given, auto-detect: prefer k8s pods,
#                    fall back to the docker container.
#
# Options:
#   --init <path>    Use an ad-hoc init SQL file instead of db/init-clickhouse.sql.
#   -y, --yes        Skip the confirmation prompt.
#
# Requirements: kubectl (k8s mode) or docker (docker mode) on PATH.

set -euo pipefail

# ── config ──────────────────────────────────────────────────────────────────────
NAMESPACE="bifract"
SECRET_NAME="bifract-secrets"
CH_USER="default"
INIT_SQL="db/init-clickhouse.sql"

# docker-specific defaults
DOCKER_CH_CONTAINER="bifract-clickhouse"
DOCKER_APP_CONTAINER="bifract-app"

MODE=""        # "k8s" | "docker" | "" (auto)
ASSUME_YES=0

# ── arg parsing ─────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --k8s)     MODE="k8s"; shift ;;
    --docker)  MODE="docker"; shift ;;
    --init)    INIT_SQL="${2:?--init requires a path}"; shift 2 ;;
    -y|--yes)  ASSUME_YES=1; shift ;;
    -h|--help)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      exit 1 ;;
  esac
done

# ── pre-flight ──────────────────────────────────────────────────────────────────
if [[ ! -f "${INIT_SQL}" ]]; then
  echo "ERROR: init SQL '${INIT_SQL}' not found. Run from the repo root or pass --init <path>." >&2
  exit 1
fi

# ── auto-detect mode ────────────────────────────────────────────────────────────
detect_k8s_pods() {
  command -v kubectl >/dev/null 2>&1 || return 1
  local pods
  pods=$(kubectl get pods -n "${NAMESPACE}" \
    -l "app.kubernetes.io/name=clickhouse-server" \
    --no-headers -o custom-columns=":metadata.name" 2>/dev/null || true)
  [[ -z "${pods}" ]] && pods=$(kubectl get pods -n "${NAMESPACE}" --no-headers \
    -o custom-columns=":metadata.name" 2>/dev/null | grep "^bifract-ch" || true)
  [[ -n "${pods}" ]] && echo "${pods}"
}

detect_docker() {
  command -v docker >/dev/null 2>&1 || return 1
  docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${DOCKER_CH_CONTAINER}"
}

if [[ -z "${MODE}" ]]; then
  if PODS_DETECTED=$(detect_k8s_pods); [[ -n "${PODS_DETECTED:-}" ]]; then
    MODE="k8s"
  elif detect_docker; then
    MODE="docker"
  else
    echo "ERROR: could not auto-detect a target. No k8s ClickHouse pods and no" >&2
    echo "       '${DOCKER_CH_CONTAINER}' docker container found." >&2
    echo "       Pass --k8s or --docker explicitly." >&2
    exit 1
  fi
fi

# ── confirmation ────────────────────────────────────────────────────────────────
echo "WARNING: This will DROP the logs database on the ${MODE} ClickHouse target(s)."
echo "         All ingested log data will be permanently deleted."
echo "         Fractal configs, secrets, and other resources are NOT affected."
echo "         Init SQL: ${INIT_SQL}"
echo ""
if [[ "${ASSUME_YES}" -ne 1 ]]; then
  read -r -p "Type 'yes' to continue: " confirm
  if [[ "${confirm}" != "yes" ]]; then
    echo "Aborted."
    exit 0
  fi
  echo ""
fi

# ──────────────────────────────────────────────────────────────────────────────────
# Backend-specific implementations. Each mode defines:
#   TARGETS            newline-separated list of node identifiers
#   run_sql <t> <sql>  run a query on target t
#   pipe_sql <t>       pipe stdin (SQL file) into clickhouse-client on target t
#   touch_force_drop   create the force_drop_table flag on target t
#   restart_app        restart the bifract app so it reinitializes
# ──────────────────────────────────────────────────────────────────────────────────

if [[ "${MODE}" == "k8s" ]]; then
  echo "Fetching ClickHouse password from secret ${SECRET_NAME}..."
  CH_PASS=$(kubectl get secret -n "${NAMESPACE}" "${SECRET_NAME}" \
    -o jsonpath='{.data.CLICKHOUSE_PASSWORD}' | base64 -d)
  if [[ -z "${CH_PASS}" ]]; then
    echo "ERROR: Could not retrieve CLICKHOUSE_PASSWORD from secret ${SECRET_NAME}" >&2
    exit 1
  fi

  TARGETS="${PODS_DETECTED:-$(detect_k8s_pods)}"
  if [[ -z "${TARGETS}" ]]; then
    echo "ERROR: No ClickHouse pods found in namespace ${NAMESPACE}" >&2
    exit 1
  fi

  run_sql() {
    kubectl exec -n "${NAMESPACE}" "$1" -- \
      clickhouse-client --user="${CH_USER}" --password="${CH_PASS}" --query="$2"
  }
  pipe_sql() {
    kubectl exec -i -n "${NAMESPACE}" "$1" -- \
      clickhouse-client --user="${CH_USER}" --password="${CH_PASS}"
  }
  touch_force_drop() {
    kubectl exec -n "${NAMESPACE}" "$1" -- \
      sh -c 'touch /var/lib/clickhouse/flags/force_drop_table && chmod 666 /var/lib/clickhouse/flags/force_drop_table'
  }
  restart_app() {
    echo "Restarting bifract deployment so containers reinitialize against the new schema..."
    kubectl rollout restart deployment -n "${NAMESPACE}" bifract
    kubectl rollout status deployment -n "${NAMESPACE}" bifract
  }

else  # docker
  # Resolve password: prefer .env, fall back to compose default "bifract".
  CH_PASS="bifract"
  if [[ -f .env ]]; then
    env_pass=$(grep -E '^CLICKHOUSE_PASSWORD=' .env | tail -n1 | cut -d= -f2- | tr -d '"'"'"'' || true)
    [[ -n "${env_pass}" ]] && CH_PASS="${env_pass}"
  fi
  [[ -n "${CLICKHOUSE_PASSWORD:-}" ]] && CH_PASS="${CLICKHOUSE_PASSWORD}"

  TARGETS="${DOCKER_CH_CONTAINER}"

  run_sql() {
    docker exec "$1" \
      clickhouse-client --user="${CH_USER}" --password="${CH_PASS}" --query="$2"
  }
  pipe_sql() {
    docker exec -i "$1" \
      clickhouse-client --user="${CH_USER}" --password="${CH_PASS}"
  }
  touch_force_drop() {
    docker exec "$1" \
      sh -c 'touch /var/lib/clickhouse/flags/force_drop_table && chmod 666 /var/lib/clickhouse/flags/force_drop_table'
  }
  restart_app() {
    echo "Restarting ${DOCKER_APP_CONTAINER} so it reinitializes against the new schema..."
    # Restart by container name so we do not depend on which compose files
    # (or working directory) are present on this host.
    docker restart "${DOCKER_APP_CONTAINER}"
  }
fi

TARGET_COUNT=$(echo "${TARGETS}" | wc -l | tr -d ' ')
echo "Mode: ${MODE}. Target(s) (${TARGET_COUNT}): $(echo "${TARGETS}" | tr '\n' ' ')"
echo ""

# ── drop + recreate on each target ──────────────────────────────────────────────
for T in ${TARGETS}; do
  echo "=== ${T} ==="

  echo "  Dropping logs database (clears all data, mutations, merges, replication queue)..."
  # ClickHouse refuses to drop tables >50 GB without a force flag file.
  touch_force_drop "${T}"
  run_sql "${T}" "DROP DATABASE IF EXISTS logs"
  echo "  Dropped."

  echo "  Recreating schema from ${INIT_SQL}..."
  pipe_sql "${T}" < "${INIT_SQL}"
  echo "  Schema created."
  echo ""
done

# ── verify ──────────────────────────────────────────────────────────────────────
echo "Verifying index definition on each target:"
for T in ${TARGETS}; do
  echo "  ${T}:"
  run_sql "${T}" \
    "SELECT name, type, expr, granularity FROM system.data_skipping_indices WHERE database='logs' AND table='logs'" \
    | sed 's/^/    /'
done

echo ""
restart_app
echo "Done."
