#!/usr/bin/env bash
# collect-logs.sh - collect a Bifract support bundle from a docker compose or
# Kubernetes install. No arguments required: the platform is auto-detected.
#
# Usage:
#   ./collect-logs.sh                  Write bifract-support-<host>-<ts>.zip to the current directory.
#   ./collect-logs.sh --dir mybundle   Write everything to ./mybundle instead of an archive.
#
# Options:
#   --dir <path>       Output to a new directory instead of an archive.
#   --docker | --k8s   Force the platform instead of auto-detecting.
#   --namespace <ns>   Kubernetes namespace (default: bifract).
#   --tail <n>         Log lines collected per container (default: 3000).
#   --quick            Skip the second merge sample (no stuck-merge progress delta).
#   -h, --help         Show this help.
#
# The bundle is size-capped (per file, per log, and overall) so it stays small
# enough to send and cannot fill the host disk. Values that look like passwords,
# tokens, or keys are redacted; Kubernetes secret values are never read.

set -uo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NAMESPACE="bifract"
CH_USER="default"
PG_USER="bifract"
PG_DB="bifract"

TAIL_LINES=3000
MAX_FILE_BYTES=$((4 * 1024 * 1024))   # per captured command output
MAX_LOG_BYTES=$((6 * 1024 * 1024))    # per container log
MAX_TARGETS=60                        # container/pod log targets
MAX_ERR_LINES=2000                    # error-only excerpt per log
CMD_TIMEOUT=90                        # seconds per command
MERGE_SAMPLE_SECS=8                   # gap between the two merge samples
MIN_FREE_MB=1024                      # refuse to run below this much free space
BUNDLE_WARN_BYTES=$((256 * 1024 * 1024))

PLATFORM=""
OUT_DIR=""
QUICK=0

# Tables whose schema is dumped and cross-checked against their _distributed twin.
BASE_TABLES="logs logs_hot logs_raw logs_histogram proc_lineage proc_freq process_edges"

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)       OUT_DIR="${2:?--dir requires a path}"; shift 2 ;;
    --docker)    PLATFORM="docker"; shift ;;
    --k8s|--kubernetes) PLATFORM="k8s"; shift ;;
    --namespace) NAMESPACE="${2:?--namespace requires a value}"; shift 2 ;;
    --tail)      TAIL_LINES="${2:?--tail requires a value}"; shift 2 ;;
    --quick)     QUICK=1; shift ;;
    -h|--help)   awk 'NR>1 { if ($0 !~ /^#/) exit; sub(/^# ?/, ""); print }' "$0"; exit 0 ;;
    *) echo "ERROR: unknown argument: $1 (try --help)" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------
have() { command -v "$1" >/dev/null 2>&1; }
say()  { printf '%s\n' "$*"; }
step() { printf '  %s\n' "$*"; }

TIMEOUT_BIN=""
if have timeout; then TIMEOUT_BIN="timeout"
elif have gtimeout; then TIMEOUT_BIN="gtimeout"
fi

# run_to <cmd...> - run with a timeout when one is available.
run_to() {
  if [[ -n "$TIMEOUT_BIN" ]]; then "$TIMEOUT_BIN" "$CMD_TIMEOUT" "$@"
  else "$@"
  fi
}

fsize() { wc -c <"$1" 2>/dev/null | tr -d ' ' || echo 0; }

# Redaction: perl when present (best regex support), GNU sed next, then a
# fixed-case fallback that still covers env-var and JSON/YAML spellings.
REDACT_MODE="none"
if have perl; then REDACT_MODE="perl"
elif sed --version 2>/dev/null | grep -q GNU; then REDACT_MODE="gsed"
else REDACT_MODE="basic"
fi

# A value is redacted when the identifier immediately left of ':' or '=' contains
# a sensitive word as a whole underscore/dot/dash separated segment. Segment
# matching is what keeps "tokenizer = ngrams(3)" in an index definition intact
# while still catching BIFRACT_ARCHIVE_S3_SECRET_KEY and PASSWORD_PEPPER.
REDACT_SEG='password|passwd|pwd|secret|token|credentials?|authorization|pepper|apikey|passphrase'
REDACT_CKEY='(access|secret|private|api|master|license|encryption|signing|client|ingest|auth|session)[_-]key'
REDACT_ID="([A-Za-z0-9]+[_.-])*($REDACT_SEG|$REDACT_CKEY)([_.-][A-Za-z0-9]+)*"

redact_file() {
  [[ -f "$1" ]] || return 0
  case "$REDACT_MODE" in
    perl)
      perl -i -pe "
        s/(Bearer\s+)[A-Za-z0-9._\-]{8,}/\${1}***REDACTED***/gi;
        s/($REDACT_ID[\"']?\s*[:=]\s*[\"']?)([^\s\"',;}\]]+)/\${1}***REDACTED***/gi;
        s{(://)[^/\s:\@]+:[^/\s\@]+\@}{\${1}***REDACTED***\@}g;
      " "$1" 2>/dev/null || true
      ;;
    gsed)
      sed -E -i \
        -e "s/(Bearer[[:space:]]+)[A-Za-z0-9._-]{8,}/\1***REDACTED***/gI" \
        -e "s/($REDACT_ID[\"']?[[:space:]]*[:=][[:space:]]*[\"']?)[^[:space:]\"',;}]+/\1***REDACTED***/gI" \
        "$1" 2>/dev/null || true
      ;;
    basic)
      sed -e 's/\([A-Z0-9_]*\(PASSWORD\|SECRET\|TOKEN\|CREDENTIAL\|PEPPER\|APIKEY\|API_KEY\|ACCESS_KEY\|MASTER_KEY\|LICENSE_KEY\|ENCRYPTION_KEY\|PRIVATE_KEY\)[A-Z0-9_]*[[:space:]]*[:=][[:space:]]*"\?\)[^[:space:]",;}]*/\1***REDACTED***/g' \
          -e 's/\("\?[a-z0-9_.-]*\(password\|secret\|token\|credential\|pepper\|apikey\|api_key\|access_key\|master_key\|license_key\|encryption_key\|private_key\)[a-z0-9_.-]*"\?[[:space:]]*[:=][[:space:]]*"\?\)[^[:space:]",;}]*/\1***REDACTED***/g' \
          "$1" >"$1.red" 2>/dev/null && mv "$1.red" "$1"
      rm -f "$1.red"
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Output layout
# ---------------------------------------------------------------------------
HOSTNAME_S="$(hostname 2>/dev/null || echo unknown)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BUNDLE_NAME="bifract-support-${HOSTNAME_S%%.*}-${STAMP}"

STAGING=""
if [[ -n "$OUT_DIR" ]]; then
  if [[ -e "$OUT_DIR" ]]; then
    if [[ ! -d "$OUT_DIR" ]] || [[ -n "$(ls -A "$OUT_DIR" 2>/dev/null)" ]]; then
      echo "ERROR: --dir '$OUT_DIR' already exists and is not an empty directory." >&2
      exit 1
    fi
  fi
  mkdir -p "$OUT_DIR" || { echo "ERROR: cannot create '$OUT_DIR'." >&2; exit 1; }
  BUNDLE="$(cd "$OUT_DIR" && pwd)"
  PARENT="$(dirname "$BUNDLE")"
else
  PARENT="$(pwd)"
  STAGING="$(mktemp -d "${TMPDIR:-/tmp}/bifract-collect.XXXXXX")" || exit 1
  BUNDLE="$STAGING/$BUNDLE_NAME"
  mkdir -p "$BUNDLE"
fi
chmod 700 "$BUNDLE" 2>/dev/null || true

TMPD="$(mktemp -d "${TMPDIR:-/tmp}/bifract-collect-tmp.XXXXXX")" || exit 1
cleanup() { [[ -n "$STAGING" ]] && rm -rf "$STAGING"; rm -rf "$TMPD"; }
trap cleanup EXIT INT TERM

# Free-space guard on whatever filesystem we are writing to.
free_mb="$(df -Pk "$PARENT" 2>/dev/null | awk 'NR==2 {print int($4/1024)}')"
if [[ -n "$free_mb" ]] && [[ "$free_mb" -lt "$MIN_FREE_MB" ]]; then
  echo "ERROR: only ${free_mb}MB free at $PARENT; need at least ${MIN_FREE_MB}MB." >&2
  exit 1
fi

HOST_DIR="$BUNDLE/host"
LOG_DIR="$BUNDLE/logs"
CH_DIR="$BUNDLE/clickhouse"
PG_DIR="$BUNDLE/postgres"
PLAT_DIR=""
ERRLOG="$BUNDLE/collection-errors.txt"
FACTS="$TMPD/facts"
mkdir -p "$HOST_DIR" "$LOG_DIR" "$CH_DIR" "$PG_DIR"
: >"$ERRLOG"
: >"$FACTS"

note() { printf '%s\n' "$*" >>"$ERRLOG"; }
fact() { printf '%s\t%s\n' "$1" "$2" >>"$FACTS"; }

# capture <outfile> <cmd...> - run a command, cap and redact its output.
capture() {
  local out="$1"; shift
  mkdir -p "$(dirname "$out")"
  local rc=0
  run_to "$@" >"$TMPD/cap.out" 2>"$TMPD/cap.err" || rc=$?
  {
    printf '# command: %s\n# exit: %s\n\n' "$*" "$rc"
  } >"$out"
  head -c "$MAX_FILE_BYTES" "$TMPD/cap.out" >>"$out" 2>/dev/null
  if [[ "$(fsize "$TMPD/cap.out")" -gt "$MAX_FILE_BYTES" ]]; then
    printf '\n[TRUNCATED at %s bytes]\n' "$MAX_FILE_BYTES" >>"$out"
  fi
  if [[ -s "$TMPD/cap.err" ]]; then
    printf '\n--- stderr ---\n' >>"$out"
    head -c 32768 "$TMPD/cap.err" >>"$out"
  fi
  [[ "$rc" -ne 0 ]] && note "exit ${rc}: $*"
  redact_file "$out"
  return 0
}

# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------
k8s_pods() {
  have kubectl || return 1
  run_to kubectl get pods -n "$NAMESPACE" --no-headers -o custom-columns=":metadata.name" 2>/dev/null
}

docker_containers_all() {
  have docker || return 1
  run_to docker ps -a --format '{{.Names}}' 2>/dev/null
}

if [[ "$PLATFORM" == "k8s" ]] && ! have kubectl; then
  echo "ERROR: --k8s was requested but kubectl is not on PATH." >&2
  exit 1
fi
if [[ "$PLATFORM" == "docker" ]] && ! have docker; then
  echo "ERROR: --docker was requested but docker is not on PATH." >&2
  exit 1
fi
if [[ "$PLATFORM" == "k8s" ]] && [[ -z "$(k8s_pods | head -1)" ]]; then
  echo "WARNING: no pods found in namespace '$NAMESPACE'. Collecting what is reachable." >&2
fi

if [[ -z "$PLATFORM" ]]; then
  if [[ -n "$(k8s_pods | head -1)" ]]; then
    PLATFORM="k8s"
  elif docker_containers_all 2>/dev/null | grep -q '^bifract'; then
    PLATFORM="docker"
  elif [[ -n "$(docker_containers_all | head -1)" ]]; then
    PLATFORM="docker"
    note "No container named bifract* found; collecting all docker containers."
  else
    echo "ERROR: could not detect a Bifract install." >&2
    echo "       No pods in namespace '$NAMESPACE' and no docker containers found." >&2
    echo "       Pass --docker or --k8s (and --namespace) explicitly." >&2
    exit 1
  fi
fi

say "Bifract support bundle"
say "  platform: $PLATFORM"
say "  output:   ${OUT_DIR:-$BUNDLE_NAME.zip}"
say ""

PLAT_DIR="$BUNDLE/$( [[ "$PLATFORM" == "k8s" ]] && echo kubernetes || echo docker )"
mkdir -p "$PLAT_DIR"
fact platform "$PLATFORM"
fact hostname "$HOSTNAME_S"
fact collected_at "$(date -u '+%Y-%m-%d %H:%M:%S UTC')"

# ---------------------------------------------------------------------------
# Host
# ---------------------------------------------------------------------------
say "Collecting host info..."
capture "$HOST_DIR/hostname.txt" hostname
capture "$HOST_DIR/uname.txt" uname -a
[[ -r /etc/os-release ]] && capture "$HOST_DIR/os-release.txt" cat /etc/os-release
capture "$HOST_DIR/uptime.txt" uptime
have nproc   && capture "$HOST_DIR/cpu-count.txt" nproc
[[ -r /proc/cpuinfo ]] && capture "$HOST_DIR/cpuinfo.txt" sh -c "grep -E 'model name|processor|MHz' /proc/cpuinfo | head -60"
have free    && capture "$HOST_DIR/memory.txt" free -h
[[ -r /proc/meminfo ]] && capture "$HOST_DIR/meminfo.txt" head -30 /proc/meminfo
capture "$HOST_DIR/disk.txt" df -h
capture "$HOST_DIR/disk-inodes.txt" df -i
have lsblk   && capture "$HOST_DIR/lsblk.txt" lsblk
have top     && capture "$HOST_DIR/top.txt" sh -c "top -b -n1 2>/dev/null | head -40"
have vmstat  && capture "$HOST_DIR/vmstat.txt" vmstat 1 3
have iostat  && capture "$HOST_DIR/iostat.txt" iostat -x 1 2
[[ -r /proc/loadavg ]] && capture "$HOST_DIR/loadavg.txt" cat /proc/loadavg
have dmesg   && capture "$HOST_DIR/dmesg-oom.txt" sh -c "dmesg -T 2>/dev/null | grep -iE 'oom|killed process|out of memory' | tail -100"
have ss      && capture "$HOST_DIR/listening-ports.txt" ss -lntp
have ulimit  && capture "$HOST_DIR/limits.txt" sh -c "ulimit -a"

host_free_pct="$(df -Pk / 2>/dev/null | awk 'NR==2 {printf "%d", 100-($3/$2*100)}')"
[[ -n "$host_free_pct" ]] && fact host_root_free_pct "${host_free_pct}%"

# ---------------------------------------------------------------------------
# Platform inventory
# ---------------------------------------------------------------------------
LOG_TARGETS=""   # lines of: <id>|<container-or-empty>|<label>
CH_NODES=""
PG_NODE=""

if [[ "$PLATFORM" == "docker" ]]; then
  say "Collecting docker inventory..."
  capture "$PLAT_DIR/version.txt" docker version
  capture "$PLAT_DIR/info.txt" docker info
  capture "$PLAT_DIR/ps.txt" docker ps -a
  capture "$PLAT_DIR/images.txt" docker images
  capture "$PLAT_DIR/system-df.txt" docker system df -v
  capture "$PLAT_DIR/stats.txt" docker stats --no-stream
  capture "$PLAT_DIR/networks.txt" docker network ls
  capture "$PLAT_DIR/volumes.txt" docker volume ls

  # Containers of the bifract compose project, plus any bifract-* by name.
  proj="$(run_to docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' bifract-app 2>/dev/null | head -1)"
  if [[ -z "$proj" || "$proj" == "<no value>" ]]; then
    seed="$(docker_containers_all | grep '^bifract' | head -1)"
    [[ -n "$seed" ]] && proj="$(run_to docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$seed" 2>/dev/null | head -1)"
  fi
  containers=""
  if [[ -n "$proj" && "$proj" != "<no value>" ]]; then
    containers="$(run_to docker ps -a --filter "label=com.docker.compose.project=$proj" --format '{{.Names}}' 2>/dev/null)"
    fact compose_project "$proj"
    # compose config only resolves from the project directory, which is not
    # necessarily where this script was started.
    wd="$(run_to docker inspect --format '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}' \
      "$(printf '%s\n' "$containers" | head -1)" 2>/dev/null)"
    if [[ -n "$wd" && "$wd" != "<no value>" && -d "$wd" ]]; then
      fact compose_working_dir "$wd"
      capture "$PLAT_DIR/compose-config.txt" sh -c "cd '$wd' && docker compose config"
    fi
  fi
  containers="$(printf '%s\n%s\n' "$containers" "$(docker_containers_all | grep '^bifract' || true)" | grep -v '^$' | sort -u)"
  [[ -z "$containers" ]] && containers="$(docker_containers_all | head -"$MAX_TARGETS")"

  n=0
  for c in $containers; do
    n=$((n + 1))
    [[ "$n" -gt "$MAX_TARGETS" ]] && { note "Container list capped at $MAX_TARGETS."; break; }
    capture "$PLAT_DIR/inspect/${c}.json" docker inspect "$c"
    LOG_TARGETS="${LOG_TARGETS}${c}||${c}"$'\n'
  done
  fact containers "$(printf '%s\n' "$containers" | grep -cv '^$')"
  printf '%s\n' "$containers" >"$TMPD/ours"
  not_running="$(run_to docker ps -a --format '{{.Names}} {{.State}}' 2>/dev/null \
    | awk 'NR==FNR {ours[$1]=1; next} ($1 in ours) && $2!="running" {print $1"("$2")"}' "$TMPD/ours" - | tr '\n' ' ')"
  fact containers_not_running "${not_running:-none}"

  # ClickHouse and Postgres nodes.
  CH_NODES="$(printf '%s\n' "$containers" | grep -i 'clickhouse' || true)"
  PG_NODE="$(printf '%s\n' "$containers" | grep -i 'postgres' | head -1 || true)"

  if [[ -n "$CH_NODES" ]]; then
    ch_first="$(printf '%s\n' "$CH_NODES" | head -1)"
    CH_PASS="$(run_to docker exec "$ch_first" sh -c 'printf "%s" "$CLICKHOUSE_PASSWORD"' 2>/dev/null)"
    if [[ -z "$CH_PASS" && -f .env ]]; then
      CH_PASS="$(grep -E '^CLICKHOUSE_PASSWORD=' .env | tail -1 | cut -d= -f2- | tr -d '"'"'"'')"
    fi
    [[ -z "$CH_PASS" ]] && CH_PASS="${CLICKHOUSE_PASSWORD:-bifract}"
  fi
  if [[ -n "$PG_NODE" ]]; then
    PG_PASS="$(run_to docker exec "$PG_NODE" sh -c 'printf "%s" "$POSTGRES_PASSWORD"' 2>/dev/null)"
    v="$(run_to docker exec "$PG_NODE" sh -c 'printf "%s" "$POSTGRES_USER"' 2>/dev/null)"; [[ -n "$v" ]] && PG_USER="$v"
    v="$(run_to docker exec "$PG_NODE" sh -c 'printf "%s" "$POSTGRES_DB"' 2>/dev/null)"; [[ -n "$v" ]] && PG_DB="$v"
  fi

else
  say "Collecting kubernetes inventory..."
  capture "$PLAT_DIR/kubectl-version.txt" kubectl version
  capture "$PLAT_DIR/nodes.txt" kubectl get nodes -o wide
  capture "$PLAT_DIR/nodes-describe.txt" kubectl describe nodes
  capture "$PLAT_DIR/top-nodes.txt" kubectl top nodes
  capture "$PLAT_DIR/top-pods.txt" kubectl top pods -n "$NAMESPACE"
  capture "$PLAT_DIR/pods.txt" kubectl get pods -n "$NAMESPACE" -o wide
  capture "$PLAT_DIR/all.txt" kubectl get all -n "$NAMESPACE" -o wide
  capture "$PLAT_DIR/events.txt" kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp
  capture "$PLAT_DIR/pvc.txt" kubectl get pvc -n "$NAMESPACE" -o wide
  capture "$PLAT_DIR/pv.txt" kubectl get pv
  capture "$PLAT_DIR/storageclasses.txt" kubectl get storageclass
  capture "$PLAT_DIR/networkpolicies.txt" kubectl get networkpolicy -n "$NAMESPACE" -o wide
  capture "$PLAT_DIR/configmaps.txt" kubectl get configmap -n "$NAMESPACE"
  capture "$PLAT_DIR/resourcequotas.txt" kubectl get resourcequota,limitrange -n "$NAMESPACE" -o wide
  # Secret names and key names only, never values.
  capture "$PLAT_DIR/secret-keys.txt" kubectl get secrets -n "$NAMESPACE" \
    -o go-template='{{range .items}}{{.metadata.name}}:{{range $k,$v := .data}} {{$k}}{{end}}{{"\n"}}{{end}}'

  capture "$PLAT_DIR/manifests/deployments.yaml" kubectl get deploy -n "$NAMESPACE" -o yaml
  capture "$PLAT_DIR/manifests/statefulsets.yaml" kubectl get sts -n "$NAMESPACE" -o yaml
  capture "$PLAT_DIR/manifests/services.yaml" kubectl get svc -n "$NAMESPACE" -o yaml
  capture "$PLAT_DIR/manifests/bifract-config.yaml" kubectl get configmap bifract-config -n "$NAMESPACE" -o yaml
  capture "$PLAT_DIR/endpoints.txt" kubectl get endpoints -n "$NAMESPACE"
  capture "$PLAT_DIR/manifests/clickhousecluster.yaml" kubectl get clickhousecluster -n "$NAMESPACE" -o yaml
  capture "$PLAT_DIR/manifests/keepercluster.yaml" kubectl get keepercluster -n "$NAMESPACE" -o yaml
  capture "$PLAT_DIR/describe/clickhousecluster.txt" kubectl describe clickhousecluster -n "$NAMESPACE"
  capture "$PLAT_DIR/describe/keepercluster.txt" kubectl describe keepercluster -n "$NAMESPACE"

  # Operator pods live outside the bifract namespace; find them, then pull logs
  # from whichever namespace they are actually in.
  capture "$PLAT_DIR/clickhouse-operator.txt" sh -c '
    ns_pods=$(kubectl get pods -A -o custom-columns=":metadata.namespace,:metadata.name" --no-headers 2>/dev/null \
      | grep -i "clickhouse-operator\|keeper-operator" | head -4)
    [ -z "$ns_pods" ] && { echo "no clickhouse operator pods found"; exit 0; }
    echo "$ns_pods"
    echo "$ns_pods" | while read -r ns pod; do
      echo ""
      echo "--- logs: $ns/$pod ---"
      kubectl logs -n "$ns" "$pod" --tail=400 --limit-bytes=1048576 2>&1 | tail -400
    done'

  pods="$(k8s_pods)"
  n=0
  for p in $pods; do
    n=$((n + 1))
    [[ "$n" -gt "$MAX_TARGETS" ]] && { note "Pod list capped at $MAX_TARGETS."; break; }
    capture "$PLAT_DIR/describe/${p}.txt" kubectl describe pod -n "$NAMESPACE" "$p"
    ctrs="$(run_to kubectl get pod -n "$NAMESPACE" "$p" \
      -o jsonpath='{range .spec.initContainers[*]}{.name}{"\n"}{end}{range .spec.containers[*]}{.name}{"\n"}{end}' 2>/dev/null)"
    for c in $ctrs; do
      LOG_TARGETS="${LOG_TARGETS}${p}|${c}|${p}.${c}"$'\n'
    done
  done
  fact pods "$(printf '%s\n' "$pods" | grep -cv '^$')"
  bad="$(run_to kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | awk '$3!="Running" && $3!="Completed" {print $1"("$3")"}' | tr '\n' ' ')"
  fact pods_not_running "${bad:-none}"
  restarts="$(run_to kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | awk '$4>0 {print $1"="$4}' | tr '\n' ' ')"
  fact pod_restarts "${restarts:-none}"

  CH_NODES="$(run_to kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=clickhouse-server" \
    --no-headers -o custom-columns=":metadata.name" 2>/dev/null)"
  [[ -z "$CH_NODES" ]] && CH_NODES="$(printf '%s\n' "$pods" | grep -E '^bifract-ch|clickhouse' | grep -v keeper || true)"
  PG_NODE="$(printf '%s\n' "$pods" | grep -E '^postgres' | head -1 || true)"
  [[ -z "$PG_NODE" ]] && PG_NODE="$(printf '%s\n' "$pods" | grep -i 'postgres' | head -1 || true)"

  # Resolve the container to exec into so kubectl does not have to guess.
  for p in $CH_NODES $PG_NODE; do
    [[ -z "$p" ]] && continue
    run_to kubectl get pod -n "$NAMESPACE" "$p" -o jsonpath='{.spec.containers[0].name}' \
      >"$TMPD/ctr.$p" 2>/dev/null
  done

  CH_PASS="$(run_to kubectl get secret -n "$NAMESPACE" bifract-secrets -o jsonpath='{.data.CLICKHOUSE_PASSWORD}' 2>/dev/null | base64 -d 2>/dev/null)"
  PG_PASS="$(run_to kubectl get secret -n "$NAMESPACE" bifract-secrets -o jsonpath='{.data.POSTGRES_PASSWORD}' 2>/dev/null | base64 -d 2>/dev/null)"
  if [[ -n "$PG_NODE" ]]; then
    [[ -z "$PG_PASS" ]] && PG_PASS="$(run_to kubectl exec -n "$NAMESPACE" "$PG_NODE" -- printenv POSTGRES_PASSWORD 2>/dev/null | tr -d '\r\n')"
    v="$(run_to kubectl exec -n "$NAMESPACE" "$PG_NODE" -- printenv POSTGRES_USER 2>/dev/null | tr -d '\r\n')"; [[ -n "$v" ]] && PG_USER="$v"
    v="$(run_to kubectl exec -n "$NAMESPACE" "$PG_NODE" -- printenv POSTGRES_DB 2>/dev/null | tr -d '\r\n')"; [[ -n "$v" ]] && PG_DB="$v"
  fi
fi

CH_PASS="${CH_PASS:-}"
PG_PASS="${PG_PASS:-}"
fact clickhouse_nodes "$(printf '%s\n' "$CH_NODES" | grep -cv '^$')"

# ---------------------------------------------------------------------------
# Bifract application identity
# ---------------------------------------------------------------------------
say "Collecting bifract version info..."
mkdir -p "$BUNDLE/bifract"
if [[ "$PLATFORM" == "docker" ]]; then
  capture "$BUNDLE/bifract/images.txt" sh -c \
    "docker ps -a --format '{{.Names}}\t{{.Image}}\t{{.Status}}' | sort"
  capture "$BUNDLE/bifract/image-digests.txt" sh -c \
    "for c in \$(docker ps -a --format '{{.Names}}'); do printf '%s\t' \"\$c\"; docker inspect --format '{{.Image}} {{index .Config.Image}}' \"\$c\" 2>/dev/null; done"
  app_image="$(run_to docker inspect --format '{{.Config.Image}}' bifract-app 2>/dev/null)"
else
  capture "$BUNDLE/bifract/images.txt" kubectl get pods -n "$NAMESPACE" \
    -o custom-columns='POD:.metadata.name,IMAGES:.spec.containers[*].image,NODE:.spec.nodeName,STATUS:.status.phase'
  app_image="$(run_to kubectl get pods -n "$NAMESPACE" -l app=bifract \
    -o jsonpath='{.items[0].spec.containers[0].image}' 2>/dev/null)"
fi
fact bifract_image "${app_image:-unknown}"
capture "$BUNDLE/bifract/env.txt" sh -c "env | grep -i bifract | sort"

# Reverse proxy config: TLS, routing, and the ingest listener live here.
if [[ "$PLATFORM" == "docker" ]]; then
  caddy_c="$(printf '%s\n' "$containers" | grep -i caddy | head -1)"
  [[ -n "$caddy_c" ]] && capture "$BUNDLE/bifract/caddyfile.txt" docker exec "$caddy_c" cat /etc/caddy/Caddyfile
else
  capture "$BUNDLE/bifract/caddyfile.txt" kubectl get configmap caddy-config -n "$NAMESPACE" -o yaml
fi

# ---------------------------------------------------------------------------
# Container / pod logs
# ---------------------------------------------------------------------------
say "Collecting container logs (tail ${TAIL_LINES})..."
mkdir -p "$LOG_DIR/errors"
err_total=0
printf '%s' "$LOG_TARGETS" | while IFS='|' read -r id ctr label; do
  [[ -z "$id" ]] && continue
  out="$LOG_DIR/${label}.log"
  if [[ "$PLATFORM" == "docker" ]]; then
    run_to docker logs --tail "$TAIL_LINES" --timestamps "$id" >"$TMPD/log.out" 2>&1
  else
    run_to kubectl logs -n "$NAMESPACE" "$id" -c "$ctr" --tail="$TAIL_LINES" \
      --limit-bytes="$MAX_LOG_BYTES" --timestamps >"$TMPD/log.out" 2>&1
  fi
  head -c "$MAX_LOG_BYTES" "$TMPD/log.out" >"$out"
  [[ "$(fsize "$TMPD/log.out")" -gt "$MAX_LOG_BYTES" ]] && \
    printf '\n[TRUNCATED at %s bytes]\n' "$MAX_LOG_BYTES" >>"$out"
  redact_file "$out"

  # Previous container instance: the only record of a crash loop.
  if [[ "$PLATFORM" == "k8s" ]]; then
    if run_to kubectl logs -n "$NAMESPACE" "$id" -c "$ctr" --previous --tail="$TAIL_LINES" \
         --limit-bytes="$MAX_LOG_BYTES" >"$TMPD/prev.out" 2>/dev/null && [[ -s "$TMPD/prev.out" ]]; then
      mkdir -p "$LOG_DIR/previous"
      head -c "$MAX_LOG_BYTES" "$TMPD/prev.out" >"$LOG_DIR/previous/${label}.log"
      redact_file "$LOG_DIR/previous/${label}.log"
    fi
  fi

  grep -inE '\b(error|fatal|panic|exception|traceback|failed|failure|denied|refused|timeout|timed out|too many|backpressure|oom)\b' \
    "$out" 2>/dev/null | tail -"$MAX_ERR_LINES" >"$LOG_DIR/errors/${label}.txt"
  if [[ ! -s "$LOG_DIR/errors/${label}.txt" ]]; then
    rm -f "$LOG_DIR/errors/${label}.txt"
  else
    printf '%s %s\n' "$label" "$(wc -l <"$LOG_DIR/errors/${label}.txt" | tr -d ' ')" >>"$TMPD/errcounts"
  fi
done

# Every bifract process logs "Bifract <version> (os/arch, go...)" as it starts,
# which pins the exact build even when the image tag is a moving target.
{
  printf 'Version banner, as logged by each process at startup.\n'
  printf 'Empty means the collected log tail does not reach back to the last\n'
  printf 'restart; fall back to bifract/images.txt for the image tag.\n\n'
  grep -hE 'Bifract [^ ]+ \((linux|darwin|windows)/' "$LOG_DIR"/*.log 2>/dev/null | sort -u | head -20
} >"$BUNDLE/bifract/version.txt"
fact bifract_version "$(grep -hoE 'Bifract [^ ]+ \(' "$LOG_DIR"/*.log 2>/dev/null | head -1 | awk '{print $2}')"

if [[ -s "$TMPD/errcounts" ]]; then
  sort -k2 -rn "$TMPD/errcounts" >"$LOG_DIR/errors/00-counts.txt"
  fact log_error_lines "$(awk '{s+=$2} END {print s+0}' "$TMPD/errcounts")"
  fact log_errors_top "$(head -3 "$LOG_DIR/errors/00-counts.txt" | awk '{printf "%s(%s) ", $1, $2}')"
else
  fact log_error_lines "0"
fi

# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------
# ch_query <node> <sql> [format]
ch_query() {
  local node="$1" sql="$2" fmt="${3:-}"
  local -a a
  # Capped execution time so a diagnostic query can never load a busy cluster.
  a=(--user "$CH_USER" --max_execution_time=45 --query "$sql")
  [[ -n "$fmt" ]] && a=("${a[@]}" --format "$fmt")
  if [[ "$PLATFORM" == "docker" ]]; then
    run_to docker exec -e "CLICKHOUSE_PASSWORD=${CH_PASS}" "$node" clickhouse-client "${a[@]}"
  else
    local -a k
    k=(exec -n "$NAMESPACE" "$node")
    [[ -s "$TMPD/ctr.$node" ]] && k=("${k[@]}" -c "$(cat "$TMPD/ctr.$node")")
    run_to kubectl "${k[@]}" -- env "CLICKHOUSE_PASSWORD=${CH_PASS}" clickhouse-client "${a[@]}"
  fi
}

# ch_capture <node> <relpath> <title> <sql> [format]
ch_capture() {
  local node="$1" rel="$2" title="$3" sql="$4" fmt="${5:-PrettyCompactMonoBlock}"
  local out="$CH_DIR/nodes/$node/$rel" rc=0
  mkdir -p "$(dirname "$out")"
  ch_query "$node" "$sql" "$fmt" >"$TMPD/ch.out" 2>"$TMPD/ch.err" || rc=$?
  {
    printf '== %s\n-- node: %s\n-- sql:  %s\n\n' "$title" "$node" "$(printf '%s' "$sql" | tr '\n' ' ' | tr -s ' ')"
  } >"$out"
  head -c "$MAX_FILE_BYTES" "$TMPD/ch.out" >>"$out"
  [[ "$(fsize "$TMPD/ch.out")" -gt "$MAX_FILE_BYTES" ]] && printf '\n[TRUNCATED]\n' >>"$out"
  if [[ "$rc" -ne 0 && -s "$TMPD/ch.err" ]]; then
    printf '\n--- error ---\n' >>"$out"
    head -c 8192 "$TMPD/ch.err" >>"$out"
    note "clickhouse ${node} ${rel}: $(head -1 "$TMPD/ch.err")"
  fi
  redact_file "$out"
}

# ch_value <node> <sql> - single scalar, empty on failure.
ch_value() { ch_query "$1" "$2" TSVRaw 2>/dev/null | head -1 | tr -d '\r'; }

if [[ -n "$CH_NODES" ]]; then
  say "Collecting ClickHouse diagnostics..."
  mkdir -p "$CH_DIR/consistency"
  all_tables=""
  for t in $BASE_TABLES; do
    all_tables="${all_tables}'${t}','${t}_distributed',"
  done
  all_tables="${all_tables%,}"

  for node in $CH_NODES; do
    step "$node"
    ch_capture "$node" server/version.txt "Server version and uptime" \
      "SELECT version() AS version, uptime() AS uptime_sec, now() AS server_time, timezone() AS tz, hostname() AS host" Vertical
    ch_capture "$node" server/clusters.txt "Cluster topology (system.clusters)" \
      "SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local, errors_count, slowdowns_count, estimated_recovery_time FROM system.clusters ORDER BY cluster, shard_num, replica_num"
    ch_capture "$node" server/macros.txt "Macros" "SELECT * FROM system.macros"
    ch_capture "$node" server/disks.txt "Disks" \
      "SELECT name, path, formatReadableSize(free_space) AS free, formatReadableSize(total_space) AS total, round(100 - free_space / total_space * 100, 1) AS used_pct, type, is_encrypted FROM system.disks"
    ch_capture "$node" server/storage-policies.txt "Storage policies (cold tiering)" \
      "SELECT policy_name, volume_name, volume_priority, disks, max_data_part_size, move_factor FROM system.storage_policies"
    ch_capture "$node" server/settings-changed.txt "Non-default session settings" \
      "SELECT name, value, default FROM system.settings WHERE changed ORDER BY name"
    ch_capture "$node" server/server-settings-changed.txt "Non-default server settings" \
      "SELECT name, value, default, description FROM system.server_settings WHERE changed ORDER BY name"
    ch_capture "$node" server/warnings.txt "Server warnings" "SELECT message FROM system.warnings"
    ch_capture "$node" server/metrics.txt "Current metrics (non-zero)" \
      "SELECT metric, value, description FROM system.metrics WHERE value != 0 ORDER BY metric"
    ch_capture "$node" server/asynchronous-metrics.txt "Asynchronous metrics" \
      "SELECT metric, value FROM system.asynchronous_metrics ORDER BY metric"
    ch_capture "$node" server/events.txt "Top profile events" \
      "SELECT event, value, description FROM system.events ORDER BY value DESC LIMIT 150"
    ch_capture "$node" server/dictionaries.txt "Dictionaries (model_lookup)" \
      "SELECT database, name, status, origin, element_count, formatReadableSize(bytes_allocated) AS bytes, last_successful_update_time, last_exception FROM system.dictionaries"
    if [[ "$PLATFORM" == "docker" ]]; then
      capture "$CH_DIR/nodes/$node/server/df.txt" docker exec "$node" df -h /var/lib/clickhouse
    else
      capture "$CH_DIR/nodes/$node/server/df.txt" kubectl exec -n "$NAMESPACE" "$node" -- df -h /var/lib/clickhouse
    fi

    # --- merges, mutations, moves, queues -----------------------------------
    ch_capture "$node" activity/merges.txt "Active merges" \
      "SELECT database, table, result_part_name, elapsed, round(progress, 4) AS progress, num_parts, formatReadableSize(total_size_bytes_compressed) AS total_size, formatReadableSize(bytes_read_uncompressed) AS read, formatReadableSize(memory_usage) AS mem, is_mutation, merge_type, merge_algorithm, partition_id FROM system.merges ORDER BY elapsed DESC"
    ch_capture "$node" activity/mutations-pending.txt "Unfinished mutations" \
      "SELECT database, table, mutation_id, command, create_time, parts_to_do, is_killed, latest_failed_part, latest_fail_time, latest_fail_reason FROM system.mutations WHERE is_done = 0 ORDER BY create_time"
    ch_capture "$node" activity/mutations-recent.txt "Recent mutations" \
      "SELECT database, table, mutation_id, substring(command, 1, 200) AS command, create_time, is_done, parts_to_do, latest_fail_reason FROM system.mutations ORDER BY create_time DESC LIMIT 100"
    ch_capture "$node" activity/moves.txt "Part moves between disks" "SELECT * FROM system.moves"
    ch_capture "$node" activity/distribution-queue.txt "Distributed insert queue" \
      "SELECT database, table, is_blocked, error_count, data_files, formatReadableSize(data_compressed_bytes) AS pending, broken_data_files, last_exception FROM system.distribution_queue"
    ch_capture "$node" activity/replication-queue.txt "Replication queue" \
      "SELECT database, table, type, create_time, num_tries, is_currently_executing, num_postponed, postpone_reason, last_exception FROM system.replication_queue ORDER BY create_time LIMIT 200"
    ch_capture "$node" activity/replicas.txt "Replica health" \
      "SELECT database, table, is_readonly, is_session_expired, future_parts, parts_to_check, queue_size, inserts_in_queue, merges_in_queue, absolute_delay, last_queue_update_exception, zookeeper_exception FROM system.replicas"
    # Keeper-backed tables only exist once a real cluster is configured.
    if [[ "$(ch_value "$node" "SELECT count() FROM system.clusters WHERE cluster != 'default'")" != "0" ]]; then
      ch_capture "$node" activity/distributed-ddl-queue.txt "Distributed DDL queue (pending schema changes)" \
        "SELECT entry, host, status, cluster, substring(query, 1, 300) AS query, initiator, query_create_time, query_finish_time, exception_code, exception_text FROM system.distributed_ddl_queue ORDER BY query_create_time DESC LIMIT 100"
      ch_capture "$node" activity/keeper-connection.txt "Keeper connection" \
        "SELECT * FROM system.zookeeper_connection"
    else
      mkdir -p "$CH_DIR/nodes/$node/activity"
      printf '== Distributed DDL queue\nNot applicable: only the built-in "default" cluster is configured, so this\nnode is a single-node install with no Keeper and no distributed DDL.\n' \
        >"$CH_DIR/nodes/$node/activity/distributed-ddl-queue.txt"
    fi
    ch_capture "$node" activity/processes.txt "Running queries" \
      "SELECT query_id, user, elapsed, formatReadableSize(memory_usage) AS mem, read_rows, formatReadableSize(read_bytes) AS read, substring(query, 1, 300) AS query FROM system.processes ORDER BY elapsed DESC"
    ch_capture "$node" activity/merge-history.txt "Merge history last 24h" \
      "SELECT table, event_type, count() AS n, round(avg(duration_ms)) AS avg_ms, max(duration_ms) AS max_ms, formatReadableSize(sum(size_in_bytes)) AS bytes, sum(error != 0) AS errors FROM system.part_log WHERE event_date >= today() - 1 AND database = 'logs' GROUP BY table, event_type ORDER BY n DESC"
    ch_capture "$node" activity/merge-failures.txt "Failed part operations last 3 days" \
      "SELECT event_time, table, event_type, part_name, error, substring(exception, 1, 300) AS exception FROM system.part_log WHERE event_date >= today() - 3 AND error != 0 ORDER BY event_time DESC LIMIT 200"

    # Headline counters for SUMMARY.txt.
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$node" \
      "$(ch_value "$node" 'SELECT count() FROM system.merges')" \
      "$(ch_value "$node" 'SELECT count() FROM system.mutations WHERE is_done = 0')" \
      "$(ch_value "$node" 'SELECT count() FROM system.detached_parts')" \
      "$(ch_value "$node" 'SELECT sum(data_files) FROM system.distribution_queue')" \
      "$(ch_value "$node" 'SELECT sum(error_count) FROM system.distribution_queue')" \
      "$(ch_value "$node" "SELECT max(c) FROM (SELECT count() AS c FROM system.parts WHERE active AND database = 'logs' GROUP BY table, partition)")" \
      "$(ch_value "$node" 'SELECT round(100 - min(free_space / total_space) * 100, 1) FROM system.disks')" \
      >>"$TMPD/chstats"

    # --- parts and storage --------------------------------------------------
    ch_capture "$node" parts/summary.txt "Parts by table" \
      "SELECT table, count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS on_disk, formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed, min(modification_time) AS oldest, max(modification_time) AS newest FROM system.parts WHERE active AND database = 'logs' GROUP BY table ORDER BY sum(bytes_on_disk) DESC"
    ch_capture "$node" parts/logs-partitions.txt "Largest logs partitions" \
      "SELECT partition, count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS on_disk, any(disk_name) AS disk FROM system.parts WHERE active AND database = 'logs' AND table = 'logs' GROUP BY partition ORDER BY sum(bytes_on_disk) DESC LIMIT 50"
    ch_capture "$node" parts/part-counts.txt "Part-count pressure (too many parts risk)" \
      "SELECT table, partition, count() AS parts FROM system.parts WHERE active AND database = 'logs' GROUP BY table, partition ORDER BY parts DESC LIMIT 30"
    ch_capture "$node" parts/detached.txt "Detached parts" \
      "SELECT database, table, partition_id, name, reason, disk FROM system.detached_parts LIMIT 200"

    # --- schema -------------------------------------------------------------
    ch_capture "$node" schema/tables.txt "Tables in logs database" \
      "SELECT name, engine, total_rows, formatReadableSize(total_bytes) AS size, create_table_query != '' AS has_ddl FROM system.tables WHERE database = 'logs' ORDER BY name"
    ch_capture "$node" schema/skip-indexes.txt "Data skipping indexes" \
      "SELECT table, name, type_full, expr, granularity, formatReadableSize(data_compressed_bytes) AS size FROM system.data_skipping_indices WHERE database = 'logs' ORDER BY table, name"
    ch_capture "$node" schema/columns.tsv "Columns of core tables" \
      "SELECT table, name, type FROM system.columns WHERE database = 'logs' AND table IN (${all_tables}) ORDER BY table, name" TSV
    for t in $BASE_TABLES; do
      ch_capture "$node" "schema/ddl/${t}.sql" "SHOW CREATE ${t}" \
        "SHOW CREATE TABLE logs.${t}" TSVRaw
      if [[ "$(ch_value "$node" "SELECT count() FROM system.tables WHERE database = 'logs' AND name = '${t}_distributed'")" == "1" ]]; then
        ch_capture "$node" "schema/ddl/${t}_distributed.sql" "SHOW CREATE ${t}_distributed" \
          "SHOW CREATE TABLE logs.${t}_distributed" TSVRaw
      fi
    done

    # --- migrations ---------------------------------------------------------
    ch_capture "$node" migrations/applied.txt "ClickHouse migrations applied" \
      "SELECT number, name, applied_at FROM logs._bifract_migrations FINAL ORDER BY number"
    ch_capture "$node" migrations/steps.txt "Per-statement migration progress" \
      "SELECT number, statement_index, name, applied_at FROM logs._bifract_migration_steps FINAL ORDER BY number DESC, statement_index DESC LIMIT 200"

    # --- errors and query log ----------------------------------------------
    ch_capture "$node" errors/error-counters.txt "Error counters" \
      "SELECT name, code, value, last_error_time, substring(last_error_message, 1, 300) AS last_message FROM system.errors WHERE value > 0 ORDER BY value DESC"
    ch_capture "$node" errors/failed-queries.txt "Recent failed queries" \
      "SELECT event_time, type, query_duration_ms, exception_code, substring(exception, 1, 300) AS exception, substring(query, 1, 300) AS query FROM system.query_log WHERE event_date >= today() - 3 AND exception_code != 0 ORDER BY event_time DESC LIMIT 150"
    ch_capture "$node" errors/slow-queries.txt "Slowest queries last 24h" \
      "SELECT event_time, query_duration_ms, formatReadableSize(memory_usage) AS mem, read_rows, formatReadableSize(read_bytes) AS read, substring(query, 1, 300) AS query FROM system.query_log WHERE event_date >= today() - 1 AND type = 'QueryFinish' ORDER BY query_duration_ms DESC LIMIT 50"
    ch_capture "$node" errors/query-volume.txt "Query volume last 24h" \
      "SELECT toStartOfHour(event_time) AS hour, type, count() AS n, round(avg(query_duration_ms)) AS avg_ms FROM system.query_log WHERE event_date >= today() - 1 GROUP BY hour, type ORDER BY hour DESC"
    ch_capture "$node" errors/text-log.txt "Server log Error and above" \
      "SELECT event_time, level, logger_name, substring(message, 1, 400) AS message FROM system.text_log WHERE event_date >= today() - 3 AND level <= 'Error' ORDER BY event_time DESC LIMIT 300"

    # --- ingest shape -------------------------------------------------------
    # Both read the logs_histogram rollup, never logs itself: cheap at any scale.
    ch_capture "$node" data/ingest-per-day.txt "Rows ingested per day (logs_histogram rollup)" \
      "SELECT toDate(minute) AS day, sum(cnt) AS rows FROM logs.logs_histogram GROUP BY day ORDER BY day DESC LIMIT 30"
    ch_capture "$node" data/fractals.txt "Rows per fractal (logs_histogram rollup)" \
      "SELECT fractal_id, sum(cnt) AS rows, min(minute) AS oldest, max(minute) AS newest FROM logs.logs_histogram GROUP BY fractal_id ORDER BY rows DESC LIMIT 50"
    ch_capture "$node" data/on-disk-per-day.txt "Bytes on disk per logs partition day" \
      "SELECT partition, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS on_disk, any(disk_name) AS disk FROM system.parts WHERE active AND database = 'logs' AND table = 'logs' GROUP BY partition ORDER BY partition DESC LIMIT 40"
  done

  # --- stuck vs moving merges ---------------------------------------------
  if [[ "$QUICK" -eq 0 ]]; then
    step "sampling merge progress (${MERGE_SAMPLE_SECS}s)"
    merge_sql="SELECT concat(database, '.', table, '|', result_part_name), elapsed, progress, bytes_read_uncompressed, rows_read FROM system.merges ORDER BY 1"
    for node in $CH_NODES; do
      ch_query "$node" "$merge_sql" TSV >"$TMPD/m1.$node" 2>/dev/null
    done
    sleep "$MERGE_SAMPLE_SECS"
    for node in $CH_NODES; do
      ch_query "$node" "$merge_sql" TSV >"$TMPD/m2.$node" 2>/dev/null
    done
    {
      printf '== Merge progress over a %ss window\n' "$MERGE_SAMPLE_SECS"
      printf 'MOVING means bytes_read or progress advanced between the two samples.\n'
      printf 'STUCK means an active merge made no measurable progress; correlate with\n'
      printf 'system.errors, part_log failures, and background pool metrics.\n\n'
      for node in $CH_NODES; do
        printf -- '--- %s ---\n' "$node"
        if [[ ! -s "$TMPD/m1.$node" && ! -s "$TMPD/m2.$node" ]]; then
          printf 'no merges running in either sample\n\n'
          continue
        fi
        awk -F'\t' '
          NR==FNR { p[$1] = $3; b[$1] = $4; r[$1] = $5; next }
          {
            if (!($1 in p)) { printf "NEW\t%s\telapsed=%.1fs\t(started during sample)\n", $1, $2; next }
            dp = $3 - p[$1]; db = $4 - b[$1]; dr = $5 - r[$1]
            printf "%s\t%s\telapsed=%.1fs\tdprogress=%.4f\tdbytes=%d\tdrows=%d\n",
              (db > 0 || dp > 0.0001 || dr > 0) ? "MOVING" : "STUCK", $1, $2, dp, db, dr
          }' "$TMPD/m1.$node" "$TMPD/m2.$node"
        awk -F'\t' 'NR==FNR { seen[$1]=1; next } { if (!($1 in seen)) printf "%s\t%s\t(finished during sample)\n", "DONE", $1 }' \
          "$TMPD/m2.$node" "$TMPD/m1.$node"
        printf '\n'
      done
    } >"$CH_DIR/consistency/merge-progress.txt"
    fact merges_stuck "$(awk -F'\t' '$1=="STUCK"' "$CH_DIR/consistency/merge-progress.txt" | wc -l | tr -d ' ')"
    fact merges_moving "$(awk -F'\t' '$1=="MOVING"' "$CH_DIR/consistency/merge-progress.txt" | wc -l | tr -d ' ')"
  else
    printf '== Merge progress\nNot sampled: --quick was used, which skips the second system.merges sample.\nSee activity/merges.txt per node for the single-point-in-time view.\n' \
      >"$CH_DIR/consistency/merge-progress.txt"
    fact merges_stuck "not sampled (--quick)"
    fact merges_moving "not sampled (--quick)"
  fi

  # --- schema consistency --------------------------------------------------
  step "checking schema consistency"
  {
    printf '== Schema consistency\n\n'
    printf 'Section 1: local table vs its Distributed twin, per node.\n'
    printf 'A mismatch here means an ALTER reached the local table but not the\n'
    printf 'Distributed table, which surfaces to users as ClickHouse code 16 or 47.\n\n'
    for node in $CH_NODES; do
      cols="$CH_DIR/nodes/$node/schema/columns.tsv"
      [[ -f "$cols" ]] || continue
      printf -- '--- %s ---\n' "$node"
      for t in $BASE_TABLES; do
        awk -F'\t' -v t="$t" '$1==t {print $2"\t"$3}' "$cols" | sort >"$TMPD/a"
        awk -F'\t' -v t="${t}_distributed" '$1==t {print $2"\t"$3}' "$cols" | sort >"$TMPD/b"
        if [[ ! -s "$TMPD/a" ]]; then
          printf '%-18s MISSING (local table not found)\n' "$t"
        elif [[ ! -s "$TMPD/b" ]]; then
          printf '%-18s n/a (no %s_distributed; single-node install)\n' "$t" "$t"
        else
          awk -F'\t' -v L="$t" -v R="${t}_distributed" '
            NR==FNR { a[$1] = $2; next }
            { b[$1] = $2 }
            END {
              for (k in a) if (!(k in b))                     printf "      only in %s: %s %s\n", L, k, a[k]
              for (k in b) if (!(k in a))                     printf "      only in %s: %s %s\n", R, k, b[k]
              for (k in a) if ((k in b) && a[k] != b[k])      printf "      type differs: %s\n        %s: %s\n        %s: %s\n", k, L, a[k], R, b[k]
            }' "$TMPD/a" "$TMPD/b" | sort >"$TMPD/d"
          if [[ -s "$TMPD/d" ]]; then
            printf '%-18s MISMATCH\n' "$t"
            cat "$TMPD/d"
            printf '\n'
          else
            printf '%-18s MATCH (%s columns)\n' "$t" "$(wc -l <"$TMPD/a" | tr -d ' ')"
          fi
        fi
      done
      printf '\n'
    done

    printf 'Section 2: the same table across nodes.\n'
    printf 'A mismatch means one shard did not receive a schema change.\n\n'
    for t in $BASE_TABLES; do
      mismatch=0; first=""
      for node in $CH_NODES; do
        cols="$CH_DIR/nodes/$node/schema/columns.tsv"
        [[ -f "$cols" ]] || continue
        s="$(awk -F'\t' -v t="$t" '$1==t {print $2"\t"$3}' "$cols" | sort | cksum | awk '{print $1}')"
        n="$(awk -F'\t' -v t="$t" '$1==t' "$cols" | wc -l | tr -d ' ')"
        [[ -z "$first" ]] && first="$s"
        [[ "$s" != "$first" ]] && mismatch=1
        printf '  %-40s %s cols  fingerprint=%s\n' "$node/$t" "$n" "$s"
      done
      if [[ "$mismatch" -eq 1 ]]; then
        printf '%-18s MISMATCH ACROSS NODES\n\n' "$t"
      else
        printf '%-18s consistent across nodes\n\n' "$t"
      fi
    done
  } >"$CH_DIR/consistency/schema-match.txt"
  redact_file "$CH_DIR/consistency/schema-match.txt"

  fact schema_mismatches "$(awk '$2 ~ /^MISMATCH/' "$CH_DIR/consistency/schema-match.txt" | wc -l | tr -d ' ')"

  # --- migration consistency ----------------------------------------------
  {
    printf '== Migration state\n\n'
    printf 'ClickHouse migrations are applied per node (max(number) in logs._bifract_migrations).\n'
    printf 'Every node must report the same number; a lagging node means a shard is\n'
    printf 'running an older schema.\n\n'
    maxnum=""; mismatch=0
    for node in $CH_NODES; do
      v="$(ch_value "$node" "SELECT max(number) FROM logs._bifract_migrations")"
      [[ -z "$v" ]] && v="unavailable"
      last="$(ch_value "$node" "SELECT name FROM logs._bifract_migrations FINAL ORDER BY number DESC LIMIT 1")"
      partial="$(ch_value "$node" "SELECT count() FROM logs._bifract_migration_steps WHERE number > (SELECT max(number) FROM logs._bifract_migrations)")"
      printf '  %-40s migration=%-8s last=%-28s partial_steps=%s\n' "$node" "$v" "${last:-?}" "${partial:-0}"
      [[ -z "$maxnum" ]] && maxnum="$v"
      [[ "$v" != "$maxnum" ]] && mismatch=1
    done
    printf '\n'
    if [[ "$mismatch" -eq 1 ]]; then
      printf 'VERDICT: MISMATCH, the migration number differs across ClickHouse nodes.\n'
    else
      printf 'VERDICT: all ClickHouse nodes report migration %s.\n' "${maxnum:-unknown}"
    fi
    printf '\nA non-zero partial_steps count means a migration started but did not finish;\n'
    printf 'see migrations/steps.txt on that node for the last statement applied.\n'
  } >"$CH_DIR/consistency/migrations.txt"
  fact ch_migration "$(ch_value "$(printf '%s\n' "$CH_NODES" | head -1)" 'SELECT max(number) FROM logs._bifract_migrations')"
  fact ch_migration_consistent "$(grep -q 'VERDICT: MISMATCH' "$CH_DIR/consistency/migrations.txt" && echo no || echo yes)"
else
  note "No ClickHouse node found; ClickHouse diagnostics skipped."
  fact clickhouse "not found"
fi

# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------
pg_query() {
  local sql="$1"
  if [[ "$PLATFORM" == "docker" ]]; then
    run_to docker exec -e "PGPASSWORD=${PG_PASS}" "$PG_NODE" \
      psql -U "$PG_USER" -d "$PG_DB" -X -A -F $'\t' --pset=footer=off -c "$sql"
  else
    local -a k
    k=(exec -n "$NAMESPACE" "$PG_NODE")
    [[ -s "$TMPD/ctr.$PG_NODE" ]] && k=("${k[@]}" -c "$(cat "$TMPD/ctr.$PG_NODE")")
    run_to kubectl "${k[@]}" -- \
      env "PGPASSWORD=${PG_PASS}" psql -U "$PG_USER" -d "$PG_DB" -X -A -F $'\t' --pset=footer=off -c "$sql"
  fi
}

pg_capture() {
  local rel="$1" title="$2" sql="$3" rc=0
  local out="$PG_DIR/$rel"
  mkdir -p "$(dirname "$out")"
  pg_query "$sql" >"$TMPD/pg.out" 2>"$TMPD/pg.err" || rc=$?
  printf '== %s\n-- sql: %s\n\n' "$title" "$(printf '%s' "$sql" | tr '\n' ' ' | tr -s ' ')" >"$out"
  head -c "$MAX_FILE_BYTES" "$TMPD/pg.out" >>"$out"
  if [[ "$rc" -ne 0 && -s "$TMPD/pg.err" ]]; then
    printf '\n--- error ---\n' >>"$out"
    head -c 8192 "$TMPD/pg.err" >>"$out"
    note "postgres ${rel}: $(head -1 "$TMPD/pg.err")"
  fi
  redact_file "$out"
}

if [[ -n "$PG_NODE" ]]; then
  say "Collecting Postgres diagnostics..."
  pg_capture version.txt "Version" "SELECT version()"
  pg_capture migrations.txt "Applied migrations" \
    "SELECT number, name, applied_at FROM _bifract_migrations ORDER BY number"
  pg_capture settings-bifract.txt "Bifract runtime settings" \
    "SELECT key, value, updated_at FROM settings ORDER BY key"
  pg_capture table-sizes.txt "Largest tables" \
    "SELECT relname, n_live_tup AS live_rows, n_dead_tup AS dead_rows, pg_size_pretty(pg_total_relation_size(relid)) AS total_size, last_autovacuum, last_autoanalyze FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 40"
  pg_capture object-counts.txt "Configuration object counts" \
    "SELECT 'fractals' AS object, count(*) FROM fractals
     UNION ALL SELECT 'prisms', count(*) FROM prisms
     UNION ALL SELECT 'users', count(*) FROM users
     UNION ALL SELECT 'groups', count(*) FROM groups
     UNION ALL SELECT 'api_keys', count(*) FROM api_keys
     UNION ALL SELECT 'ingest_tokens', count(*) FROM ingest_tokens
     UNION ALL SELECT 'alerts', count(*) FROM alerts
     UNION ALL SELECT 'dashboards', count(*) FROM dashboards
     UNION ALL SELECT 'notebooks', count(*) FROM notebooks
     UNION ALL SELECT 'saved_queries', count(*) FROM saved_queries
     UNION ALL SELECT 'normalizers', count(*) FROM normalizers
     UNION ALL SELECT 'analytics_models', count(*) FROM analytics_models
     UNION ALL SELECT 'comments', count(*) FROM comments
     ORDER BY 1"
  pg_capture fractals.txt "Fractals" \
    "SELECT id, name, created_at FROM fractals ORDER BY created_at"
  pg_capture alerts.txt "Alerts and evaluation cursor" \
    "SELECT id, name, alert_type, enabled, severity, last_evaluated_at, last_triggered, last_execution_time_ms, substring(disabled_reason, 1, 120) AS disabled_reason FROM alerts ORDER BY enabled DESC, last_evaluated_at LIMIT 100"
  pg_capture alert-executions.txt "Recent alert executions" \
    "SELECT triggered_at, alert_id, log_count, throttled, execution_time_ms FROM alert_executions ORDER BY triggered_at DESC LIMIT 100"
  pg_capture alert-action-errors.txt "Alert actions that reported an error" \
    "SELECT triggered_at, alert_id, substring(webhook_results::text, 1, 200) AS webhook, substring(email_results::text, 1, 200) AS email, substring(fractal_results::text, 1, 200) AS fractal FROM alert_executions WHERE webhook_results::text ILIKE '%error%' OR email_results::text ILIKE '%error%' OR fractal_results::text ILIKE '%error%' ORDER BY triggered_at DESC LIMIT 50"
  pg_capture archive-status.txt "Archive status" "SELECT * FROM archive_status"
  pg_capture archive-maintain-status.txt "Archive maintainer last run" \
    "SELECT last_run_at, last_attempt_at, last_outcome, duration_ms, tables_seen, compacted, groups_failed, expired, retention_files, orphans_deleted, substring(last_error, 1, 300) AS last_error FROM archive_maintain_status"
  pg_capture archive-maintain-history.txt "Archive maintenance history" \
    "SELECT ran_at, outcome, duration_ms, tables_seen, compacted, groups_failed, expired, orphans_deleted, substring(error, 1, 200) AS error FROM archive_maintain_history ORDER BY ran_at DESC LIMIT 50"
  pg_capture archive-jobs.txt "Recall and restore jobs" \
    "SELECT 'recall' AS kind, id::text AS id, status, row_count AS rows, substring(error, 1, 200) AS error, created_at FROM archive_search_jobs
     UNION ALL
     SELECT 'restore', id::text, status, rows_restored, substring(error, 1, 200), created_at FROM archive_restore_jobs
     ORDER BY created_at DESC LIMIT 50"
  pg_capture health-notifications.txt "Recent health notifications" \
    "SELECT created_at, notification_type, severity, title, substring(message, 1, 200) AS message FROM health_notifications ORDER BY created_at DESC LIMIT 50"
  pg_capture schema-fields.txt "ClickHouse schema field hints" \
    "SELECT field_name, index_type, sync_status, substring(sync_error, 1, 200) AS sync_error, created_by, created_at FROM clickhouse_schema_fields ORDER BY field_name LIMIT 300"
  pg_capture schema-overflow.txt "Schema field overflow and ignore lists" \
    "SELECT 'overflow' AS list, count(*) FROM schema_field_overflow
     UNION ALL SELECT 'ignored', count(*) FROM schema_field_ignored"
  pg_capture activity.txt "Current activity" \
    "SELECT pid, state, wait_event_type, wait_event, now() - query_start AS duration, substring(query, 1, 200) AS query FROM pg_stat_activity WHERE datname = current_database() ORDER BY query_start"
  pg_capture locks.txt "Blocked queries" \
    "SELECT blocked.pid AS blocked_pid, blocking.pid AS blocking_pid, substring(blocked.query, 1, 120) AS blocked_query, substring(blocking.query, 1, 120) AS blocking_query FROM pg_stat_activity blocked JOIN pg_stat_activity blocking ON blocking.pid = ANY(pg_blocking_pids(blocked.pid)) WHERE cardinality(pg_blocking_pids(blocked.pid)) > 0"
  pg_capture db-stats.txt "Database statistics" \
    "SELECT datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit, deadlocks, temp_files, pg_size_pretty(pg_database_size(datname)) AS size FROM pg_stat_database WHERE datname = current_database()"
  pg_capture settings-server.txt "Non-default server settings" \
    "SELECT name, setting, unit, source FROM pg_settings WHERE source NOT IN ('default', 'override') ORDER BY name"

  pg_mig="$(pg_query "SELECT COALESCE(max(number), 0) FROM _bifract_migrations" 2>/dev/null | tail -1)"
  if [[ -z "$pg_mig" ]]; then
    pg_mig="n/a (table absent)"
    printf '\nThe _bifract_migrations table only exists on installs deployed with\nbifract-setup. The application never applies Postgres migrations itself, so a\nmissing table here is normal for a plain docker compose stack.\n' \
      >>"$PG_DIR/migrations.txt"
  fi
  fact pg_migration "$pg_mig"
else
  note "No Postgres node found; Postgres diagnostics skipped."
  fact postgres "not found"
fi

# ---------------------------------------------------------------------------
# Summary, README, manifest
# ---------------------------------------------------------------------------
say "Writing summary..."

fv() { awk -F'\t' -v k="$1" '$1==k {print $2; exit}' "$FACTS"; }

{
  printf 'BIFRACT SUPPORT BUNDLE\n'
  printf '======================\n\n'
  printf '%-26s %s\n' "Collected (UTC):" "$(fv collected_at)"
  printf '%-26s %s\n' "Hostname:" "$(fv hostname)"
  printf '%-26s %s\n' "Platform:" "$(fv platform)"
  printf '%-26s %s\n' "Bifract version:" "$(fv bifract_version)"
  printf '%-26s %s\n' "Bifract image:" "$(fv bifract_image)"
  [[ -n "$(fv compose_project)" ]] && printf '%-26s %s\n' "Compose project:" "$(fv compose_project)"
  printf '\n'
  printf 'WORKLOADS\n'
  if [[ "$PLATFORM" == "docker" ]]; then
    printf '%-26s %s\n' "Containers:" "$(fv containers)"
    printf '%-26s %s\n' "Not running:" "$(fv containers_not_running)"
  else
    printf '%-26s %s\n' "Namespace:" "$NAMESPACE"
    printf '%-26s %s\n' "Pods:" "$(fv pods)"
    printf '%-26s %s\n' "Not running:" "$(fv pods_not_running)"
    printf '%-26s %s\n' "Restarts:" "$(fv pod_restarts)"
  fi
  printf '%-26s %s\n' "ClickHouse nodes:" "$(fv clickhouse_nodes)"
  printf '%-26s %s\n' "Root fs free:" "$(fv host_root_free_pct)"
  printf '\n'
  printf 'SCHEMA AND MIGRATIONS\n'
  printf '%-26s %s\n' "ClickHouse migration:" "$(fv ch_migration)"
  printf '%-26s %s\n' "Same on all nodes:" "$(fv ch_migration_consistent)"
  printf '%-26s %s\n' "Postgres migration:" "$(fv pg_migration)"
  printf '%-26s %s\n' "Schema mismatches:" "$(fv schema_mismatches)"
  printf '\n'
  printf 'ACTIVITY\n'
  printf '%-26s %s\n' "Stuck merges:" "$(fv merges_stuck)"
  printf '%-26s %s\n' "Error lines in logs:" "$(fv log_error_lines)"
  printf '%-26s %s\n' "Noisiest logs:" "$(fv log_errors_top)"
  printf '\n'
  if [[ -s "$TMPD/chstats" ]]; then
    printf 'CLICKHOUSE PER NODE\n'
    printf '  %-34s %7s %9s %8s %10s %10s %9s %9s\n' \
      node merges pend_mut detached dist_files dist_errs max_parts disk_pct
    awk -F'\t' '{ printf "  %-34s %7s %9s %8s %10s %10s %9s %9s\n", $1, $2, $3, $4, ($5==""?"0":$5), ($6==""?"0":$6), $7, $8 }' "$TMPD/chstats"
    printf '\n  max_parts is the largest active part count in a single partition;\n'
    printf '  a few hundred means merges are falling behind ingest.\n\n'
  fi
  printf 'START HERE\n'
  printf '  clickhouse/consistency/schema-match.txt   local vs distributed, and across nodes\n'
  printf '  clickhouse/consistency/migrations.txt     migration number per node\n'
  printf '  clickhouse/consistency/merge-progress.txt merges moving or stuck\n'
  printf '  logs/errors/00-counts.txt                 which component is noisiest\n'
  printf '  collection-errors.txt                     what this run could not collect\n'
} >"$BUNDLE/SUMMARY.txt"

{
  printf 'Bifract support bundle\n'
  printf '======================\n\n'
  printf 'Read SUMMARY.txt first.\n\n'
  printf 'Layout\n'
  printf '  SUMMARY.txt              triage summary generated from everything below\n'
  printf '  collection-errors.txt    commands that failed or were skipped\n'
  printf '  host/                    OS, CPU, memory, disk, OOM evidence\n'
  if [[ "$PLATFORM" == "docker" ]]; then
    printf '  docker/                  daemon info, containers, per-container inspect\n'
  else
    printf '  kubernetes/              nodes, pods, events, manifests, describes\n'
  fi
  printf '  bifract/                 image tags and version identity\n'
  printf '  logs/                    per container log tail (errors/ holds error lines only)\n'
  printf '  clickhouse/nodes/<node>/ per node diagnostics:\n'
  printf '      server/              version, cluster topology, disks, settings, metrics\n'
  printf '      activity/            merges, mutations, moves, distribution and DDL queues\n'
  printf '      parts/               part counts, partition sizes, detached parts\n'
  printf '      schema/              table DDL and column lists\n'
  printf '      migrations/          applied migrations and per-statement progress\n'
  printf '      errors/              error counters, failed and slow queries, server log\n'
  printf '      data/                ingest rate and per-fractal row counts\n'
  printf '  clickhouse/consistency/  cross-node schema, migration, and merge analysis\n'
  printf '  postgres/                version, migrations, settings, activity, object counts\n'
  printf '\n'
  printf 'Privacy\n'
  printf '  Values that look like passwords, tokens, or keys are replaced with\n'
  printf '  ***REDACTED*** (mode: %s). Kubernetes secret values are never read;\n' "$REDACT_MODE"
  printf '  only secret and key names are listed.\n'
  printf '  The bundle still contains operational data that may be sensitive:\n'
  printf '  container log lines, query text (truncated to 300 characters), table and\n'
  printf '  column names, fractal ids, and user counts. Review before sending if that\n'
  printf '  matters for your environment.\n'
  printf '\n'
  printf 'Size limits applied\n'
  printf '  %s bytes per captured command output\n' "$MAX_FILE_BYTES"
  printf '  %s bytes / %s lines per container log\n' "$MAX_LOG_BYTES" "$TAIL_LINES"
  printf '  %s log targets maximum\n' "$MAX_TARGETS"
} >"$BUNDLE/README.txt"

{
  printf 'collector: collect-logs.sh\n'
  printf 'bundle: %s\n' "$BUNDLE_NAME"
  printf 'generated: %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf 'platform: %s\n' "$PLATFORM"
  printf 'namespace: %s\n' "$NAMESPACE"
  printf 'redaction: %s\n' "$REDACT_MODE"
  printf 'shell: %s\n' "${BASH_VERSION:-unknown}"
  printf 'tools:\n'
  for t in docker kubectl zip tar perl awk diff; do
    if have "$t"; then printf '  %-8s %s\n' "$t" "$(command -v "$t")"
    else printf '  %-8s missing\n' "$t"; fi
  done
  printf '\nfacts:\n'
  sed 's/^/  /' "$FACTS"
} >"$BUNDLE/MANIFEST.txt"

if [[ ! -s "$ERRLOG" ]]; then
  printf 'No collection errors.\n' >"$ERRLOG"
fi
redact_file "$ERRLOG"

# ---------------------------------------------------------------------------
# Package
# ---------------------------------------------------------------------------
bundle_bytes="$(du -sk "$BUNDLE" 2>/dev/null | awk '{print $1*1024}')"
if [[ -n "$bundle_bytes" && "$bundle_bytes" -gt "$BUNDLE_WARN_BYTES" ]]; then
  note "Bundle is $((bundle_bytes / 1024 / 1024))MB, above the $((BUNDLE_WARN_BYTES / 1024 / 1024))MB soft limit."
fi

if [[ -n "$OUT_DIR" ]]; then
  say ""
  say "Bundle written to: $BUNDLE"
  say "Size: $(du -sh "$BUNDLE" 2>/dev/null | awk '{print $1}')"
  say "Read $BUNDLE/SUMMARY.txt first."
  exit 0
fi

archive=""
if have zip; then
  archive="$PARENT/${BUNDLE_NAME}.zip"
  ( cd "$STAGING" && run_to zip -q -r -9 "$archive" "$BUNDLE_NAME" ) || archive=""
fi
if [[ -z "$archive" ]] && have tar; then
  archive="$PARENT/${BUNDLE_NAME}.tar.gz"
  ( cd "$STAGING" && tar -czf "$archive" "$BUNDLE_NAME" ) || archive=""
fi
if [[ -z "$archive" ]]; then
  fallback="$PARENT/$BUNDLE_NAME"
  cp -r "$BUNDLE" "$fallback"
  say ""
  say "Neither zip nor tar is available. Bundle left as a directory: $fallback"
  exit 0
fi

chmod 600 "$archive" 2>/dev/null || true
say ""
say "Bundle: $archive"
say "Size:   $(du -h "$archive" 2>/dev/null | awk '{print $1}')"
say "Send this file to Bifract support. README.txt inside describes the contents."
