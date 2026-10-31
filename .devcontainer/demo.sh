#!/usr/bin/env bash
# Runs the Try Bifract codespace: install, load, rotate and welcome, one per
# lifecycle hook in devcontainer.json. Each step can run again safely.
set -euo pipefail

INSTALL_DIR=/opt/bifract
PASSWORD_FILE=$INSTALL_DIR/admin-password
# The codespace the stack was installed in; another name means a prebuild's copy.
MARKER=$INSTALL_DIR/.codespace
DATASET=bifract-labs-001
BASE=https://localhost
NOTICE=/usr/local/etc/vscode-dev-containers/first-run-notice.txt

log() { printf '\n==> %s\n' "$*"; }

wait_for() {
	local what=$1 seconds=$2
	shift 2
	for ((i = 0; i < seconds; i += 2)); do
		"$@" >/dev/null 2>&1 && return 0
		sleep 2
	done
	echo "Timed out after ${seconds}s waiting for $what" >&2
	return 1
}

healthy() { curl -fsk "$BASE/api/v1/health"; }

# Signs in as admin, keeping the session in $JAR.
login() {
	JAR=$(mktemp)
	jq -n --arg p "$(sudo cat "$PASSWORD_FILE")" '{username: "admin", password: $p}' |
		curl -fsk -c "$JAR" -H 'Content-Type: application/json' --data @- "$BASE/api/v1/auth/login" >/dev/null
}

api() { curl -sk -b "$JAR" -H 'Content-Type: application/json' "$@"; }

# The newest import of the dataset, as JSON, or nothing.
dataset_import() {
	api "$BASE/api/v1/exchange/datasets" | jq -c --arg d "$DATASET" '[.data[]? | select(.dataset_id == $d)][0] // empty'
}

install_stack() {
	if [ -f "$INSTALL_DIR/.env" ]; then
		log "Bifract is already installed"
		return
	fi
	sudo apt-get update -qq && sudo apt-get install -y -qq jq >/dev/null
	wait_for "Docker" 120 docker info

	local version=${BIFRACT_DEMO_VERSION:?set BIFRACT_DEMO_VERSION in devcontainer.json}
	local cli
	cli=$(mktemp -d)
	if [ "$version" = source ]; then
		log "Building Bifract from this checkout"
		docker build --target builder -t bifract-builder .
		docker build -t ghcr.io/zanegittins/bifract:dev .
		docker run --rm -e CGO_ENABLED=0 -v "$cli:/out" bifract-builder go build -o /out/bifract ./cmd/bifract
	else
		log "Downloading the bifract CLI $version"
		curl -fsSL -o "$cli/bifract" "https://github.com/zaneGittins/bifract/releases/download/$version/bifract-linux-amd64"
	fi
	sudo install -m 0755 "$cli/bifract" /usr/local/bin/bifract

	log "Installing Bifract"
	local config=$cli/install.yaml
	cat >"$config" <<EOF
version: 1
domain: localhost
access: {mode: all}
output: {admin_password: file}
compose:
  install_dir: $INSTALL_DIR
  tls: {mode: self-signed}
EOF
	sudo bifract --install --config "$config"
	echo "${CODESPACE_NAME:-}" | sudo tee "$MARKER" >/dev/null
	wait_for "Bifract" 300 healthy
}

load() {
	wait_for "Bifract" 300 healthy
	login
	if [ -z "$(dataset_import)" ]; then
		# pgr() baselines build only from logs that arrive after this.
		log "Turning on endpoint analytics"
		api --fail -X POST --data '{"enabled": true}' "$BASE/api/v1/system/endpoint-analysis" >/dev/null
		log "Adding $DATASET from the Exchange"
		api -X POST "$BASE/api/v1/exchange/datasets/$DATASET/import" | jq -r '.error // "Import queued"'
	fi
	local imp status
	while :; do
		imp=$(dataset_import)
		status=$(jq -r '.status // "missing"' <<<"$imp")
		case $status in
		succeeded)
			log "Lab 001 is loaded: $(jq -r .logs_done <<<"$imp") logs"
			return
			;;
		failed | missing)
			echo "Loading $DATASET failed: $(jq -r '.error // "no import"' <<<"$imp")" >&2
			return 1
			;;
		esac
		printf '  %s: %s of %s logs\n' "$status" "$(jq -r .logs_done <<<"$imp")" "$(jq -r .logs_total <<<"$imp")"
		sleep 20
	done
}

# A codespace made from a prebuild shares the prebuild's admin password, so it
# gets its own.
rotate() {
	if [ "$(sudo cat "$MARKER" 2>/dev/null)" = "${CODESPACE_NAME:-}" ]; then
		return
	fi
	wait_for "Bifract" 300 healthy
	login
	local next
	next=$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24)
	jq -n --arg c "$(sudo cat "$PASSWORD_FILE")" --arg n "$next" '{current_password: $c, new_password: $n}' |
		api --fail -X POST --data @- "$BASE/api/v1/auth/change-password" >/dev/null
	printf '%s' "$next" | sudo tee "$PASSWORD_FILE" >/dev/null
	echo "${CODESPACE_NAME:-}" | sudo tee "$MARKER" >/dev/null
	log "Set a new admin password for this codespace"
}

welcome() {
	wait_for "Bifract" 300 healthy || true
	local url="$BASE"
	if [ -n "${CODESPACE_NAME:-}" ]; then
		url="https://$CODESPACE_NAME-443.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-app.github.dev}"
	fi
	local text
	text=$(
		cat <<EOF
Bifract is running at $url
  Username: admin
  Password: $(sudo cat "$PASSWORD_FILE")

Lab 001 (Meridian workstation intrusion) is in fractal $DATASET, or loading
into it: Exchange shows progress. The time picker opens on the lab's range, and
endpoint analytics is on, so pgr() works.
Getting started: https://docs.bifract.io/getting-started/codespaces/
EOF
	)
	printf '%s\n' "$text" | sudo tee "$NOTICE" >/dev/null
	printf '\n%s\n' "$text"
}

case ${1:-} in
install) install_stack ;;
load | rotate | welcome) "$1" ;;
*)
	echo "usage: $0 install|load|rotate|welcome" >&2
	exit 2
	;;
esac
