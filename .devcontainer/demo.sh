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
# Opened when the codespace opens; rewritten as the lab loads.
WELCOME=$PWD/WELCOME.md
LAB_STORY=https://github.com/zaneGittins/bifract-labs/tree/main/labs/001
DOCS=https://docs.bifract.io/getting-started/codespaces/

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

# The address the browser reaches Bifract at.
public_url() {
	if [ -n "${CODESPACE_NAME:-}" ]; then
		echo "https://$CODESPACE_NAME-443.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-app.github.dev}"
	else
		echo "$BASE"
	fi
}

# A deep link running a query over the lab's range, $FROM to $TO.
search_url() {
	printf '%s/go/search?fractal=%s&from=%s&to=%s&q=%s' "$(public_url)" "$DATASET" "$FROM" "$TO" \
		"$(jq -rn --arg q "$1" '$q | @uri')"
}

# A number from the import, with thousands separators.
count() { jq -r ".$2" <<<"$1" | numfmt --grouping; }

# Writes WELCOME.md for the lab's state, given its import as JSON (or nothing).
write_welcome() {
	local imp=${1:-null} url status lab range='' try=''
	url=$(public_url)
	status=$(jq -r '.status // ""' <<<"$imp")
	if [ -n "$status" ]; then
		range=$(api "$BASE/api/v1/fractals/$(jq -r .fractal_id <<<"$imp")" |
			jq -r '.data | select(.default_range_start and .default_range_end) | "\(.default_range_start) \(.default_range_end)"')
	fi
	# Searches link once the lab's fractal exists, over its whole range.
	if [ -n "$range" ]; then
		FROM=$(date -u -d "${range% *}" +%Y-%m-%dT%H:%M:00Z)
		TO=$(date -u -d "${range#* } + 1 minute" +%Y-%m-%dT%H:%M:00Z)
		try="**Try a search**

1. [Every process launch]($(search_url 'bifract_category=process_creation | table(timestamp, hostname, user, image, commandline)'))
2. [Process launches per host]($(search_url 'bifract_category=process_creation | groupBy(hostname) | sort(_count, order=desc)'))
3. [Kinds of events]($(search_url '* | groupBy(bifract_category) | sort(_count, order=desc)'))
4. [Rarest programs]($(search_url 'bifract_category=process_creation | groupBy(image) | sort(_count, order=asc) | head(10)'))
5. [Rarest DNS lookups]($(search_url 'event_id=22 | groupBy(query) | sort(_count, order=asc) | head(10)'))
6. Follow a process: \`pgr(start=\"<guid>\") | pgraph()\` with any \`process_guid\`
"
	fi
	local exchange
	exchange="$url/#f/$(jq -r '.fractal_id // ""' <<<"$imp")/exchange/$DATASET"
	case $status in
	succeeded) lab="Loaded: $(count "$imp" logs_done) logs." ;;
	failed) lab="Loading failed: $(jq -r '.error' <<<"$imp"). [Retry in Exchange]($exchange)." ;;
	"") lab="Loading starts in a moment; this page updates as it goes." ;;
	*) lab="Loading: $(count "$imp" logs_done) of $(count "$imp" logs_total) logs. [Watch it in Exchange]($exchange); searches work while it loads." ;;
	esac
	cat >"$WELCOME" <<EOF
<p align="center"><img src="web/static/logo.png" alt="Bifract" width="72"></p>
<h1 align="center">Bifract is running</h1>

**[Open Bifract]($url)** and sign in as \`admin\` with password \`$(sudo cat "$PASSWORD_FILE")\`

## Lab 001: Meridian workstation intrusion

Three days of logs from a small Windows domain, with an intrusion hidden inside.
$lab

$try
---

[The lab's story]($LAB_STORY) · [Codespaces guide]($DOCS) · [Delete this codespace](https://github.com/codespaces) when you are done
EOF
}

# Signs in and rewrites WELCOME.md for the lab's current state.
refresh_welcome() {
	login && write_welcome "$(dataset_import)"
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
	if ! sudo bifract --install --config "$config"; then
		log "Install failed: container state and app log follow"
		sudo docker compose --project-directory "$INSTALL_DIR" ps -a || true
		sudo docker compose --project-directory "$INSTALL_DIR" logs --tail 100 bifract bifract-ingest || true
		return 1
	fi
	echo "${CODESPACE_NAME:-}" | sudo tee "$MARKER" >/dev/null
	wait_for "Bifract" 300 healthy
	write_welcome
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
		write_welcome "$imp"
		status=$(jq -r '.status // "missing"' <<<"${imp:-null}")
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
	refresh_welcome
}

welcome() {
	wait_for "Bifract" 300 healthy && refresh_welcome || true
	local text
	text=$(
		cat <<EOF
Bifract is running at $(public_url)
  Username: admin
  Password: $(sudo cat "$PASSWORD_FILE")
WELCOME.md has what is loaded and what to try.
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
