#!/bin/sh
# Ships Caddy JSON access logs to Bifract system fractal via internal ingest API.
# Runs as a background process inside the Caddy container.
# Transforms Caddy fields to use src_ip/src_port/dest_ip/dst_port naming.

BIFRACT_URL="${BIFRACT_INGEST_URL:-http://bifract-app:8080/api/v1/internal/ingest/system}"
BATCH_SIZE="${BATCH_SIZE:-50}"
FLUSH_INTERVAL="${FLUSH_INTERVAL:-10}"
LOG_FILE="/var/log/caddy/access.log"
BATCH_FILE="/tmp/caddy-batch.ndjson"

# Wait for jq to be available (installed by entrypoint)
while ! command -v jq > /dev/null 2>&1; do
    sleep 1
done

# Wait for log file to exist
while [ ! -f "$LOG_FILE" ]; do
    sleep 2
done

# Wait for Bifract to be healthy
while true; do
    if curl -sf http://bifract-app:8080/api/v1/health > /dev/null 2>&1; then
        break
    fi
    sleep 5
done

# Transform Caddy log to add normalized IP fields
transform_line() {
    echo "$1" | jq -c '
        . + {
            src_ip: (.request.client_ip // .request.remote_ip // ""),
            src_port: (.request.remote_port // ""),
            dest_ip: ((.request.host // "") | split(":")[0]),
            dst_port: ((.request.host // "") | split(":")[1] // "")
        }
    ' 2>/dev/null || echo "$1"
}

ship_file() {
    if [ -f "$1" ] && [ -s "$1" ]; then
        curl -sf -X POST "$BIFRACT_URL" \
            -H "Content-Type: application/x-ndjson" \
            --data-binary @"$1" > /dev/null 2>&1
        rm -f "$1"
    fi
}

# Background flush: periodically ship whatever is in the batch file
while true; do
    sleep "$FLUSH_INTERVAL"
    if [ -f "$BATCH_FILE" ] && [ -s "$BATCH_FILE" ]; then
        mv "$BATCH_FILE" /tmp/caddy-sending.ndjson 2>/dev/null && \
        ship_file /tmp/caddy-sending.ndjson
    fi
done &

# Tail new lines, transform and accumulate, ship when batch is full
tail -n 0 -F "$LOG_FILE" 2>/dev/null | while IFS= read -r line; do
    transformed=$(transform_line "$line")
    echo "$transformed" >> "$BATCH_FILE"
    lines=$(wc -l < "$BATCH_FILE" 2>/dev/null || echo 0)
    if [ "$lines" -ge "$BATCH_SIZE" ]; then
        if [ -f "$BATCH_FILE" ] && [ -s "$BATCH_FILE" ]; then
            mv "$BATCH_FILE" /tmp/caddy-sending.ndjson 2>/dev/null && \
            ship_file /tmp/caddy-sending.ndjson
        fi
    fi
done
