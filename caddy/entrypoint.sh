#!/bin/sh
# Install jq for log field transformation
apk add --no-cache jq > /dev/null 2>&1

# Start the log shipper in the background
/usr/local/bin/ship-logs.sh &

# Run Caddy as PID 1
exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile
