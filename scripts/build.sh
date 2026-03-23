#!/bin/bash
set -e

docker compose -f docker-compose.yml -f docker-compose.dev.yml build bifract || { echo "Build failed." >&2; exit 1; }
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d || { echo "Failed to start containers." >&2; exit 1; }

echo "Bifract is up and running at http://localhost:8080"
