#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/src/docker-compose/docker-compose.local-dev.yml"
DOWNLOAD_ROOT="$ROOT_DIR/src/docker-compose/datamingle/downloads"
COMPOSE=(docker-compose -f "$COMPOSE_FILE")

log() {
  printf '[e2e-reset] %s\n' "$1"
}

clear_directory() {
  local path="$1"

  mkdir -p "$path"
  find "$path" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
}

container_running() {
  local name="$1"
  docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null | grep -q true
}

log "Stopping local Docker stack"
"${COMPOSE[@]}" down -v --remove-orphans || true

log "Clearing generated downloads"
clear_directory "$DOWNLOAD_ROOT/DataExportFile"
clear_directory "$DOWNLOAD_ROOT/dictionary"

log "Rebuilding and starting local Docker stack"
"${COMPOSE[@]}" up -d --build datamingle

log "Waiting for datamingle-app container"
for _ in $(seq 1 60); do
  if container_running "datamingle-app"; then
    break
  fi
  sleep 2
done

if ! container_running "datamingle-app"; then
  log "datamingle-app did not start"
  "${COMPOSE[@]}" ps
  exit 1
fi

log "Waiting for smoke_local_demo to pass"
for attempt in $(seq 1 40); do
  if docker exec datamingle-app python manage.py smoke_local_demo; then
    log "Local demo smoke passed"
    exit 0
  fi

  log "Smoke attempt ${attempt}/40 failed; retrying in 5s"
  sleep 5
done

log "Local demo smoke did not pass in time"
docker logs --tail 200 datamingle-app || true
exit 1
