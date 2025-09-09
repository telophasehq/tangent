#!/bin/bash
set -euo pipefail

echo "Testing Rust Agent Sidecar with Vector → Sidecar…"

NET=tangent-test
APP=test-app
SIDECAR=light-node
VECTOR=vector
VECTOR_CFG="$(pwd)/vector.toml"
VECTOR_IMAGE="timberio/vector:0.49.0-alpine"

if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running"; exit 1
fi

echo "Cleaning up…"
docker rm -f "$APP" "$SIDECAR" "$VECTOR" 2>/dev/null || true
docker network rm "$NET" 2>/dev/null || true

echo "Creating network ${NET}…"
docker network create "$NET" >/dev/null

echo "Starting ${APP}…"
docker run -d --name "$APP" --network "$NET" nginx:alpine >/dev/null

echo "Starting light-node (${SIDECAR})…"
docker run -d --name "$SIDECAR" \
  --network "$NET" \
  -p 3000:3000 \
  -e LOG_LEVEL=info \
  light-node >/dev/null

echo "Waiting for sidecar to become ready…"
for i in {1..40}; do
  if docker exec "$SIDECAR" sh -lc 'curl -fsS http://light-node:3000/health' >/dev/null 2>&1; then
    echo "Sidecar is up."
    break
  fi
  sleep 0.25
done


echo "Starting Vector…"
docker run -d --name "$VECTOR" \
  --network "$NET" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$VECTOR_CFG":/etc/vector/vector.toml:ro \
  -e VECTOR_LOG=debug \
  "$VECTOR_IMAGE" -c /etc/vector/vector.toml >/dev/null

echo "Generating test logs…"
docker exec "$APP" sh -c 'echo "INFO: Application started" 1>&2; echo "ERROR: Test error message" 1>&2; echo "WARN: Test warning" 1>&2'
docker exec "$APP" sh -c 'wget -qO- localhost >/dev/null 2>&1 || true'
docker exec "$APP" sh -c 'wget -qO- localhost/doesnotexist >/dev/null 2>&1 || true'

echo "Waiting for Vector to ship events…"
sleep 6

echo
echo "===== Vector logs ====="
docker logs --since=30s "$VECTOR" || true

echo
echo "===== Sidecar logs ====="
docker logs --since=30s "$SIDECAR" || true

echo
echo "Cleaning up…"
docker rm -f "$APP" "$SIDECAR" "$VECTOR" >/dev/null || true
docker network rm "$NET" >/dev/null || true

echo "Done. Vector tailed $APP and posted to $SIDECAR."
