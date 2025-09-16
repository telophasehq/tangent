#!/bin/bash
set -euo pipefail

echo "Testing Rust Agent Sidecar with Vector → Sidecar (UDS)…"

APP=test-app
SIDECAR=light-node
VECTOR=vector
SOCKVOL=sidecar-sock
SOCKPATH=/tmp/tangent-sidecar.sock
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
VECTOR_CFG="$SCRIPT_DIR/vector.toml"
VECTOR_IMAGE="timberio/vector:0.49.0-alpine"

if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running"; exit 1
fi

echo "Cleaning up…"
docker rm -f "$APP" "$SIDECAR" "$VECTOR" 2>/dev/null || true
docker volume rm "$SOCKVOL" 2>/dev/null || true

echo "Creating shared socket volume…"
docker volume create "$SOCKVOL" >/dev/null

echo "Building light-node…"
docker build -t light-node .

echo "Starting ${APP}…"
docker run -d --name "$APP" nginx:alpine >/dev/null

echo "Starting light-node (${SIDECAR})…"
docker run -d --name "$SIDECAR" \
  -e WASM_COMPONENT=/wasm/app.component.wasm \
  -e SOCKET_PATH="$SOCKPATH" \
  -e SOCKET_MODE=660 \
  -e RUST_LOG=info \
  -v "$SOCKVOL":/tmp \
  light-node >/dev/null

echo "Waiting for sidecar socket to appear…"
for i in {1..80}; do
  if docker exec "$SIDECAR" sh -lc "test -S '$SOCKPATH'"; then
    echo "Sidecar socket is ready at $SOCKPATH."
    break
  fi
  sleep 0.25
  if [ "$i" -eq 80 ]; then
    echo "Timeout waiting for sidecar socket."; exit 1
  fi
done

echo "Starting Vector…"
docker run -d --name "$VECTOR" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$VECTOR_CFG":/etc/vector/vector.toml:ro \
  -v "$SOCKVOL":/tmp \
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
docker volume rm "$SOCKVOL" >/dev/null || true

echo "Done. Vector tailed $APP and posted to $SIDECAR via $SOCKPATH."
