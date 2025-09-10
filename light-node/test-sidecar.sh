#!/bin/bash

set -e

echo "Testing Rust Agent Sidecar..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running"
    exit 1
fi

echo "Cleaning up existing test containers..."
docker rm -f test-app light-node 2>/dev/null || true

echo "Starting test application container..."
docker run -d --name test-app nginx:alpine

sleep 2

echo "Generating test logs..."
docker exec test-app sh -c "echo 'INFO: Application started' && echo 'ERROR: Test error message' && echo 'WARN: Test warning'"

echo "Starting sidecar container..."
docker run -d --name light-node \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e SIDECAR_TARGET_CONTAINER=test-app \
    -e SIDECAR_LOG_INTERVAL_MS=1000 \
    -e LOG_LEVEL=info \
    tangent-sidecar

echo "Waiting for sidecar to process logs..."
sleep 5

echo "Sidecar logs:"
docker logs light-node

echo "Cleaning up test containers..."
docker rm -f test-app light-node

echo "Test completed successfully!"
echo ""
echo "The sidecar should have:"
echo "1. Found the test-app container"
echo "2. Read its logs"
echo "3. Processed them (with or without WASM)"
echo ""
echo "Check the logs above to verify functionality." 