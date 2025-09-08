#!/bin/bash

set -e

echo "Testing Rust Agent Sidecar..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running"
    exit 1
fi

# Clean up any existing test containers
echo "Cleaning up existing test containers..."
docker rm -f test-app test-sidecar 2>/dev/null || true

# Start a test application container
echo "Starting test application container..."
docker run -d --name test-app nginx:alpine

# Wait a moment for the container to start
sleep 2

# Generate some test logs
echo "Generating test logs..."
docker exec test-app sh -c "echo 'INFO: Application started' && echo 'ERROR: Test error message' && echo 'WARN: Test warning'"

# Start the sidecar
echo "Starting sidecar container..."
docker run -d --name test-sidecar \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e SIDECAR_TARGET_CONTAINER=test-app \
    -e SIDECAR_LOG_INTERVAL_MS=1000 \
    -e RUST_LOG=info \
    rustagent-sidecar

# Wait for the sidecar to process some logs
echo "Waiting for sidecar to process logs..."
sleep 5

# Check sidecar logs
echo "Sidecar logs:"
docker logs test-sidecar

# Clean up
echo "Cleaning up test containers..."
docker rm -f test-app test-sidecar

echo "Test completed successfully!"
echo ""
echo "The sidecar should have:"
echo "1. Found the test-app container"
echo "2. Read its logs"
echo "3. Processed them (with or without WASM)"
echo ""
echo "Check the logs above to verify functionality." 