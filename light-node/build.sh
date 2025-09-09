#!/bin/bash

set -e

echo "Building Rust Agent Sidecar..."

# Build the Rust application
echo "Building Rust application..."
cargo build --release

# Build the Docker image
echo "Building Docker image..."
docker build -t tangent-sidecar .

echo "Build complete!"
echo ""
echo "To run locally:"
echo "  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock tangent-sidecar"
echo ""
echo "To test with a sample container:"
echo "  docker run -d --name test-app nginx:alpine"
echo "  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -e SIDECAR_TARGET_CONTAINER=test-app tangent-sidecar" 