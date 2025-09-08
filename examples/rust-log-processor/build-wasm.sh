#!/bin/bash

set -e

echo "Building Rust Log Processor WASM module..."

# Check if wasm-pack is installed
if ! command -v wasm-pack &> /dev/null; then
    echo "Installing wasm-pack..."
    cargo install wasm-pack
fi

# Build the WASM module
echo "Building WASM module..."
wasm-pack build --target web --out-dir ../../wasm

echo "WASM module built successfully!"
echo "Output directory: ../../wasm/"
echo ""
echo "To use this module:"
echo "1. Copy the generated .wasm file to your EFS volume"
echo "2. Update the sidecar config to point to the module"
echo "3. Restart the sidecar container" 