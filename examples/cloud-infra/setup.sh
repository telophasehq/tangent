#!/usr/bin/env bash
# Toolchain for the packs' Rust plugins, and the enrichment data they read.
set -euo pipefail
cd "$(dirname "$0")"

has_cmd() { command -v "$1" >/dev/null 2>&1; }

if ! has_cmd rustup; then
  echo "rustup not found. Install Rust from https://rustup.rs first." >&2
  exit 1
fi
rustup target add wasm32-wasip1 --toolchain stable >/dev/null 2>&1 || true
rustup target add wasm32-wasip2 --toolchain stable >/dev/null 2>&1 || true
has_cmd cargo-component || { echo "==> Installing cargo-component"; cargo install cargo-component --locked; }
has_cmd wasm-tools || { echo "==> Installing wasm-tools"; cargo install wasm-tools; }

# GeoLite2, the cloud providers' ranges, the Tor exit list: the runtime's
# enrichment tables (packs/core/ingest.yaml). See examples/enrich.
../../examples/enrich/setup.sh data

echo "==> Done. Edit profile/profile.yaml, then: make test, make run"
