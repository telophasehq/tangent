#!/usr/bin/env bash
# The enrichment data the packs read; and, asked for, the Rust WASM toolchain,
# which only compiling the packs from source needs (`make compile`): `make
# fetch`, the default, takes them prebuilt from the release.
#
#   ./setup.sh             GeoLite2, cloud ranges, Tor exits into examples/enrich/data
#   ./setup.sh toolchain   the wasm32-wasip2 target and cargo-component too
set -euo pipefail
cd "$(dirname "$0")"

has_cmd() { command -v "$1" >/dev/null 2>&1; }

if [ "${1:-}" = "toolchain" ]; then
  if ! has_cmd rustup; then
    echo "rustup not found. Install Rust from https://rustup.rs first." >&2
    exit 1
  fi
  rustup target add wasm32-wasip1 --toolchain stable >/dev/null 2>&1 || true
  rustup target add wasm32-wasip2 --toolchain stable >/dev/null 2>&1 || true
  has_cmd cargo-component || { echo "==> Installing cargo-component"; cargo install cargo-component --locked; }
  has_cmd wasm-tools || { echo "==> Installing wasm-tools"; cargo install wasm-tools; }
fi

# GeoLite2, the cloud providers' ranges, the Tor exit list: the runtime's
# enrichment tables (packs/enrich/ingest.yaml). See examples/enrich.
../../examples/enrich/setup.sh data

echo "==> Done. Edit profile/profile.yaml, then: make test, make run"
