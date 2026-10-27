#!/usr/bin/env bash
set -euo pipefail

echo "==> Tangent setup: installing dependencies for Rust"

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

install_rustup_target() {
  if ! has_cmd rustup; then
    echo "rustup not found. Install Rust from https://rustup.rs first." >&2
    exit 1
  fi
  rustup target add wasm32-wasi --toolchain stable || true
}

install_cargo_component() {
  if has_cmd cargo-component; then
    cargo component --version || true
    return
  fi

  echo "Installing cargo-component (for WASI preview2 components)..."
  cargo install cargo-component --locked || true
}

install_wasm_tools() {
  if has_cmd wasm-tools; then
    return
  fi
  echo "Installing wasm-tools via cargo..."
  if has_cmd cargo; then
    cargo install wasm-tools || true
  else
    echo "cargo not found; install Rust toolchain first." >&2
    exit 1
  fi
}

install_rustup_target
install_cargo_component
install_wasm_tools

echo "==> Done. Verify versions:"
if has_cmd cargo; then cargo --version || true; fi
if has_cmd cargo-component; then cargo component --version || true; fi
if has_cmd wasm-tools; then wasm-tools --version || true; fi
