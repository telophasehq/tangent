#!/usr/bin/env bash
set -euo pipefail

echo "==> Tangent setup: installing dependencies for Go"

OS=$(uname -s)

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

install_wasm_tools() {
  if has_cmd wasm-tools; then
    echo "wasm-tools already installed"
    return
  fi

  echo "Installing wasm-tools..."
  if [[ "$OS" == "Darwin" ]] && has_cmd brew; then
    brew update
    brew install wasm-tools || true
  elif has_cmd apt-get; then
    sudo apt-get update -y
    # wasm-tools may not exist on all distros; fall back to cargo
    if apt-cache show wasm-tools >/dev/null 2>&1; then
      sudo apt-get install -y wasm-tools
    else
      if has_cmd cargo; then
        cargo install wasm-tools
      else
        echo "cargo not found. Please install Rust toolchain (https://rustup.rs) then re-run." && exit 1
      fi
    fi
  else
    if has_cmd cargo; then
      cargo install wasm-tools
    else
      echo "Could not install wasm-tools automatically. Install Rust (https://rustup.rs) and run: cargo install wasm-tools" && exit 1
    fi
  fi
}

install_tinygo() {
  if has_cmd tinygo; then
    echo "tinygo already installed"
    return
  fi

  echo "Installing tinygo..."
  if [[ "$OS" == "Darwin" ]] && has_cmd brew; then
    brew update
    brew install tinygo
  elif has_cmd apt-get; then
    sudo apt-get update -y
    sudo apt-get install -y tinygo
  else
    echo "Could not install tinygo automatically. See https://tinygo.org/getting-started/install/ for options." && exit 1
  fi
}

print_versions() {
  echo "\n==> Versions"
  if has_cmd wasm-tools; then wasm-tools --version || true; else echo "wasm-tools: not found"; fi
  if has_cmd tinygo; then tinygo version || true; else echo "tinygo: not found"; fi
}

install_wasm_tools
install_tinygo

print_versions

echo "\n==> Done"


