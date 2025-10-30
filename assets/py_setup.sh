#!/usr/bin/env bash
set -euo pipefail

echo "==> Tangent setup: installing dependencies for Python"

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

ensure_pipx() {
  if has_cmd pipx; then
    return
  fi
  echo "Installing pipx..."
  if [[ "$OS" == "Darwin" ]] && has_cmd brew; then
    brew update
    brew install pipx
    pipx ensurepath || true
  elif has_cmd apt-get; then
    sudo apt-get update -y
    sudo apt-get install -y pipx
    pipx ensurepath || true
  else
    if has_cmd python3; then
      python3 -m pip install --user pipx
      python3 -m pipx ensurepath || true
    else
      echo "python3 not found. Please install Python 3.10+ and re-run." && exit 1
    fi
  fi
}

install_componentize_py() {
  if has_cmd componentize-py; then
    echo "componentize-py already installed"
    return
  fi
  echo "Installing componentize-py..."
  ensure_pipx
  # Prefer pipx for isolated install
  if has_cmd pipx; then
    pipx install --python python3 componentize-py || pipx upgrade componentize-py || true
  else
    # Fallback to user install
    if has_cmd python3; then
      python3 -m pip install --user componentize-py
    else
      echo "python3 not found. Please install Python 3.10+ and re-run." && exit 1
    fi
  fi
}

print_versions() {
  echo "\n==> Versions"
  if has_cmd wasm-tools; then wasm-tools --version || true; else echo "wasm-tools: not found"; fi
  if has_cmd componentize-py; then componentize-py --version || true; else echo "componentize-py: not found"; fi
}

install_wasm_tools
install_componentize_py

print_versions

echo "\n==> Done. You may need to restart your shell so pipx changes take effect."


