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
  local version="${TINYGO_VERSION:-0.39.0}"

  if has_cmd tinygo; then
    echo "tinygo already installed: $(tinygo version || true)"
    return
  fi

  echo "Installing tinygo v${version}..."

  local uname_s uname_m platform arch ext
  uname_s="$(uname -s)"
  uname_m="$(uname -m)"

  case "$uname_s" in
    Linux)   platform="linux";   ext="tar.gz" ;;
    Darwin)  platform="darwin";  ext="tar.gz" ;;
    MINGW*|MSYS*|CYGWIN*|Windows_NT) platform="windows"; ext="zip" ;;
    *) echo "Unsupported OS: $uname_s"; exit 1 ;;
  esac

  case "$uname_m" in
    x86_64|amd64) arch="amd64" ;;
    arm64|aarch64) arch="arm64" ;;
    i386|i686) arch="386" ;;
    *) echo "Unsupported arch: $uname_m"; exit 1 ;;
  esac

  local filename="tinygo${version}.${platform}-${arch}.${ext}"
  local url="https://github.com/tinygo-org/tinygo/releases/download/v${version}/${filename}"

  local tmpdir
  tmpdir="$(mktemp -d)"
  echo "Downloading ${url}"
  local archive="${tmpdir}/${filename}"
  if command -v curl >/dev/null 2>&1; then
    curl -fL -o "$archive" "$url"
  else
    wget -O "$archive" "$url"
  fi

  local extracted="${tmpdir}/extracted"
  mkdir -p "$extracted"
  if [[ "$ext" == "zip" ]]; then
    command -v unzip >/dev/null 2>&1 || { echo "unzip is required"; exit 1; }
    unzip -q "$archive" -d "$extracted"
  else
    tar -xzf "$archive" -C "$extracted"
  fi

  local INSTALL_ROOT="${HOME}/.local/opt/tinygo/${version}"
  rm -rf "$INSTALL_ROOT"
  mkdir -p "$INSTALL_ROOT"

  if [[ -d "${extracted}/tinygo" ]]; then
    ( shopt -s dotglob nullglob; mv "${extracted}/tinygo/"* "$INSTALL_ROOT"/ )
  else
    echo "Could not find extracted 'tinygo' directory" >&2
    rm -rf "$tmpdir"
    exit 1
  fi

  mkdir -p "${HOME}/.local/bin"
  ln -sf "${INSTALL_ROOT}/bin/tinygo" "${HOME}/.local/bin/tinygo"

  if ! echo ":$PATH:" | grep -q ":${HOME}/.local/bin:"; then
    export PATH="${HOME}/.local/bin:${PATH}"
  fi

  local profile_snippet='
# TinyGo (installed by Tangent setup)
export PATH="$HOME/.local/bin:$PATH"
export TINYGOROOT="$HOME/.local/opt/tinygo/'"$version"'"
'
  for f in "${HOME}/.bashrc" "${HOME}/.zshrc"; do
    [[ -f "$f" ]] || continue
    if ! grep -q 'TINYGOROOT=.*\.local/opt/tinygo' "$f"; then
      printf "%s\n" "$profile_snippet" >> "$f"
    fi
  done

  export TINYGOROOT="$INSTALL_ROOT"

  persist_path_userbin
  persist_tinygoroot "$version"

  if [[ -n "${ZSH_VERSION:-}" && -f "$HOME/.zshrc" ]]; then
  echo "Sourcing ~/.zshrc so PATH and TINYGOROOT take effect..."
  # shellcheck disable=SC1090
  source "$HOME/.zshrc"
elif [[ -n "${BASH_VERSION:-}" && -f "$HOME/.bashrc" ]]; then
  echo "Sourcing ~/.bashrc so PATH and TINYGOROOT take effect..."
  # shellcheck disable=SC1090
  source "$HOME/.bashrc"
elif [[ -f "$HOME/.profile" ]]; then
  echo "Sourcing ~/.profile so PATH and TINYGOROOT take effect..."
  # shellcheck disable=SC1090
  source "$HOME/.profile"
fi

  echo "tinygo installed to ${INSTALL_ROOT}"
  "${HOME}/.local/bin/tinygo" version || true
  "${HOME}/.local/bin/tinygo" env || true

  rm -rf "$tmpdir"
}

persist_export_line() {
  local line="$1"
  local profiles=(
    "${HOME}/.zshrc" "${HOME}/.zprofile"
    "${HOME}/.bashrc" "${HOME}/.bash_profile"
    "${HOME}/.profile"
  )
  for f in "${profiles[@]}"; do
    [[ -f "$f" ]] || continue
    if ! grep -Fqx "$line" "$f"; then
      printf '\n%s\n' "$line" >> "$f"
    fi
  done
}

persist_path_userbin() {
  persist_export_line 'export PATH="$HOME/.local/bin:$PATH"'
}

persist_tinygoroot() {
  local ver="$1"
  persist_export_line "export TINYGOROOT=\"\$HOME/.local/opt/tinygo/${ver}\""
}

print_versions() {
  echo "==> Versions"
  if has_cmd wasm-tools; then wasm-tools --version || true; else echo "wasm-tools: not found"; fi
  if has_cmd tinygo; then tinygo version || true; else echo "tinygo: not found"; fi
}

install_wasm_tools
install_tinygo

print_versions

echo "==> Done"