#!/usr/bin/env bash
# Toolchain for a Rust plugin, and the data files the plugin reads from /data.
#
#   ./setup.sh          toolchain + data
#   ./setup.sh data     data only (refresh; MaxMind publishes Tuesdays and Fridays)
#
# GeoLite2 comes from MaxMind when MAXMIND_LICENSE_KEY is set (free account:
# https://www.maxmind.com/en/geolite2/signup), otherwise from a public mirror of
# the same files. The ranges and the Tor list need no account.
set -euo pipefail
cd "$(dirname "$0")"

has_cmd() { command -v "$1" >/dev/null 2>&1; }

install_rustup_target() {
  if ! has_cmd rustup; then
    echo "rustup not found. Install Rust from https://rustup.rs first." >&2
    exit 1
  fi
  rustup target add wasm32-wasip1 --toolchain stable >/dev/null 2>&1 || true
  rustup target add wasm32-wasip2 --toolchain stable >/dev/null 2>&1 || true
}

install_cargo_component() {
  if has_cmd cargo-component; then
    return
  fi
  echo "==> Installing cargo-component (WASI preview 2 components)"
  cargo install cargo-component --locked
}

install_wasm_tools() {
  if has_cmd wasm-tools; then
    return
  fi
  echo "==> Installing wasm-tools"
  cargo install wasm-tools
}

# fetch URL DEST: download, or refresh DEST only if the server has a newer file.
fetch() {
  local url=$1 dest=$2
  if [ -f "$dest" ]; then
    curl -fsSL -z "$dest" -o "$dest" "$url" || echo "warning: could not refresh $dest" >&2
  else
    curl -fsSL -o "$dest" "$url"
  fi
}

fetch_geolite2() {
  if [ -n "${MAXMIND_LICENSE_KEY:-}" ]; then
    echo "==> GeoLite2 from MaxMind"
    local tmp
    tmp=$(mktemp -d)
    for edition in GeoLite2-Country GeoLite2-ASN; do
      curl -fsSL "https://download.maxmind.com/app/geoip_download?edition_id=${edition}&license_key=${MAXMIND_LICENSE_KEY}&suffix=tar.gz" \
        | tar -xz -C "$tmp"
      find "$tmp" -name "${edition}.mmdb" -exec mv {} "data/${edition}.mmdb" \;
    done
    rm -rf "$tmp"
  else
    echo "==> GeoLite2 from the P3TERX mirror (MAXMIND_LICENSE_KEY unset; a copy of MaxMind's free data)"
    fetch https://github.com/P3TERX/GeoLite.mmdb/releases/latest/download/GeoLite2-Country.mmdb data/GeoLite2-Country.mmdb
    fetch https://github.com/P3TERX/GeoLite.mmdb/releases/latest/download/GeoLite2-ASN.mmdb data/GeoLite2-ASN.mmdb
  fi
}

fetch_data() {
  mkdir -p data
  fetch_geolite2
  echo "==> Published ranges: AWS, GCP, Cloudflare; the Tor exit list"
  fetch https://ip-ranges.amazonaws.com/ip-ranges.json data/aws-ip-ranges.json
  fetch https://www.gstatic.com/ipranges/cloud.json data/gcp-ip-ranges.json
  fetch https://www.cloudflare.com/ips-v4 data/cloudflare-ips-v4.txt
  fetch https://www.cloudflare.com/ips-v6 data/cloudflare-ips-v6.txt
  fetch https://check.torproject.org/torbulkexitlist data/tor-exits.txt
  ls -la data | awk 'NR>1 && $9 != "" {printf "    %8d  %s\n", $5, $9}'
}

if [ "${1:-}" = "data" ]; then
  fetch_data
  exit 0
fi

echo "==> Tangent setup: Rust toolchain for WASM components"
install_rustup_target
install_cargo_component
install_wasm_tools
fetch_data
echo "==> Done"
