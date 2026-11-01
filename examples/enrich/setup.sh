#!/usr/bin/env bash
# Toolchain for a Rust plugin, and the data files the plugin reads from /data.
#
#   ./setup.sh          toolchain + data
#   ./setup.sh data     data only (refresh; MaxMind publishes Tuesdays and Fridays)
#   ./setup.sh networks data/networks.csv again from the ranges already in data/
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

# data/networks.csv: every range above in one `prefix,label` file, for a
# runtime `cidr` enrichment table, so no plugin parses a provider's format.
# Labels go in this order, which is the order a lookup answers them in: a
# provider's own ranges, then X4BNet's broad datacenter list, VPN, Tor. A
# file that is missing adds nothing. python3 from the standard library, -I so
# nothing in data/ or the working directory is imported.
build_networks() {
  echo "==> data/networks.csv from the ranges"
  python3 -I - data <<'PY'
import ipaddress, json, os, sys

data = sys.argv[1]


def read(name):
    try:
        with open(os.path.join(data, name), encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""


def lines(*names):
    for name in names:
        for line in read(name).splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                yield line


def published(name, *lists):
    text = read(name)
    if not text:
        return
    try:
        doc = json.loads(text)
    except ValueError:
        print(f"warning: {name} is not JSON; left out", file=sys.stderr)
        return
    for key, fields in lists:
        for entry in doc.get(key) or []:
            for field in fields:
                if isinstance(entry, dict) and isinstance(entry.get(field), str):
                    yield entry[field]


labels = [
    ("aws", published("aws-ip-ranges.json", ("prefixes", ["ip_prefix"]), ("ipv6_prefixes", ["ipv6_prefix"]))),
    ("gcp", published("gcp-ip-ranges.json", ("prefixes", ["ipv4Prefix", "ipv6Prefix"]))),
    ("cloudflare", lines("cloudflare-ips-v4.txt", "cloudflare-ips-v6.txt")),
    ("datacenter", lines("datacenter-ips.txt")),
    ("vpn", lines("vpn-ips.txt")),
    ("tor", lines("tor-exits.txt")),
]
path = os.path.join(data, "networks.csv")
counts = []
with open(path + ".tmp", "w", encoding="utf-8") as out:
    out.write("# Built by setup.sh from the ranges beside it; labels answer in this order.\n")
    out.write("prefix,label\n")
    for label, prefixes in labels:
        seen = set()
        for p in prefixes:
            try:
                net = str(ipaddress.ip_network(p, strict=False))
            except ValueError:
                continue
            if net not in seen:
                seen.add(net)
                out.write(f"{net},{label}\n")
        counts.append(f"{label} {len(seen)}")
os.replace(path + ".tmp", path)
print("    " + ", ".join(counts))
PY
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
  echo "==> Datacenter and VPN ranges (X4BNet)"
  fetch https://raw.githubusercontent.com/X4BNet/lists_vpn/main/output/datacenter/ipv4.txt data/datacenter-ips.txt
  fetch https://raw.githubusercontent.com/X4BNet/lists_vpn/main/output/vpn/ipv4.txt data/vpn-ips.txt
  build_networks
  ls -la data | awk 'NR>1 && $9 != "" {printf "    %8d  %s\n", $5, $9}'
}

if [ "${1:-}" = "data" ]; then
  fetch_data
  exit 0
fi
if [ "${1:-}" = "networks" ]; then
  build_networks
  exit 0
fi

echo "==> Tangent setup: Rust toolchain for WASM components"
install_rustup_target
install_cargo_component
install_wasm_tools
fetch_data
echo "==> Done"
