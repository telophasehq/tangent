#!/usr/bin/env bash
# The packs' plugins compiled into components, what a release ships and
# `tangent plugin fetch` downloads: target/packs/<plugin>.component.wasm for
# each plugin the packs' configs name. dist builds them on release
# (dist-workspace.toml's extra-artifacts) and .github/workflows/attest-packs.yml
# attests them; run it here to see what a release would ship. A component is
# portable, built once and run on every machine, so only this needs the WASM
# toolchain: cargo-component, pinned, and the wasm32-wasip2 target, installed
# when missing.
#
#   packs/dist.sh          build them all
#   packs/dist.sh check    only check that PLUGINS below names every plugin
#                          the configs mark `prebuilt` (CI runs this)
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=target/packs
CARGO_COMPONENT_VERSION=0.21.1

# Each plugin as the configs name it, and its source. The configs with a
# `prebuilt:` are the ingest.yaml of each pack.
PLUGINS=(
  "aws packs/aws"
  "github packs/github"
  "okta packs/okta"
  "google_workspace packs/google_workspace"
  "app packs/app"
  "baseline_builder packs/behavior-baseline/builder"
  "baseline_renderer packs/behavior-baseline/renderer"
  "fresh packs/judge/fresh"
  "flagged examples/inference-local/flagged"
)

# The plugins the configs mark prebuilt: the key two lines above each
# `prebuilt:` at a plugin's indent.
prebuilt_in_configs() {
  for f in packs/*/ingest.yaml; do
    awk '/^  [a-z_]+:/ { name=$1; sub(":", "", name) } /^    prebuilt:/ { print name }' "$f"
  done | sort -u
}

check() {
  local listed configured
  listed=$(printf '%s\n' "${PLUGINS[@]}" | cut -d' ' -f1 | sort -u)
  configured=$(prebuilt_in_configs)
  if [ "$listed" != "$configured" ]; then
    echo "packs/dist.sh: PLUGINS and the configs' prebuilt plugins differ:" >&2
    diff <(echo "$listed") <(echo "$configured") >&2 || true
    exit 1
  fi
  for entry in "${PLUGINS[@]}"; do
    set -- $entry
    [ -f "$2/Cargo.toml" ] || { echo "packs/dist.sh: $1: no $2/Cargo.toml" >&2; exit 1; }
  done
  echo "packs/dist.sh: ${#PLUGINS[@]} plugins, as the configs name them"
}

toolchain() {
  command -v rustup >/dev/null 2>&1 || { echo "rustup not found: install Rust from https://rustup.rs" >&2; exit 1; }
  rustup target add wasm32-wasip2 >/dev/null 2>&1
  local have
  have=$(cargo component --version 2>/dev/null | awk '{print $2}' || true)
  if [ "$have" = "$CARGO_COMPONENT_VERSION" ]; then
    return
  fi
  if [ -n "$have" ] && [ -z "${CI:-}" ]; then
    echo "==> cargo-component $have here, $CARGO_COMPONENT_VERSION on release; building with $have" >&2
    return
  fi
  echo "==> Installing cargo-component $CARGO_COMPONENT_VERSION"
  cargo install cargo-component --version "$CARGO_COMPONENT_VERSION" --locked
}

build() {
  local name=$1 dir=$2 pkg
  echo "==> $name ($dir)"
  # The WIT this checkout's tangent hosts, where `tangent plugin compile`
  # writes it: plugins keep no copy of their own.
  rm -rf "$dir/.tangent/wit"
  mkdir -p "$dir/.tangent"
  cp -R assets/wit "$dir/.tangent/wit"
  (cd "$dir" && cargo component build --release --target wasm32-wasip2)
  pkg=$(sed -n 's/^name *= *"\(.*\)"/\1/p' "$dir/Cargo.toml" | head -1)
  cp "$dir/target/wasm32-wasip2/release/$pkg.wasm" "$OUT/$name.component.wasm"
}

case "${1:-build}" in
  check) check ;;
  build)
    check
    toolchain
    mkdir -p "$OUT"
    for entry in "${PLUGINS[@]}"; do
      set -- $entry
      build "$1" "$2"
    done
    echo "==> $OUT"
    (cd "$OUT" && shasum -a 256 ./*.component.wasm)
    ;;
  *) echo "usage: $0 [build|check]" >&2; exit 2 ;;
esac
