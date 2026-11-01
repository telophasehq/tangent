#!/usr/bin/env bash
# Writes a pack's tests/expected.json from what its plugin makes of
# tests/input.json, against the tables its ingest.yaml (or $CONFIG) names for
# the test.
# Run after the plugin or its fixtures change, read the diff, commit it if it
# is what you meant. Keep two records or more in tests/input.json: a test of
# one record compares as an object, not a list.
#
# expected.json is written only when the test ran through: it may fail for
# its output differing from expected.json (that is why this runs), but any
# other error (a table, the config, the plugin) stops here, and so does a
# plugin that left out a record.
#
#   ./fixtures.sh okta        (make fixtures-okta)
#   CONFIG=test.yaml ./fixtures.sh core/baseline    (make fixtures-baseline)
set -euo pipefail
[ $# -eq 1 ] || { echo "usage: $0 <pack>" >&2; exit 2; }
cd "$(dirname "$0")/$1"
T=${TANGENT:-tangent}
C=${CONFIG:-ingest.yaml}   # the config naming the fixtures

# A new pack has none yet, and the test needs one to compare with.
[ -f tests/expected.json ] || echo '[]' > tests/expected.json
"$T" plugin compile --config "$C"
rm -f test_out.ndjson
log=$(mktemp)
trap 'rm -f "$log"' EXIT
if ! NO_COLOR=1 "$T" plugin test --config "$C" >"$log" 2>&1; then
  if ! grep -Eq '^Error: (output differs from expected|output is array)' "$log"; then
    cat "$log" >&2
    echo "$0: the test failed for another reason (above); tests/expected.json is as it was" >&2
    exit 1
  fi
fi
python3 -I - <<'PY'
import json, os, sys
given = json.load(open("tests/input.json"))
n = len(given) if isinstance(given, list) else 1
rows = [json.loads(l) for l in open("test_out.ndjson") if l.strip()] if os.path.exists("test_out.ndjson") else []
if len(rows) != n:
    sys.exit(f"{len(rows)} records out of the {n} in tests/input.json; tests/expected.json is as it was")
json.dump(rows, open("tests/expected.json", "w"), indent=1)
print(f"tests/expected.json: {len(rows)} records")
PY
echo "now: git diff tests/expected.json"
