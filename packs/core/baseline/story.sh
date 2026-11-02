#!/usr/bin/env bash
# The sign-in story: tests/input.json through test.yaml, the renderer and then
# the builder on each record, over one state. Compares the output with
# tests/expected.json, or with `write` writes that instead, only when every
# record came out (make -C packs fixtures-baseline); read the diff before
# committing.
#
#   ./story.sh          (make -C packs test-baseline)
#   ./story.sh write    (make -C packs fixtures-baseline)
set -euo pipefail
cd "$(dirname "$0")"
T=${TANGENT:-tangent}
export RUST_LOG=${RUST_LOG:-warn}

"$T" plugin compile --config test.yaml
rm -f test_out.ndjson
"$T" run --once --config test.yaml

python3 -I - "${1:-check}" <<'PY'
import json, os, sys
given = json.load(open("tests/input.json"))
rows = [json.loads(l) for l in open("test_out.ndjson") if l.strip()] if os.path.exists("test_out.ndjson") else []
if len(rows) != len(given):
    sys.exit(f"{len(rows)} records out of the {len(given)} in tests/input.json; tests/expected.json is as it was")
if sys.argv[1] == "write":
    json.dump(rows, open("tests/expected.json", "w"), indent=1)
    print(f"tests/expected.json: {len(rows)} records; now: git diff tests/expected.json")
    sys.exit()
want = json.load(open("tests/expected.json"))
for i, (w, g) in enumerate(zip(want, rows)):
    if w != g:
        show = lambda r: json.dumps(r.get("_baseline", r), sort_keys=True)
        sys.exit(f"record {i} differs from tests/expected.json\n  expected: {show(w)}\n  produced: {show(g)}")
if len(want) != len(rows):
    sys.exit(f"tests/expected.json has {len(want)} records, the story {len(rows)}")
print(f"the story: {len(rows)} records, as tests/expected.json has them")
PY
