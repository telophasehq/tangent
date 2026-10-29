#!/usr/bin/env bash
# Regenerates tests/*/{input,expected}.json from a handful of cases by running
# each stage once: raw events → norm → enrich → gate → window. Run after a
# plugin changes, read the diff, commit it if it is what you meant.
set -euo pipefail
cd "$(dirname "$0")/.."
T=${TANGENT:-tangent}
export JUDGE_URL=${JUDGE_URL:-http://localhost:8080/v1} JUDGE_MODEL=${JUDGE_MODEL:-stub} JUDGE_API_KEY=${JUDGE_API_KEY:-none}
CASES="cloud/root-console-login cloud/cloudtrail-stop-logging identity/impossible-travel supply-chain/ci-action-tags-retargeted
       vendor/platform-breach-env-secrets-reused negative/oncall-3am-login negative/scanner-action-usual-run"

python3 - $CASES <<'PY'
import json, sys
events = []
for c in sys.argv[1:]:
    events += [json.loads(l) for l in open(f"cases/{c}/events.ndjson") if l.strip()]
events.sort(key=lambda e: e["ts"])
json.dump(events, open("tests/norm/input.json", "w"), indent=1)
print(f"{len(events)} events from {len(sys.argv) - 1} cases")
PY

# One stage at a time, each against the real config's runtime and plugin entry.
stage() {  # name  input  output
  python3 - "$1" "$2" <<'PY'
import sys, yaml
name, inp = sys.argv[1], sys.argv[2]
cfg = yaml.safe_load(open("tangent.yaml"))
plugin = cfg["plugins"][name]
plugin["tests"] = [{"input": inp, "expected": ".empty.json"}]
stores = {n: s for n, s in cfg["sinks"].items() if s["type"] == "store"}  # the test writes history first
stage = {"runtime": cfg["runtime"], "plugins": {name: plugin},
         "sources": {"events": {"type": "tcp", "bind_address": "127.0.0.1:0"}},
         "sinks": {"out": {"type": "blackhole"}, **stores},
         "dag": [{"from": {"kind": "source", "name": "events"}, "to": [{"kind": "plugin", "name": name}]},
                 {"from": {"kind": "plugin", "name": name}, "to": [{"kind": "sink", "name": "out"}]}]}
yaml.safe_dump(stage, open(".stage.yaml", "w"), sort_keys=False)
PY
  echo '[]' > .empty.json
  "$T" plugin test --config .stage.yaml >/dev/null 2>&1 || true
  python3 -c "
import json, sys
rows = [json.loads(l) for l in open('test_out.ndjson') if l.strip()]
json.dump(rows, open(sys.argv[1], 'w'), indent=1)
print(f'  {sys.argv[2]}: {len(rows)} records')
" "$3" "$1"
  rm -f test_out.ndjson .stage.yaml .empty.json
}

stage norm   tests/norm/input.json     tests/norm/expected.json
stage enrich tests/norm/expected.json  tests/gate/input.json
stage gate   tests/gate/input.json     tests/gate/expected.json
cp tests/gate/expected.json tests/window/input.json
stage window tests/window/input.json   tests/window/expected.json
echo "fixtures written; now: make test"
