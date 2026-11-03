"""The replay's windows for a truth file: an hour either side of every case
event, merged.
    python3 -I evals/harness/windows.py evals/runs/stream/truth-past-v3.json evals/runs/windows-v3.json"""
import json
import sys
from datetime import datetime, timedelta

truth, out = sys.argv[1:3]
p = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
H = timedelta(hours=1)
merged = []
for a, b in sorted((p(t) - H, p(t) + H) for c in json.load(open(truth)) for t in c["times"]):
    if merged and a <= merged[-1][1]:
        merged[-1][1] = max(merged[-1][1], b)
    else:
        merged.append([a, b])
json.dump([[a.strftime("%Y-%m-%dT%H:%M:%SZ"), b.strftime("%Y-%m-%dT%H:%M:%SZ")] for a, b in merged], open(out, "w"))
print(f"{len(merged)} windows")
