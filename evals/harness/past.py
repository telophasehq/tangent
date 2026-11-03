"""The corpus moved 104 weeks back, so the sqlite sink (which takes no time
from the future) keeps its times; weekdays and hours stay as they were.
    python3 -I evals/harness/past.py      # evals/runs/stream/{stream,truth}.* -> {stream,truth}-past.*
    python3 -I evals/harness/past.py v3   # evals/runs/stream/{stream,truth}-v3.* -> {stream,truth}-past-v3.*"""
import json, os, re, sys
from datetime import datetime, timedelta

H = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "runs", "stream")
SHIFT = timedelta(weeks=104)
ISO = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?")
V = "".join("-" + v for v in sys.argv[1:2])


def back(m):
    t = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S") - SHIFT
    return t.strftime("%Y-%m-%dT%H:%M:%S") + (m.group(2) or "") + (m.group(3) or "")


with open(os.path.join(H, f"stream{V}.ndjson")) as f, open(os.path.join(H, f"stream-past{V}.ndjson"), "w") as out:
    for line in f:
        out.write(ISO.sub(back, line))
truth = json.load(open(os.path.join(H, f"truth{V}.json")))
json.dump(json.loads(ISO.sub(back, json.dumps(truth))), open(os.path.join(H, f"truth-past{V}.json"), "w"))
