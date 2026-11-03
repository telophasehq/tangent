"""A replay in N shards at once. Shard k judges a contiguous run of the windows after replaying every earlier one as
history (replay.py's FROM_WINDOW), in its own work dir under WORK; then the shards' sinks are joined into WORK, which
score_v2.py reads as one run. Same records judged, with the same context, as one replay; about N times sooner.

    python3 -I evals/harness/shard.py WORK STREAM WINDOWS N   (WORK holds the configs, as for replay.py; each shard gets a copy)"""
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone

work, stream, windows, n = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
work = os.path.abspath(work)
here = os.path.dirname(os.path.abspath(__file__))


def ms(s):
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def iso(t):
    return datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# the windows as replay.py merges them, so a shard's prefix is the same windows
merged = []
for a, b in sorted([ms(a), ms(b)] for a, b in json.load(open(windows))):
    if merged and a <= merged[-1][1]:
        merged[-1][1] = max(merged[-1][1], b)
    else:
        merged.append([a, b])
cuts = [round(len(merged) * k / n) for k in range(n + 1)]

open(os.path.join(work, "started"), "w").write(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ\n"))
procs = []
for k in range(n):
    d = os.path.join(work, f"s{k}")
    os.makedirs(d, exist_ok=True)
    for f in sorted(f for f in os.listdir(work) if f.endswith((".yaml", ".md"))):  # maincfg.py's configs, profile and judge.md
        text = open(os.path.join(work, f)).read().replace(work + "/", d + "/")
        if f == "ingest.yaml":
            port = next(l.rsplit(":", 1)[1] for l in text.splitlines() if "bind_address:" in l)
            text = text.replace(f":{port}", f":{free_port()}")
        open(os.path.join(d, f), "w").write(text)
    json.dump([[iso(a), iso(b)] for a, b in merged[:cuts[k + 1]]], open(os.path.join(d, "windows.json"), "w"))
    env = dict(os.environ, FROM_WINDOW=str(cuts[k] + 1))
    procs.append(subprocess.Popen([sys.executable, "-I", os.path.join(here, "replay.py"), d, stream, os.path.join(d, "windows.json")],
                                  env=env, stderr=open(os.path.join(d, "run.log"), "w")))
    time.sleep(2)
codes = [p.wait() for p in procs]
open(os.path.join(work, "ended"), "w").write(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ\n"))

for f in ("verdicts.ndjson", "seen.ndjson", "alerts.ndjson", "judge.log", "headers.ndjson", "history.log"):
    with open(os.path.join(work, f), "w") as out:
        for k in range(n):
            p = os.path.join(work, f"s{k}", f)
            if os.path.exists(p):
                out.write(open(p, errors="replace").read())
print("shards exited", codes)
# a shard killed by a signal exits negative, which max() would count as success
sys.exit(0 if all(c == 0 for c in codes) else 1)
