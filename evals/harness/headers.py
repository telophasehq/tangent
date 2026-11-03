#!/usr/bin/env python3
"""What baseline_renderer tells the judge, from a JUDGE=judge-sim.yaml replay
(WORK/headers.ndjson, no model): how many records carry `_baseline`, its size
as logfmt writes it (once per request), and per case the `_baseline` on each
of its actors' own records, as the lines logfmt writes.

    python3 -I evals/harness/headers.py TRUTH WORK [CASE...]   # no CASE: every case with a twin, and the twins"""
import collections
import json
import os
import sys

truth = json.load(open(sys.argv[1]))
work = sys.argv[2]
want = sys.argv[3:] or [x for c in truth if c.get("twin") for x in (c["id"], c["twin"])]
by = {c["id"]: c for c in truth}
recs = [json.loads(l) for l in open(os.path.join(work, "headers.ndjson"))]
KEY = "_baseline"


def lines(h, pre=KEY):
    for k, v in sorted(h.items()):
        if isinstance(v, dict):
            yield from lines(v, f"{pre}.{k}")
        else:
            yield f"{pre}.{k}={v}"


fresh = [r for r in recs if r.get("_context") is not True]
stamped = [r for r in fresh if KEY in r]
heads = {((r.get("_norm") or {}).get("org"), (r.get("_norm") or {}).get("actor"), json.dumps(r[KEY], sort_keys=True)) for r in stamped}
sizes = sorted(sum(len(l) + 1 for l in lines(json.loads(h))) for *_, h in heads)
keys = collections.Counter(l.split("=", 1)[0] for *_, h in heads for l in lines(json.loads(h)))
print(f"{len(stamped)} of {len(fresh)} judged records carry {KEY}; {len(heads)} distinct headers (actor, value)")
if sizes:
    print(f"logfmt bytes per header: median {sizes[len(sizes) // 2]}, p90 {sizes[int(len(sizes) * 0.9)]}, max {sizes[-1]}")
    print("lines: " + ", ".join(f"{k.removeprefix(KEY + '.')} {n}" for k, n in keys.most_common()))
for cid in want:
    c = by[cid]
    times = set(c["times"])
    print(f"\n=== {cid} ({'attack' if c['flag'] else 'must not flag'})")
    for actor in c["actor_keys"]:
        own = [r for r in fresh if r.get("ts") in times and (r.get("_norm") or {}).get("actor") == actor]
        seen = collections.Counter(json.dumps(r.get(KEY), sort_keys=True) for r in own)
        for h, n in seen.items():
            print(f"-- {actor}, {n} of its records" + ("" if h != "null" else f": no {KEY}"))
            for l in lines(json.loads(h) or {}):
                print(l)
