"""Score a replay of the corpus through the pack (replay.py): recall over the
flag cases, false positives over the negatives, the flag rate over ordinary
records, and what the requests held.
    python3 -I evals/harness/score_v2.py TRUTH WORK"""
import collections
import json
import os
import re
import sys

import yaml

truth = json.load(open(sys.argv[1]))
work = sys.argv[2]
out = [json.loads(l) for l in open(os.path.join(work, "verdicts.ndjson"))]
# a window judged again after a stop: the last verdict per record
last = {}
for r in out:
    last[(r.get("ts"), r.get("eventID") or r.get("uuid") or json.dumps({k: v for k, v in r.items() if k != "judge"}, sort_keys=True))] = r
out = list(last.values())
case_times = {t for c in truth for t in c["times"]}


def flagged(r):
    return (r.get("judge") or {}).get("flag") is True


def keys(c):
    actors = c.get("actor_keys") or ([c["actor_key"]] if c.get("actor_key") else [])
    orgs = c.get("org_keys") or ([c["org_key"]] if c.get("org_key") else [])
    return actors, orgs


def near(r, c):
    """The fast eval's rule: the case's actor (or org), in the two hours before its last event."""
    n = r.get("_norm") or {}
    t = r.get("ts", "")
    if not (c["live_from"] <= t <= c["live_to"]):
        return False
    actors, orgs = keys(c)
    return n["actor"] in actors if n.get("actor") else n.get("org") in orgs


def own(r, c):
    """One of the case's own records."""
    return r.get("ts") in c["times"] and near(r, dict(c, live_from=min(c["times"]), live_to=max(c["times"])))


errors = sum(1 for r in out if "error" in (r.get("judge") or {}))
pos = [c for c in truth if c["flag"]]
neg = [c for c in truth if not c["flag"]]
flags = [r for r in out if flagged(r)]
caught = [c for c in pos if any(near(r, c) for r in flags)]
caught_own = [c for c in pos if any(own(r, c) for r in flags)]
fps = [c for c in neg if any(near(r, c) for r in flags)]
ordinary = [r for r in out if r.get("ts") not in case_times]
ord_flags = [r for r in ordinary if flagged(r)]
print(f"judged {len(out)} records, {errors} not judged, {len(flags)} flagged")
print(f"recall {len(caught)}/{len(pos)} (a flag on the case's actor in its last two hours); "
      f"{len(caught_own)}/{len(pos)} on one of the case's own records")
print(f"negatives flagged {len(fps)}/{len(neg)}")
print(f"ordinary: {len(ord_flags)} of {len(ordinary)} flagged ({len(ord_flags) / max(len(ordinary), 1):.1%})")

suites = collections.OrderedDict()
for c in truth:
    s = c["id"].split("/")[0]
    s = {"stratus": "stratus " + c["id"].split("/")[1], "control": "control " + c["id"].split("/")[1]}.get(s, "ours" if c["flag"] else "ours (negatives)")
    suites.setdefault(s, []).append(c)
print("\nby suite:")
for s, cs in suites.items():
    hit = [c for c in cs if c in (caught if c["flag"] else fps)]
    print(f"   {s}: {len(hit)}/{len(cs)} {'caught' if cs[0]['flag'] else 'flagged'}")
for s, cs in suites.items():
    if cs[0]["flag"]:
        missed = [c["id"] for c in cs if c not in caught]
        if missed:
            print(f"   missed, {s}: " + ", ".join(m.split("/")[-1] for m in missed))
    else:
        hit = [c["id"] for c in cs if c in fps]
        if hit:
            print(f"   flagged, {s}: " + ", ".join(m.split("/")[-1] for m in hit))

pairs = [(c, next(t for t in neg if t["id"] == c["twin"])) for c in pos if c.get("twin")]
if pairs:
    print("\nattack / its twin (v3: the same person, hour and actions, from where they always are):")
    for c, t in pairs:
        print(f"   {c['id']}: {'caught' if c in caught else 'MISSED'}; twin {'flagged' if t in fps else 'quiet'}")

g = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "stratus", "grounding.yaml")))
print("\nstratus by what decides it:")
for d in ("event", "actor", "volume", "data"):
    row = []
    for v in ("away", "usual"):
        cs = [c for c in pos if c["id"].startswith(f"stratus/{v}/") and g[c["id"].split("/", 2)[2]]["decides"] == d]
        ctl = [c for c in neg if c["id"].startswith(f"control/{v}/") and g[c["id"].split("/", 2)[2]]["decides"] == d]
        if cs:
            row.append(f"{v} {sum(c in caught for c in cs)}/{len(cs)} (controls flagged {sum(c in fps for c in ctl)}/{len(ctl)})")
    if row:
        print(f"   {d}: " + ", ".join(row))

# What the requests held: every record the model read, _context ones too.
seen = os.path.join(work, "seen.ndjson")
if os.path.exists(seen):
    n = ctx = size = 0
    for l in open(seen):
        r = json.loads(l)
        r.pop("judge", None)
        n += 1
        ctx += r.get("_context") is True
        size += len(json.dumps(r, separators=(",", ":"))) + 8
    reqs = sum(1 for l in open(os.path.join(work, "judge.log")) if re.search(r"inference.*records=\d+ ms=", re.sub(r"\x1b\[[0-9;]*m", "", l)))
    print(f"\nrequests {reqs}; records read {n} ({ctx} as context); {size / 1e6:.1f} MB of records")
