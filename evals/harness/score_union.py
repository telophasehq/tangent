#!/usr/bin/env python3
"""The judge and a deterministic tripwire scored together on one replay of the
corpus: per case, caught by the judge, by the tripwire, by either (the union);
the same for the must-not-flag cases; ordinary records and incidents flagged
inside the judged windows; and the tripwire alone over the whole stream.

    python3 -I evals/harness/score_union.py TRUTH WORK WINDOWS [TRIPWIRE.ndjson]

WORK is a replay.py / shard.py run (WORK/verdicts.ndjson). TRIPWIRE is what the
ingest-side plugin wrote (default WORK/tripwire.ndjson; after shard.py, the
shards' s*/tripwire.ndjson concatenated: each shard re-ingests the stream from
its start, so the same record appears in several, and is counted once here).
A record is the same record in both files when it is equal without the keys
the judge side adds (judge, _baseline, _hist, _context) and the tripwire's own
key (TRIP_KEY, default `tripwire`). Catch rules are score_v2.py's: `near` (a
flag on the case's actor, or org when it has no actor, between live_from and
live_to) and `own` (on one of the case's own records)."""
import bisect
import collections
import glob
import hashlib
import json
import os
import sys
from datetime import datetime

TRIP_KEY = os.environ.get("TRIP_KEY", "tripwire")
STRIP = {"judge", "_baseline", "_hist", "_context", TRIP_KEY}
truth_path, work, windows_path = sys.argv[1:4]
trip_path = sys.argv[4] if len(sys.argv) > 4 else os.path.join(work, "tripwire.ndjson")
truth = json.load(open(truth_path))
ms = lambda s: int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def key(r):
    return hashlib.sha1(json.dumps({k: v for k, v in r.items() if k not in STRIP}, sort_keys=True).encode()).hexdigest()


# the judged windows, merged as replay.py merges them: a record is judged when a <= ts < b
merged = []
for a, b in sorted([ms(a), ms(b)] for a, b in json.load(open(windows_path))):
    if merged and a <= merged[-1][1]:
        merged[-1][1] = max(merged[-1][1], b)
    else:
        merged.append([a, b])
starts = [a for a, _ in merged]


def in_window(ts):
    t = ms(ts)
    k = bisect.bisect_right(starts, t) - 1
    return k >= 0 and t < merged[k][1]


# the judge: the last verdict per record (a window judged again after a stop)
judged = {}
for line in open(os.path.join(work, "verdicts.ndjson")):
    r = json.loads(line)
    if r.get("_context") is True:
        continue
    judged[key(r)] = r
jflag = {k for k, r in judged.items() if (r.get("judge") or {}).get("flag") is True}
errors = sum(1 for r in judged.values() if "flag" not in (r.get("judge") or {}))

# the tripwire, once per record
if not os.path.exists(trip_path) and glob.glob(os.path.join(work, "s*", "tripwire.ndjson")):
    sys.exit(f"{trip_path} missing: cat {work}/s*/tripwire.ndjson > {trip_path}")
if not os.path.exists(trip_path):
    sys.exit(f"{trip_path}: no tripwire output. score_union.py scores a run whose ingest has a tripwire plugin "
             "(maincfg.py --tripwire NAME=PATH, compiled with `$TANGENT plugin compile --config WORK/ingest.yaml`), "
             "or takes TRIPWIRE.ndjson from such a run; score_v2.py scores the judge alone")
tripped = {}
for line in open(trip_path):
    r = json.loads(line)
    tripped.setdefault(key(r), r)
twin = {k for k, r in tripped.items() if in_window(r.get("ts", "1970-01-01T00:00:00Z"))}
unjudged = sorted(twin - set(judged))  # tripped in a window, but no verdict to pair it with

case_times = {t for c in truth for t in c["times"]}


def keys(c):
    actors = c.get("actor_keys") or ([c["actor_key"]] if c.get("actor_key") else [])
    orgs = c.get("org_keys") or ([c["org_key"]] if c.get("org_key") else [])
    return actors, orgs


def near(r, c):
    n = r.get("_norm") or {}
    t = r.get("ts", "")
    if not (c["live_from"] <= t <= c["live_to"]):
        return False
    actors, orgs = keys(c)
    return n.get("actor") in actors if n.get("actor") else n.get("org") in orgs


def own(r, c):
    return r.get("ts") in c["times"] and near(r, dict(c, live_from=min(c["times"]), live_to=max(c["times"])))


# records of each side that flagged, as records
J = [judged[k] for k in jflag]
T = [tripped[k] for k in twin]  # in-window only: what the judge also had the chance to see
T_all = list(tripped.values())


def caught(rs, c, rule=near):
    return any(rule(r, c) for r in rs)


pos = [c for c in truth if c["flag"]]
neg = [c for c in truth if not c["flag"]]


def suite(c):
    s = c["id"].split("/")[0]
    return {"stratus": "stratus " + c["id"].split("/")[1], "control": "control " + c["id"].split("/")[1]}.get(
        s, "ours" if c["flag"] else "ours (negatives)")


res = {c["id"]: (caught(J, c), caught(T, c), caught(T_all, c)) for c in truth}
own_res = {c["id"]: (caught(J, c, own), caught(T, c, own)) for c in pos}
print(f"judge: {len(judged)} records judged, {errors} not judged, {len(jflag)} flagged")
print(f"tripwire: {len(tripped)} distinct records tripped over the stream, {len(twin)} inside judged windows"
      f" ({len(unjudged)} of those without a verdict to pair with)")
for name, cs in (("attacks caught", pos), ("must-not-flag flagged", neg)):
    j = sum(res[c["id"]][0] for c in cs)
    t = sum(res[c["id"]][1] for c in cs)
    u = sum(res[c["id"]][0] or res[c["id"]][1] for c in cs)
    print(f"{name}: judge {j}/{len(cs)}, tripwire {t}/{len(cs)}, either {u}/{len(cs)}")
print(f"attacks caught on one of their own records: judge {sum(v[0] for v in own_res.values())}, "
      f"tripwire {sum(v[1] for v in own_res.values())}, either {sum(v[0] or v[1] for v in own_res.values())} of {len(pos)}")
print("\nby suite (judge / tripwire / either):")
by = collections.OrderedDict()
for c in truth:
    by.setdefault(suite(c), []).append(c)
for s, cs in by.items():
    j, t = (sum(res[c["id"]][i] for c in cs) for i in (0, 1))
    u = sum(res[c["id"]][0] or res[c["id"]][1] for c in cs)
    print(f"   {s}: {j} / {t} / {u} of {len(cs)} {'caught' if cs[0]['flag'] else 'flagged'}")
only_t = [c["id"] for c in pos if res[c["id"]][1] and not res[c["id"]][0]]
missed = [c["id"] for c in pos if not (res[c["id"]][0] or res[c["id"]][1])]
print(f"   caught by the tripwire alone: {only_t or 'none'}")
print(f"   missed by both: {missed or 'none'}")

# ordinary records inside the windows, and incidents (org, actor; a new one after an hour without a flag)
ordinary = {k for k, r in judged.items() if r.get("ts") not in case_times}
t_ord = {k for k in twin if tripped[k].get("ts") not in case_times}


def incidents(rs):
    n, last = 0, {}
    for r in sorted(rs, key=lambda r: r["ts"]):
        k, t = ((r.get("_norm") or {}).get("org"), (r.get("_norm") or {}).get("actor")), ms(r["ts"])
        if k not in last or t - last[k] > 3_600_000:
            n += 1
        last[k] = t
    return n


rec = lambda ks: [judged.get(k) or tripped[k] for k in ks]
hours = sum(b - a for a, b in merged) / 3.6e6
for name, ks in (("judge", jflag & ordinary), ("tripwire", t_ord), ("either", (jflag & ordinary) | t_ord)):
    i = incidents(rec(ks))
    print(f"ordinary, inside the windows ({hours:.0f} h): {name} {len(ks)} records, {i} incidents ({i / hours * 24:.1f} per 24 judged hours)")

# the tripwire alone, over every record it saw
t_all_ord = [r for r in T_all if r.get("ts") not in case_times]
if T_all:
    span = (max(ms(r["ts"]) for r in T_all) - min(ms(r["ts"]) for r in T_all)) / 8.64e7
    i = incidents(t_all_ord)
    print(f"tripwire over the stream it saw: {len(t_all_ord)} ordinary records tripped, {i} incidents; "
          f"about {i / max(span, 1):.2f} a day over {span:.0f} days (synthetic ordinary traffic)")
