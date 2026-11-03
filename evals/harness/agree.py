"""How repeatable the judge is: two runs of the same build, record by record (matched on the record as judged),
and case by case.

    python3 -I evals/harness/agree.py TRUTH.json RUN_A RUN_B"""
import collections
import hashlib
import json
import os
import sys

truth_path, a, b = sys.argv[1:4]


def flags(run):
    out = collections.defaultdict(list)
    for line in open(os.path.join(run, "verdicts.ndjson")):
        r = json.loads(line)
        j = r.pop("judge", None) or {}
        if r.get("_context") or "flag" not in j:
            continue
        out[hashlib.sha1(json.dumps(r, sort_keys=True).encode()).hexdigest()].append(bool(j["flag"]))
    return out


fa, fb = flags(a), flags(b)
both = set(fa) & set(fb)
same = flipped_on = flipped_off = 0
for k in both:
    x, y = sorted(fa[k]), sorted(fb[k])
    n = min(len(x), len(y))
    for p, q in zip(x[:n], y[:n]):
        same += p == q
        flipped_on += (not p) and q
        flipped_off += p and not q
total = same + flipped_on + flipped_off
print(f"{a} vs {b}: {len(both)} records judged in both; verdict agrees on {same}/{total} ({same / total:.2%}); "
      f"flagged only in {a}: {flipped_off}, only in {b}: {flipped_on}")


def caught(run):
    """Cases with a flag on one of their own records."""
    rows = [json.loads(l) for l in open(os.path.join(run, "verdicts.ndjson"))]
    hit = set()
    for c in json.load(open(truth_path)):
        if not c["flag"]:
            continue
        times = set(c["times"])
        actors = set(c.get("actor_keys") or ([c["actor_key"]] if c.get("actor_key") else []))
        if any(r.get("ts") in times and (r.get("judge") or {}).get("flag") is True and not r.get("_context")
               and (not actors or (r.get("_norm") or {}).get("actor") in actors) for r in rows):
            hit.add(c["id"])
    return hit


ca, cb = caught(a), caught(b)
print(f"attacks caught: {len(ca)} and {len(cb)}; in both {len(ca & cb)}; only in {a}: {sorted(ca - cb)}; only in {b}: {sorted(cb - ca)}")
