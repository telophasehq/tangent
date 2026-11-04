#!/usr/bin/env python3
"""Score a run of packs/macos against the truth gen.py wrote.

    python3 -I evals/macos/score.py TRUTH WORK

Matches WORK/out.ndjson's records to the cases' key events by (ts,
process.pid, event): the plugin writes eslogger's `time` as `ts`, and for an
exec its `process` is the program that started (the exec's target, same
pid). A record belongs to a case when its ts is one of the case's event
times (times are unique in a stream); the rest are background.

Prints: events in and records kept; per group and overall, the cases whose
key events all survived the plugin, and the key events kept of all; each case
with a key event missing, and which. With verdicts (a `judge` field on the
records): recall (a must-flag case with a flag on one of its own records, and
on one of its key events), must-not-flag cases flagged, background records
flagged of background records kept, and records the judge could not read.
Writes WORK/score.txt and WORK/cases.tsv."""
import collections
import json
import os
import sys


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    truth, work = json.load(open(sys.argv[1])), sys.argv[2]
    recs = [json.loads(l) for l in open(os.path.join(work, "out.ndjson"))]
    by_key = {(r.get("ts"), (r.get("process") or {}).get("pid"), r.get("event")): r for r in recs}
    owner = {t: c["id"] for c in truth["cases"] for t in c["times"]}
    judged = any("judge" in r for r in recs)
    flagged = lambda r: (r.get("judge") or {}).get("flag") is True
    erred = lambda r: "error" in (r.get("judge") or {})

    own = collections.defaultdict(list)
    for r in recs:
        own[owner.get(r.get("ts"))].append(r)

    out, rows = [], []
    say = out.append
    say(f"{truth['stream']['events']} events in, {len(recs)} records kept")
    groups = collections.defaultdict(lambda: [0, 0, 0, 0])  # cases, cases whole, keys, keys kept
    missed = []
    for c in truth["cases"]:
        got = [(k, by_key.get((k["time"], k["pid"], k["event"]))) for k in c["keys"]]
        lost = [k for k, r in got if r is None]
        for g in (c["group"], "all"):
            x = groups[g]
            x[0] += 1
            x[1] += not lost
            x[2] += len(got)
            x[3] += len(got) - len(lost)
        if lost:
            missed.append((c, lost))
        mine = own.get(c["id"], [])
        row = {"id": c["id"], "group": c["group"], "flag": c["flag"], "eaa": ",".join(c["eaa"]),
               "keys": len(got), "keys_kept": len(got) - len(lost), "records": len(mine)}
        if judged:
            row.update(flagged=sum(map(flagged, mine)), key_flagged=sum(1 for _, r in got if r and flagged(r)),
                       errors=sum(map(erred, mine)))
            row["verdict"] = ("caught" if row["flagged"] else "missed") if c["flag"] else ("flagged" if row["flagged"] else "quiet")
        row["lost"] = "; ".join(f"{k['event']} {k['path']} at {k['time']} pid {k['pid']}" for k in lost)
        rows.append(row)

    say("\nkey events through the plugin")
    say(f"  {'group':24} {'cases whole':>12} {'key events kept':>16}")
    for g in sorted(groups, key=lambda g: (g == "all", g)):
        n, whole, keys, kept = groups[g]
        say(f"  {g:24} {whole:>5} of {n:<5} {kept:>7} of {keys:<6}")
    for c, lost in missed:
        say(f"  {c['id']} ({c['group']}): {len(lost)} key events not kept")
        for k in lost:
            say(f"    {k['time']} pid {k['pid']} {k['event']} {k['path']}")

    if judged:
        attacks = [r for r in rows if r["flag"]]
        quiet = [r for r in rows if not r["flag"]]
        bg = own.get(None, [])
        say("\nverdicts")
        say(f"  recall: {sum(r['flagged'] > 0 for r in attacks)} of {len(attacks)} must-flag cases flagged on a record of "
            f"their own; {sum(r['key_flagged'] > 0 for r in attacks)} on a key event")
        say(f"  must-not-flag cases flagged: {sum(r['flagged'] > 0 for r in quiet)} of {len(quiet)}")
        say(f"  background records flagged: {sum(map(flagged, bg))} of {len(bg)}")
        say(f"  records the judge could not read: {sum(map(erred, recs))} of {len(recs)}")
        for r in rows:
            if r["verdict"] in ("missed", "flagged"):
                say(f"    {r['verdict']}: {r['id']} ({r['group']})")

    text = "\n".join(out) + "\n"
    sys.stdout.write(text)
    open(os.path.join(work, "score.txt"), "w").write(text)
    cols = list(rows[0]) if rows else []
    with open(os.path.join(work, "cases.tsv"), "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join(str(r[k]) for k in cols) + "\n")


if __name__ == "__main__":
    main()
