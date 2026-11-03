"""Does any case's history hold another case? For every case, the latest
`--keep` records before it of each actor and org it touches (as the judge
reads an actor's latest forty), checked for records that belong to another
case, in the stream stream.py wrote (evals/runs/stream/).

    python3 -I evals/harness/isolation.py [--v3] [--keep 40]

Prints each case whose history holds another case's records, and which."""
import argparse
import bisect
import collections
import importlib.util
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
CASES = HERE.parent  # evals/: stream.py, and runs/stream/


def load_stream_module():
    spec = importlib.util.spec_from_file_location("stream", CASES / "stream.py")
    m = importlib.util.module_from_spec(spec)
    sys.argv, argv = ["stream.py"], sys.argv
    spec.loader.exec_module(m)
    sys.argv = argv
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", type=int, default=40)
    ap.add_argument("--v3", action="store_true", help="stream-v3.ndjson, with the history cases")
    args = ap.parse_args()
    s = load_stream_module()
    placed, truth = s.placed_cases(args.v3)
    # Which case each placed event belongs to: they come out in truth's order.
    owner, at = {}, 0
    for c in truth:
        for e in placed[at:at + c["events"]]:
            owner[json.dumps(e, sort_keys=True)] = c["id"]
        at += c["events"]
    rows = [json.loads(l) for l in open(CASES / "runs" / "stream" / ("stream-v3.ndjson" if args.v3 else "stream.ndjson"))]
    by_scope = collections.defaultdict(list)  # (scope, key) -> [(ts, case id or None)]
    for e in rows:
        actor, org = s.norm_keys(e)
        case = owner.get(json.dumps(e, sort_keys=True))
        if actor:
            by_scope[("actor", actor)].append((e["ts"], case))
        if org:
            by_scope[("org", org)].append((e["ts"], case))
    for v in by_scope.values():
        v.sort(key=lambda x: x[0])
    bad = collections.Counter()
    for c in truth:
        start = min(c["times"])
        seen = collections.Counter()
        for scope, keys in (("actor", c.get("actor_keys") or []), ("org", c.get("org_keys") or [])):
            for k in keys:
                lst = by_scope.get((scope, k), [])
                i = bisect.bisect_left(lst, (start,))
                for ts, other in lst[max(0, i - args.keep):i]:
                    if other and other != c["id"]:
                        seen[(scope, k, other)] += 1
        if seen:
            kinds = c["id"].split("/")[0] if c["id"].split("/")[0] in ("stratus", "control") else "ours"
            bad[kinds] += 1
            print(c["id"], "<-", "; ".join(f"{sc} {k}: {o}" for (sc, k, o) in sorted(seen))[:300])
    totals = collections.Counter(c["id"].split("/")[0] if c["id"].split("/")[0] in ("stratus", "control") else "ours" for c in truth)
    print(f"cases whose history holds another case (latest {args.keep} per scope): "
          + ", ".join(f"{k} {bad[k]}/{totals[k]}" for k in ("ours", "stratus", "control")))


if __name__ == "__main__":
    main()
