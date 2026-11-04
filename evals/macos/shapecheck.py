#!/usr/bin/env python3
"""Check a synthetic eslogger stream against the shapes of a real capture.

    python3 -I evals/macos/shapecheck.py STREAM SHAPE.json...

A SHAPE file holds, per event type, every field path of a capture and the
JSON types seen at it, with counts and no values ({"events": {type: n},
"fields": {type: {path: {jtype: count}}}}; list elements as `path[]`). Each
event in STREAM must use only the paths and types the capture has for its
event type, and must carry every path the capture always has (always: as
often as the object that holds it). Event types no SHAPE file has are
counted, not checked. Prints what differs; exits 1 if anything does. The
shape files come from a capture of a real Mac and stay out of the repository.
STREAM may be gzipped (.gz)."""
import collections
import gzip
import json
import sys


def jtype(v):
    return {dict: "object", list: "array", str: "string", bool: "bool", int: "int", float: "float",
            type(None): "null"}[type(v)]


def walk(v, path, out):
    out.add((path or "/", jtype(v)))
    if isinstance(v, dict):
        for k, x in v.items():
            walk(x, f"{path}/{k}", out)
    elif isinstance(v, list):
        for x in v:
            walk(x, f"{path}[]", out)


def parent(p):
    return None if p == "/" else (p[:p.rfind("/")] or "/")


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    stream, shapes = sys.argv[1], sys.argv[2:]
    real = {}
    for f in shapes:
        real.update(json.load(open(f))["fields"])
    allowed = {k: {(p, t) for p, ts in v.items() for t in ts} for k, v in real.items()}
    required = {}
    for k, v in real.items():
        n = {p: sum(ts.values()) for p, ts in v.items()}
        required[k] = sorted(p for p in v if not p.endswith("[]") and parent(p) is not None
                             and n[p] == v.get(parent(p), {}).get("object", -1))
        required[k].append("/")

    count = collections.Counter()
    unchecked = collections.Counter()
    extra = collections.defaultdict(lambda: [0, None])    # (kind, path, type) -> [count, first line]
    missing = collections.defaultdict(lambda: [0, None])  # (kind, path) -> [count, first line]
    for i, line in enumerate((gzip.open if stream.endswith(".gz") else open)(stream, "rt"), 1):
        ev = json.loads(line)
        kind = next(iter(ev["event"]))
        if kind not in real:
            unchecked[kind] += 1
            continue
        count[kind] += 1
        got = set()
        walk(ev, "", got)
        for p, t in got - allowed[kind]:
            e = extra[(kind, p, t)]
            e[0] += 1
            e[1] = e[1] or i
        paths = {p for p, _ in got}
        objects = {p for p, t in got if t == "object"}
        for p in required[kind]:
            if p not in paths and (p == "/" or parent(p) in objects):
                m = missing[(kind, p)]
                m[0] += 1
                m[1] = m[1] or i

    for kind in sorted(count):
        bad = sum(1 for k in extra if k[0] == kind) + sum(1 for k in missing if k[0] == kind)
        print(f"{kind:10} {count[kind]:7} events  {'ok' if not bad else f'{bad} differences'}")
    for kind, n in sorted(unchecked.items()):
        print(f"{kind:10} {n:7} events  not checked: no capture has this event type")
    for (kind, p, t), (n, first) in sorted(extra.items()):
        print(f"  {kind}: {p} as {t}: not in the capture ({n} events, first at line {first})")
    for (kind, p), (n, first) in sorted(missing.items()):
        print(f"  {kind}: {p}: the capture always has it ({n} events lack it, first at line {first})")
    sys.exit(1 if extra or missing else 0)


if __name__ == "__main__":
    main()
