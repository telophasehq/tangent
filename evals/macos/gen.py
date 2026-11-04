#!/usr/bin/env python3
"""Lay the cases on a synthetic timeline of dev's work days, inside the
background, and write the stream and its truth.

    python3 -I evals/macos/gen.py [--only GROUP_OR_CASE ...] [--out DIR] [--hours N] [--seed S]

The timeline is N work hours (09:00-18:00Z, Monday to Friday, from Monday
2026-10-05; default: as many as the cases need) in 30-minute slots. Each case
(cases/<group>/<case_id>.py) gets a slot of its own, spread over the timeline
in an order the seed shuffles, and starts a few minutes into it; it must end
inside it. The background (background.py) runs through every work hour.
Every event's time is unique, and so is every pid. Writes DIR/stream.ndjson
(eslogger's lines in time order) and DIR/truth.json (per case: its id, flag,
eaa, group, title, key events as {time, pid, event, path}, the times of all
its events, and its slot). DIR defaults to evals/runs/macos. Same arguments,
same bytes.

The background is serialized as it is built, a day at a time, and kept
compressed until it is merged with the cases: a day of it is some 35,000
events and 100 MB of JSON, too many to keep as objects and, on a full disk,
too big for a temporary file beside the stream."""
import argparse
import heapq
import importlib.util
import itertools
import json
import math
import os
import random
import re
import sys
import zlib
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import background  # noqa: E402
import es  # noqa: E402

CASES = os.path.join(HERE, "cases")
START = datetime(2026, 10, 5, tzinfo=timezone.utc)  # a Monday
DAY = (9, 18)  # work hours, UTC
SLOT = 30  # minutes
FIELDS = {"id", "title", "flag", "eaa", "group", "grounding", "notes"}


def load(only):
    found, cases, groups = set(), [], set()
    for group in sorted(os.listdir(CASES)):
        d = os.path.join(CASES, group)
        if not os.path.isdir(d) or group.startswith(("_", ".")):
            continue
        groups.add(group)
        for f in sorted(os.listdir(d)):
            if not f.endswith(".py") or f.startswith("_"):
                continue
            cid = f[:-3]
            if only and group not in only and cid not in only:
                continue
            found.update({group, cid})
            spec = importlib.util.spec_from_file_location("case_" + re.sub(r"\W", "_", f"{group}_{cid}"), os.path.join(d, f))
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            check(m, group, cid, os.path.join(d, f))
            cases.append(m)
    if only and set(only) & groups - found:  # a group directory holding only _ modules
        sys.exit(f"no cases in group {', '.join(sorted(set(only) & groups - found))}: its directory has only _ modules")
    if only and set(only) - found:
        sys.exit(f"no group or case named {', '.join(sorted(set(only) - found))}")
    return cases


def check(m, group, cid, path):
    c = getattr(m, "CASE", None)
    bad = lambda why: sys.exit(f"{path}: {why}")
    if not isinstance(c, dict) or set(c) != FIELDS:
        bad(f"CASE must have exactly {sorted(FIELDS)}")
    if c["id"] != cid or c["group"] != group:
        bad("CASE id and group must be the file's name and directory")
    if not isinstance(c["flag"], bool) or not all(re.fullmatch(r"EAA-\d{3}", e) for e in c["eaa"]):
        bad("flag is a bool, eaa a list of technique ids (EAA-001)")
    g = c["grounding"]
    if not isinstance(g, dict) or set(g) != {"catalog_case", "steps", "sources"}:
        bad("grounding is {catalog_case, steps, sources}")
    if g["catalog_case"] is not None and not re.fullmatch(r"EAA-C-\d{3}", g["catalog_case"]):
        bad("grounding.catalog_case is EAA-C-nnn or null")
    if c["flag"] and not (g["sources"] and (g["catalog_case"] or g["steps"])):
        bad("an attack cites its catalog case (or the technique page's procedure, in steps) and its sources")
    if not callable(getattr(m, "build", None)):
        bad("no build(mac)")


ENCODE = json.JSONEncoder(sort_keys=True, separators=(",", ":")).encode


def lines(rows):
    """mac.log rows as (time, event type, JSON line), in time order; the line's
    sequence numbers are still 0, numbered as it is written (numbered())."""
    return sorted((e["time"], e["event_type"], ENCODE(e)) for e, _, _ in rows)


def packed(rows):
    """Lines from lines() as one zlib blob, about a twelfth of their size."""
    z, out, buf = zlib.compressobj(1), [], []
    for t, kind, text in rows:
        buf.append(f"{t}\t{kind}\t{text}\n")
        if len(buf) == 2000:
            out.append(z.compress("".join(buf).encode()))
            buf = []
    out += [z.compress("".join(buf).encode()), z.flush()]
    return b"".join(out)


def unpacked(blob):
    """packed()'s lines back, a megabyte of the blob at a time (the JSON is ASCII)."""
    d, rest = zlib.decompressobj(), ""
    for i in range(0, len(blob) + 1, 1 << 20):
        chunk = d.decompress(blob[i:i + (1 << 20)]) if i < len(blob) else d.flush()
        lines = (rest + chunk.decode()).split("\n")
        rest = lines.pop()
        for line in lines:
            t, kind, text = line.split("\t", 2)
            yield t, int(kind), text


def numbered(rows):
    """es.number() on lines: seq_num per event type, and global_seq_num, in the
    order given. Only the top level holds these keys, and a string holding the
    text would have its quotes escaped."""
    per = {}
    for i, (_, kind, text) in enumerate(rows, 1):
        per[kind] = per.get(kind, 0) + 1
        yield (text.replace('"global_seq_num":0,', f'"global_seq_num":{i},', 1)
               .replace(',"seq_num":0,', f',"seq_num":{per[kind]},', 1))


def workdays(start):
    d = start
    while True:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--only", nargs="+", metavar="GROUP_OR_CASE")
    ap.add_argument("--out", default=os.path.normpath(os.path.join(HERE, "..", "runs", "macos")))
    ap.add_argument("--hours", type=int)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()

    cases = load(a.only)
    per_day = DAY[1] - DAY[0]
    hours = a.hours or max(1, math.ceil(len(cases) * SLOT / 60))
    slots = hours * 60 // SLOT
    if len(cases) > slots:
        sys.exit(f"{len(cases)} cases need {len(cases)} slots; {hours} hours have {slots}")
    days = [d for _, d in zip(range(math.ceil(hours / per_day)), workdays(START))]

    def when(minute):
        """The minute-th minute of work time."""
        day, m = divmod(minute, per_day * 60)
        return days[day] + timedelta(hours=DAY[0], minutes=m)

    mac = es.Mac(seed=a.seed, boot=(days[0] - timedelta(hours=14, minutes=47, seconds=51)).isoformat())
    spool, n_background = [], 0  # per day, its background lines in time order, packed()
    for i, d in enumerate(days):
        mac.reset_files()
        mac.owner, mac.rng = None, random.Random(f"{a.seed}:background:{i}")
        runs = []

        def flush():
            """background.build calls this when it has finished a part of the day."""
            runs.append(lines(mac.log))
            mac.log.clear()
            mac._index.clear()

        background.build(mac, d + timedelta(hours=DAY[0]), min(per_day, hours - i * per_day), flush)
        flush()
        n_background += sum(map(len, runs))
        spool.append(packed(heapq.merge(*runs)))
        del runs

    order = sorted(cases, key=lambda m: (m.CASE["group"], m.CASE["id"]))
    random.Random(f"{a.seed}:layout").shuffle(order)
    truth = []
    for i, m in enumerate(order):
        c = m.CASE
        slot = i * slots // len(order)
        lo, hi = when(slot * SLOT), when(slot * SLOT) + timedelta(minutes=SLOT)
        mac.reset_files()
        mac.owner, mac.rng = c["id"], random.Random(f"{a.seed}:{c['id']}")
        mac.at(lo + timedelta(seconds=mac.rng.randint(60, 480)))
        m.build(mac)
        rows = [r for r in mac.log if r[1] == c["id"]]
        if not rows:
            sys.exit(f"{c['id']}: no events")
        times = sorted(r[0]["time"] for r in rows)
        if times[-1] >= es.iso(es.ns(hi)):
            sys.exit(f"{c['id']}: its events run to {times[-1]}, past its slot's end {es.iso(es.ns(hi))}")
        keys = sorted(({"time": e["time"], "pid": e["process"]["audit_token"]["pid"] if next(iter(e["event"])) != "exec"
                        else e["event"]["exec"]["target"]["audit_token"]["pid"], "event": next(iter(e["event"])),
                        "path": es.key_path(e)} for e, _, k in rows if k), key=lambda k: k["time"])
        if not keys:
            sys.exit(f"{c['id']}: no key events (mac.key or key=True)")
        truth.append({"id": c["id"], "group": c["group"], "flag": c["flag"], "eaa": c["eaa"], "title": c["title"],
                      "slot": {"start": es.iso(es.ns(lo)), "end": es.iso(es.ns(hi))}, "keys": keys, "times": times})

    case_lines = lines(mac.log)
    n_events = n_background + len(case_lines)
    os.makedirs(a.out, exist_ok=True)
    background_lines = itertools.chain.from_iterable(map(unpacked, spool))  # a day at a time: the days do not overlap
    with open(os.path.join(a.out, "stream.ndjson"), "w") as f:
        for text in numbered(heapq.merge(background_lines, case_lines)):
            f.write(text + "\n")
    truth.sort(key=lambda c: (c["group"], c["id"]))
    meta = {"seed": a.seed, "hours": hours, "slot_minutes": SLOT, "days": [d.date().isoformat() for d in days],
            "events": n_events, "background_events": n_background}
    with open(os.path.join(a.out, "truth.json"), "w") as f:
        json.dump({"stream": meta, "cases": truth}, f, indent=1, sort_keys=True)
        f.write("\n")
    print(f"{a.out}: {n_events} events ({n_background} background), {len(truth)} cases, "
          f"{hours} work hours from {days[0].date()}", file=sys.stderr)


if __name__ == "__main__":
    main()
