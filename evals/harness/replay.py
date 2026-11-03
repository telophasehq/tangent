#!/usr/bin/env python3
"""Replay a time-ordered NDJSON stream through a pack's two processes, ingest
and judge, as if it arrived live, judging only inside the given windows.

    TANGENT=/path/to/tangent python3 -I evals/harness/replay.py WORK STREAM WINDOWS

WORK holds ingest.yaml (a tcp source, the sqlite sink) and judge.yaml (the
sqlite source over the sink's file), as maincfg.py writes them; WINDOWS is a
JSON file, a list of [from, to] ISO times (windows.py). STREAM must be in
`ts` order. TANGENT, the binary, is required; the plugins in the configs'
plugins_path must have been compiled by that same binary.

Records before a window are history: ingested with the judge stopped, then
marked emitted as if judged already, so the judge reads them only as context.
Inside a window, records go in STEP seconds of record time; after each step
the driver waits until the judge has nothing due by the sqlite source's rule
(a value with batch_size pending, or pending timeout_secs older than the
newest row). At the window's end a marker row timeout_secs later lets what is
left time out, as the next hour of traffic would; then the judge stops.

TRUTH (a v3 truth file, whose cases list their rows) keeps the cases apart:
before each window, the rows of every case already over leave logs.sqlite, so
no later case reads them as _context. With history.yaml in WORK (maincfg.py
isolated) the judge reads state.window.sqlite: state.ordinary.sqlite, the stream's
ordinary records up to the window's end (as far as ingest runs ahead of the
judge, no further: a session row keeps only its last day), plus every row of
the cases this window touches (their setup too), folded by history.yaml before
the judge starts. JUDGE names
the judge's config: judge-sim.yaml stamps and writes the records with no model."""
import atexit
import bisect
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone

import yaml

STEP = int(os.environ.get("STEP", "300"))
SIMULATE = os.environ.get("SIMULATE") == "1"  # no model: the source's groups formed and acked here, and counted
FROM_WINDOW = int(os.environ.get("FROM_WINDOW", "1"))  # resume: earlier windows are history
STALL = int(os.environ.get("STALL", "900"))
T = os.environ.get("TANGENT") or sys.exit("TANGENT: set it to the tangent binary that compiled the plugins")
JUDGE = os.environ.get("JUDGE", "judge.yaml")
TRUTH = os.environ.get("TRUTH")
LEARN_CHUNK = 20000  # records a history.yaml run takes: a --once run drops what it has not done in two minutes


def ms(s):
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, file=sys.stderr, flush=True)


work, stream, windows = sys.argv[1:4]
ingest_cfg = yaml.safe_load(open(os.path.join(work, "ingest.yaml")))
judge_cfg = yaml.safe_load(open(os.path.join(work, JUDGE)))
port = next(int(s["bind_address"].rsplit(":", 1)[1]) for s in ingest_cfg["sources"].values() if s["type"] == "tcp")
sink = next(s for s in ingest_cfg["sinks"].values() if s["type"] == "sqlite")
src = next(s for s in judge_cfg["sources"].values() if s["type"] == "sqlite")
table, batch, timeout = sink["table"], src["batch_size"], src["timeout_secs"] * 1000
cols = ", ".join('"' + f + '"' for f in sink["partitions"][src["partition"]])
by_key = " AND ".join(f"{c} IS ?" for c in cols.split(", "))  # a v3 truth row's org, actor
wins = sorted([ms(a), ms(b)] for a, b in json.load(open(windows)))
merged = []
for a, b in wins:
    if merged and a <= merged[-1][1]:
        merged[-1][1] = max(merged[-1][1], b)
    else:
        merged.append([a, b])

sim = {"groups": 0, "requests": 0, "fresh": 0, "context": 0, "bytes": 0, "windows": []}
BATCH_BYTES = ((judge_cfg.get("inference") or {}).get("judge") or {}).get("batch_bytes", 48000)
for f in ("logs.sqlite", "state.sqlite", "state.window.sqlite", "state.ordinary.sqlite"):
    for x in ("", "-wal", "-shm"):
        if os.path.exists(os.path.join(work, f + x)):
            os.remove(os.path.join(work, f + x))
env = dict(os.environ, RUST_LOG=os.environ.get("RUST_LOG", "info,tangent_runtime::inference=debug"))
# A Bedrock bearer token in the environment is used instead of judge.yaml's aws
# profile, so it goes when judge.yaml names one (maincfg.py writes AWS_PROFILE's);
# with no profile named, the token may be the only credential, and stays.
if ((((judge_cfg.get("inference") or {}).get("judge") or {}).get("aws") or {}).get("profile")):
    env.pop("AWS_BEARER_TOKEN_BEDROCK", None)
judge = None
cases = json.load(open(TRUTH)) if TRUTH else []
if any("rows" not in c for c in cases):
    sys.exit(f"{TRUTH}: its cases list no rows (stream.py v3 writes them)")
HISTORY = os.path.exists(os.path.join(work, "history.yaml")) and not SIMULATE
if HISTORY and not cases:
    sys.exit("history.yaml needs TRUTH: the ordinary stream is the stream without the cases' rows")
case_lines = {}  # line in the stream -> the line, for every case's rows
ordinary = os.path.join(work, "state.ordinary.sqlite")


def learn(lines, state):
    """`lines` folded into `state` by history.yaml (into state.window.sqlite,
    which is then moved to `state`, unless that is where they go)."""
    window, logf = os.path.join(work, "state.window.sqlite"), os.path.join(work, "history.log")
    with open(logf, "ab") as out:
        for k in range(0, len(lines), LEARN_CHUNK):
            with open(os.path.join(work, "learn.ndjson"), "wb") as f:
                f.writelines(lines[k:k + LEARN_CHUNK])
            at = os.path.getsize(logf)
            if subprocess.run([T, "run", "--once", "--config", "history.yaml"], cwd=work, env=env, stdout=out,
                              stderr=subprocess.STDOUT).returncode:
                sys.exit("history.yaml failed; see history.log")
            with open(logf, "rb") as f:
                f.seek(at)
                text = f.read().decode(errors="replace")
            if "timeout exceeded" in text:
                sys.exit("history.yaml timed out and dropped records; see history.log")
            if "plugin trapped" in text or "guest error" in text:
                log(f"history.yaml: {text.count('plugin trapped') + text.count('guest error')} failed calls; see history.log")
    c = sqlite3.connect(window)  # what is still in the WAL, into the file
    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    c.close()
    if state != window:
        os.replace(window, state)


if cases:
    want = {i for c in cases for i, *_ in c["rows"]}
    rest = []
    with open(stream, "rb") as f:
        for i, line in enumerate(f):
            line = line if line.endswith(b"\n") else line + b"\n"
            if i in want:
                case_lines[i] = line
            elif HISTORY:
                rest.append(line)
rest_at = [ms(json.loads(l)["ts"]) for l in rest] if cases else []  # in ts order, as the stream
folded = 0  # ordinary records in state.ordinary.sqlite


def window_from(state):
    """state.window.sqlite as a copy of `state`, or none."""
    for x in ("", "-wal", "-shm"):
        if os.path.exists(os.path.join(work, "state.window.sqlite" + x)):
            os.remove(os.path.join(work, "state.window.sqlite" + x))
    if os.path.exists(state):
        shutil.copyfile(state, os.path.join(work, "state.window.sqlite"))


gone = set()  # cases whose rows have left logs.sqlite
ingest = subprocess.Popen([T, "run", "--config", "ingest.yaml"], cwd=work, env=env,
                          stdout=open(os.path.join(work, "ingest.log"), "w"), stderr=subprocess.STDOUT)


@atexit.register
def stop_all():
    """On any exit, nothing left running to hold the port."""
    for p in (judge, ingest):
        if p is not None and p.poll() is None:
            p.send_signal(signal.SIGINT)
            p.wait()


for _ in range(240):
    try:
        sock = socket.create_connection(("127.0.0.1", port))
        break
    except OSError:
        if ingest.poll() is not None:
            sys.exit("ingest exited; see ingest.log")
        time.sleep(0.25)
db = sqlite3.connect(os.path.join(work, sink["path"]), timeout=60, isolation_level=None)
judge_log = open(os.path.join(work, "judge.log"), "a")
sent = 0
short = 0  # records a plugin dropped: never stored


def stored():
    global short
    last, still = -1, 0
    while True:
        try:
            n = db.execute(f'SELECT count(*) FROM "{table}" WHERE emitted IS NOT 0').fetchone()[0]
        except sqlite3.OperationalError:  # the sink has not made the table yet
            n = 0
        if n + short >= sent:
            return
        if ingest.poll() is not None:
            sys.exit("ingest exited; see ingest.log")
        still = still + 1 if n == last else 0
        last = n
        if still >= 500:  # 10 s with nothing new: the rest were dropped
            log(f"{sent - n - short} records not stored")
            short = sent - n
            return
        time.sleep(0.02)


def due():
    newest = db.execute(f'SELECT max(at) FROM "{table}"').fetchone()[0]
    return db.execute(
        f'SELECT count(*), coalesce(sum(n), 0) FROM (SELECT count(*) n, min(at) f FROM "{table}" '
        f"WHERE emitted IS NULL GROUP BY {cols}) WHERE n >= ? OR f <= ?",
        (batch, newest - timeout)).fetchone()


def simulate():
    """The sqlite source's groups, round after round, acked at once; then
    inference's split at batch_bytes. What the requests would hold."""
    key = lambda r: tuple(r[2:])
    n = len(sink["partitions"][src["partition"]])
    by = " AND ".join(f'{c} IS ?' for c in cols.split(", "))
    while True:
        newest = db.execute(f'SELECT max(at) FROM "{table}"').fetchone()[0]
        old = newest - timeout
        parts = {}
        for r in db.execute(f'SELECT id, at, {cols} FROM "{table}" WHERE emitted IS NULL ORDER BY at, id'):
            parts.setdefault(key(r), []).append(r)
        groups = []
        for k, rows in parts.items():
            context = []
            if len(rows) < batch:
                if rows[0][1] > old:
                    continue
                context = [c for (c,) in db.execute(
                    f'SELECT id FROM "{table}" WHERE {by} AND at <= ? AND emitted IS NOT NULL ORDER BY at DESC, id DESC LIMIT ?',
                    (*k, rows[0][1], batch - len(rows)))][::-1]
            groups.append((context, [r[0] for r in rows[:batch]]))
        if not groups:
            return
        for context, ids in groups:
            sizes = [l + 17 for (l,) in db.execute(f'SELECT length(record) FROM "{table}" WHERE id IN ({",".join(map(str, context))})')]
            sizes += [l for (l,) in db.execute(f'SELECT length(record) FROM "{table}" WHERE id IN ({",".join(map(str, ids))})')]
            reqs, cur = 1, 0
            for z in sizes:
                if cur and cur + z > BATCH_BYTES:
                    reqs, cur = reqs + 1, 0
                cur += z
            sim["groups"] += 1; sim["requests"] += reqs; sim["fresh"] += len(ids); sim["context"] += len(context); sim["bytes"] += sum(sizes)
            db.executemany(f'UPDATE "{table}" SET emitted = ? WHERE id = ?', [(int(time.time() * 1000), i) for i in ids])


verdict_at = os.path.getsize(os.path.join(work, "verdicts.ndjson")) if os.path.exists(os.path.join(work, "verdicts.ndjson")) else 0  # a rerun: only its own errors
recoveries = []


def errors():
    """Records the model could not judge since the last look: an expired
    credential turns every one into this, so stop before it eats a day."""
    global verdict_at
    p = os.path.join(work, "verdicts.ndjson")
    if not os.path.exists(p):
        return []
    with open(p, "rb") as f:
        f.seek(verdict_at)
        data = f.read()
    cut = data.rfind(b"\n") + 1
    verdict_at += cut
    # a reply that skipped a record is the model's miss, not a failed request: counted, not retried
    return [r for r in map(json.loads, data[:cut].splitlines())
            if "error" in (r.get("judge") or {}) and not r["judge"]["error"].startswith("no answer for record")]


PROFILE = os.environ.get("AWS_WAIT_PROFILE")  # on errors, wait for this profile's credentials, then go on


def recover(bad):
    """Stop the judge, put the records it could not judge back as pending
    (by CloudTrail's eventID), wait for credentials, start it again."""
    global judge
    stop_judge()
    time.sleep(1)
    bad += errors()
    ids = [r["eventID"] for r in bad if r.get("eventID")]
    if not PROFILE or len(ids) < len(bad):
        sys.exit(f"{len(bad)} records not judged (see judge.log; credentials?); rerun with FROM_WINDOW={window}")
    db.executemany(f"UPDATE \"{table}\" SET emitted = NULL WHERE json_extract(record, '$.eventID') = ?", [(i,) for i in ids])
    log(f"{len(bad)} records not judged ({bad[0]['judge']['error'][:160]}); put back; waiting for {PROFILE} credentials")
    while subprocess.run(["aws", "sts", "get-caller-identity", "--profile", PROFILE], capture_output=True).returncode:
        time.sleep(30)
    log("credentials back; judging again")
    judge = subprocess.Popen([T, "run", "--config", JUDGE], cwd=work, env=env, stdout=judge_log, stderr=subprocess.STDOUT)


def drain():
    if SIMULATE:
        return simulate()
    verdicts = os.path.join(work, "verdicts.ndjson")
    last, since = None, time.time()
    while True:
        d = due()
        if d[0] == 0:
            return
        if judge.poll() is not None:
            sys.exit("judge exited; see judge.log")
        bad = errors()
        if bad:
            recoveries.append(time.time())
            if len(recoveries) >= 3 and recoveries[-1] - recoveries[-3] < 600:
                stop_judge()
                sys.exit(f"{len(bad)} records not judged, a third time in ten minutes: not credentials (see judge.log)")
            recover(bad)
        size = os.path.getsize(verdicts) if os.path.exists(verdicts) else 0
        if (d, size) != last:
            last, since = (d, size), time.time()
        elif time.time() - since > STALL:
            sys.exit(f"judge stalled {STALL}s with {d[0]} values due ({d[1]} records)")
        time.sleep(0.5)


def stop_judge():
    global judge
    if judge is None:
        return
    judge.send_signal(signal.SIGINT)
    judge.wait()
    judge = None


lines = open(stream, "rb")
pending = None  # a line read past the current step
started = time.time()


def send_until(t):
    """Every record with ts before t."""
    global pending, sent
    buf = []
    while True:
        line = pending or lines.readline()
        pending = None
        if not line:
            break
        if ms(json.loads(line)["ts"]) >= t:
            pending = line
            break
        buf.append(line if line.endswith(b"\n") else line + b"\n")
        if len(buf) >= 2000:
            sock.sendall(b"".join(buf)); sent += len(buf); buf = []
    if buf:
        sock.sendall(b"".join(buf)); sent += len(buf)
    stored()


def skip_until(t):
    global pending
    while True:
        line = pending or lines.readline()
        pending = None
        if not line:
            return
        if ms(json.loads(line)["ts"]) >= t:
            pending = line
            return


window = 0
for k, (a, b) in enumerate(merged):
    window = k + 1
    hist = int(os.environ.get("HISTORY_SECS", "0")) * 1000
    if hist:
        skip_until(a - hist)
    send_until(a)
    db.execute(f'UPDATE "{table}" SET emitted = at WHERE emitted IS NULL')
    dropped = 0
    for c in cases:
        if c["id"] not in gone and ms(c["live_to"]) < a:
            gone.add(c["id"])
            for _, t, *key in c["rows"]:
                dropped += db.execute(f'DELETE FROM "{table}" WHERE at = ? AND {by_key}', (ms(t), *key)).rowcount
    short += dropped  # stored, then taken out: not missing
    if window < FROM_WINDOW:
        continue
    if HISTORY:
        upto = bisect.bisect_left(rest_at, b)
        if upto > folded:
            window_from(ordinary)
            learn(rest[folded:upto], ordinary)
            folded = upto
        mine = [c for c in cases if any(a <= ms(t) <= b for t in c["times"])]
        window_from(ordinary)
        learn([case_lines[i] for c in mine for i, *_ in c["rows"]], os.path.join(work, "state.window.sqlite"))
    before = dict(sim)
    if not SIMULATE:
        judge = subprocess.Popen([T, "run", "--config", JUDGE], cwd=work, env=env, stdout=judge_log, stderr=subprocess.STDOUT)
    t = a
    while t < b:
        t = min(t + STEP * 1000, b)
        send_until(t)
        drain()
    db.execute(f'INSERT INTO "{table}" (at, record, emitted) VALUES (?, ?, 0)', (b + timeout + 1000, '{"_replay": true}'))
    drain()
    stop_judge()
    db.execute(f'DELETE FROM "{table}" WHERE emitted = 0')
    judged = db.execute(f'SELECT count(*) FROM "{table}" WHERE at >= ? AND at < ? AND emitted IS NOT NULL', (a, b)).fetchone()[0]
    if SIMULATE:
        sim["windows"].append({"from": a, "to": b, **{x: sim[x] - before[x] for x in ("groups", "requests", "fresh", "context", "bytes")}})
    log(f"window {k + 1}/{len(merged)} {datetime.fromtimestamp(a / 1000, timezone.utc):%Y-%m-%d %H:%M}: "
        f"{judged} records judged; {sent} sent in all; {dropped} rows of earlier cases dropped; {time.time() - started:.0f}s")

if SIMULATE:
    json.dump(sim, open(os.path.join(work, "sim.json"), "w"))
    log({x: sim[x] for x in ("groups", "requests", "fresh", "context", "bytes")})
sock.close()
ingest.send_signal(signal.SIGINT)
ingest.wait()
