#!/usr/bin/env python3
"""Replay a stream through packs/macos: the plugin, and with --judge the
model after it.

    python3 -I evals/macos/run.py STREAM WORK [--judge none|local|bedrock] [--no-run]

Writes WORK/config.yaml (JSON, which is YAML): a file source reading STREAM
(gzipped when its name ends in .gz), `runtime.workers: 1` and lru state as packs/macos/ingest.yaml has them, the
`macos` plugin from packs/macos/plugins/, and a file sink, WORK/out.ndjson,
of every record the plugin keeps. Then runs `$TANGENT run` (TANGENT: this
worktree's target/release/tangent) until the stream is through and the sink
flushed, and stops it.

--judge none (the default): `tangent run --once`, which reads the file,
  drains and exits.
--judge local: packs/macos/judge.yaml's inference node between the plugin and
  the sink, every verdict kept (no `flagged` filter): any OpenAI-compatible
  server at JUDGE_URL (http://localhost:8080/v1), JUDGE_MODEL.
--judge bedrock: Claude Haiku 5.5 on Bedrock as evals/harness/maincfg.py
  configures it: thinking disabled, max_tokens 400, timeout 90 s, retries 6,
  concurrency 16; BEDROCK_MODEL, BEDROCK_REGION (us-west-2) and AWS_PROFILE
  as there.
A judged run first counts what the plugin keeps (WORK/plugin.yaml, --once),
then runs config.yaml until out.ndjson holds that many records (each with a
`judge` field: a verdict, or the error that took its place) and stops it with
SIGINT. STALL (900 s) without a new record ends it. A judged run costs money
or a local model's hour. --no-run writes the configs only."""
import argparse
import gzip
import json
import os
import re
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
PACK = os.path.join(ROOT, "packs", "macos")
TANGENT = os.environ.get("TANGENT") or os.path.join(ROOT, "target", "release", "tangent")
STALL = int(os.environ.get("STALL", "900"))
node = lambda kind, name: {"kind": kind, "name": name}


def judge_yaml():
    """The scalar settings of judge.yaml's `inference.judge` (no YAML parser
    here: its block is `key: value` lines, comments after)."""
    text = open(os.path.join(PACK, "judge.yaml")).read()
    block = re.search(r"^inference:\n  judge:\n((?:    .*\n|\s*\n)+)", text, re.M).group(1)
    out = {}
    for k, v in re.findall(r"^    (\w+):[ \t]*([^#\n]*?)[ \t]*(?:#.*)?$", block, re.M):
        if v:
            out[k] = int(v) if v.isdigit() else v
    return out


def configs(stream, work, judge):
    base = {
        "runtime": {"workers": 1, "plugins_path": os.path.join(PACK, "plugins") + "/",
                    "state": {"backend": "lru", "default_ttl_ms": 86400000, "max_ttl_ms": 0}},
        "sources": {"es": {"type": "file", "path": stream,
                           "decoding": {"format": {"type": "ndjson"},
                                        "compression": {"type": "gzip" if stream.endswith(".gz") else "none"}}}},
        "plugins": {"macos": {"module_type": "rust", "path": PACK}},
        "sinks": {"out": {"type": "file", "path": os.path.join(work, "out.ndjson"), "compression": {"type": "none"}}},
        "dag": [{"from": node("source", "es"), "to": [node("plugin", "macos")]},
                {"from": node("plugin", "macos"), "to": [node("sink", "out")]}],
    }
    if judge == "none":
        return base, None
    plugin_only = json.loads(json.dumps(base))
    plugin_only["sinks"]["out"]["path"] = os.path.join(work, "plugin.ndjson")
    j = judge_yaml()
    prompt, schema = (os.path.normpath(os.path.join(PACK, j[k])) for k in ("prompt_file", "schema_file"))
    if judge == "local":
        m = {"backend": j["backend"], "url": j["url"], "model": j["model"], "api_key": j["api_key"],
             "prompt_file": prompt, "schema_file": schema, "batch_bytes": j["batch_bytes"],
             "concurrency": j["concurrency"], "max_tokens": j["max_tokens"], "timeout_ms": j["timeout_ms"],
             "extra": {"chat_template_kwargs": {"enable_thinking": False}}}
    else:
        aws = {"region": os.environ.get("BEDROCK_REGION") or "us-west-2"}
        if os.environ.get("AWS_PROFILE"):
            aws["profile"] = os.environ["AWS_PROFILE"]
        m = {"backend": "bedrock", "model": os.environ.get("BEDROCK_MODEL") or "us.anthropic.claude-haiku-5-5",
             "aws": aws, "prompt_file": prompt, "schema_file": schema, "batch_bytes": j["batch_bytes"],
             "max_tokens": 400, "timeout_ms": 90000, "retries": 6, "concurrency": 16,
             "extra": {"thinking": {"type": "disabled"}}}
    base["inference"] = {"judge": m}
    base["dag"] = [{"from": node("source", "es"), "to": [node("plugin", "macos")]},
                   {"from": node("plugin", "macos"), "to": [node("inference", "judge")]},
                   {"from": node("inference", "judge"), "to": [node("sink", "out")]}]
    return base, plugin_only


def lines(path):
    if not os.path.exists(path):
        return 0
    with (gzip.open if path.endswith(".gz") else open)(path, "rb") as f:
        return sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1 << 20), b""))


def once(cfg, work, env, logname):
    log = os.path.join(work, logname)
    with open(log, "w") as out:
        rc = subprocess.run([TANGENT, "run", "--once", "--config", cfg], cwd=work, env=env, stdout=out,
                            stderr=subprocess.STDOUT).returncode
    text = open(log, errors="replace").read()
    bad = [w for w in ("timeout exceeded", "may be dropped", "plugin trapped", "guest error") if w in text]
    if rc or bad:
        sys.exit(f"tangent run failed (exit {rc}{'; ' + ', '.join(bad) if bad else ''}); see {log}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("stream")
    ap.add_argument("work")
    ap.add_argument("--judge", choices=["none", "local", "bedrock"], default="none")
    ap.add_argument("--no-run", action="store_true")
    a = ap.parse_args()
    stream, work = os.path.abspath(a.stream), os.path.abspath(a.work)
    os.makedirs(work, exist_ok=True)
    cfg, plugin_only = configs(stream, work, a.judge)
    at = lambda f: os.path.join(work, f)
    json.dump(cfg, open(at("config.yaml"), "w"), indent=1)
    if plugin_only:
        json.dump(plugin_only, open(at("plugin.yaml"), "w"), indent=1)
    if a.no_run:
        print(at("config.yaml"))
        return
    env = dict(os.environ, RUST_LOG=os.environ.get("RUST_LOG", "info"))
    if ((cfg.get("inference") or {}).get("judge") or {}).get("aws", {}).get("profile"):
        env.pop("AWS_BEARER_TOKEN_BEDROCK", None)  # it would be used instead of the profile (replay.py)
    for f in ("out.ndjson", "plugin.ndjson"):
        if os.path.exists(at(f)):
            os.remove(at(f))  # the file sink appends
    n_in = lines(stream)
    started = time.time()
    if not plugin_only:
        once(at("config.yaml"), work, env, "run.log")
        print(f"{n_in} events in, {lines(at('out.ndjson'))} records kept, {time.time() - started:.1f}s: {at('out.ndjson')}")
        return

    once(at("plugin.yaml"), work, env, "plugin.log")
    want = lines(at("plugin.ndjson"))
    print(f"{n_in} events in, {want} records kept by the plugin; judging ({a.judge})", file=sys.stderr)
    log = open(at("run.log"), "w")
    p = subprocess.Popen([TANGENT, "run", "--config", at("config.yaml")], cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        last, since = -1, time.time()
        while True:
            n = lines(at("out.ndjson"))
            if n >= want:
                break
            if p.poll() is not None:
                sys.exit(f"tangent exited ({p.returncode}) with {n} of {want} records judged; see {at('run.log')}")
            if n != last:
                last, since = n, time.time()
            elif time.time() - since > STALL:
                sys.exit(f"no new verdict in {STALL}s: {n} of {want} records judged; see {at('run.log')}")
            time.sleep(1)
    finally:
        if p.poll() is None:
            p.send_signal(signal.SIGINT)
            try:
                p.wait(timeout=180)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
    errors = sum(1 for l in open(at("out.ndjson")) if "error" in (json.loads(l).get("judge") or {}))
    print(f"{n_in} events in, {lines(at('out.ndjson'))} records judged ({errors} errors), "
          f"{time.time() - started:.0f}s: {at('out.ndjson')}")


if __name__ == "__main__":
    main()
