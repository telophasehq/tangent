#!/usr/bin/env python3
"""A WORK dir for replay.py / shard.py that runs this repo's shipped shape:
examples/cloud-infra's ingest.yaml and judge.yaml with their includes (packs/core
+ aws, github, okta, google_workspace, app) merged the way tangent merges them,
fed by one tcp source.

    python3 -I evals/harness/maincfg.py shipped|isolated WORK [--port N] [--judge bedrock|local]
                                        [--profile FILE] [--plugins DIR] [--tripwire NAME=PATH]

shipped:  as examples/cloud-infra runs it: baseline_builder in ingest folds every
          record (every case too) into WORK/state.sqlite; the judge reads it
          read-only. An earlier case's calls, addresses and resources are then
          "seen" for a later case.
isolated: baseline_builder taken out of ingest; history.yaml (the learn config
          replay.py runs per window) folds the ordinary stream plus only that
          window's cases into state.window.sqlite, which the judge reads. No case
          reads another case's history.

--judge bedrock (the default): Claude Haiku 5.5 on Bedrock: thinking disabled,
          max_tokens 400, no temperature or top_p (Haiku 5.5 refuses them),
          concurrency 16, retries 6. BEDROCK_MODEL overrides the model,
          BEDROCK_REGION the region (us-west-2); AWS_PROFILE, when set, is the
          profile written into judge.yaml (else the default credential chain).
--judge local: packs/core's judge as it ships, any OpenAI-compatible server at
          JUDGE_URL (http://localhost:8080/v1 by default), JUDGE_MODEL, with one
          request in flight: one model on one machine is no faster with more.

What it changes from examples/cloud-infra (and nothing else):
- sources: one tcp source `events` on 127.0.0.1:PORT (9501), routed to every
  pack as examples/cloud-infra routes its `events`
- profile: FILE (evals/profile/profile.yaml) copied to WORK/profile.yaml
- paths absolute into WORK; plugins_path DIR (examples/cloud-infra/plugins,
  where `make compile` there writes them, with the binary replay.py runs)
- ingest runs one worker and no runtime partition; the judge's runtime takes
  batch_size 4096 / batch_age 200, and a `seen` sink records every record the
  model read, for cost
- --tripwire NAME=PATH: a plugin NAME at PATH after every pack plugin (beside
  `logs`), writing to WORK/tripwire.ndjson; left out of history.yaml

It also writes judge-sim.yaml: the judge's groups with `_baseline` stamped and
written to WORK/headers.ndjson, no model (JUDGE=judge-sim.yaml replay.py ...)."""
import argparse
import copy
import os
import shutil
import sys

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))  # the repository
EX = os.path.join(ROOT, "examples", "cloud-infra")

ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
ap.add_argument("arm", choices=["shipped", "isolated"])
ap.add_argument("work")
ap.add_argument("--port", type=int, default=9501)
ap.add_argument("--judge", choices=["bedrock", "local"], default="bedrock")
ap.add_argument("--profile", default=os.path.join(HERE, "..", "profile", "profile.yaml"))
ap.add_argument("--plugins", default=os.path.join(EX, "plugins"))
ap.add_argument("--tripwire", metavar="NAME=PATH")
args = ap.parse_args()
arm, port = args.arm, args.port
work, profile, plugins = (os.path.abspath(p) for p in (args.work, args.profile, args.plugins))
trip = args.tripwire.split("=", 1) if args.tripwire else None
os.makedirs(work, exist_ok=True)
at = lambda f: os.path.join(work, f)


class Plain(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


put = lambda f, c: yaml.dump(c, open(at(f), "w"), Dumper=Plain, sort_keys=False, width=160)
node = lambda kind, name: {"kind": kind, "name": name}

# ---- include, as crates/shared/src/include.rs merges it (maps by key, a file over
# what it includes, `dag` edges added up in file order, paths rebased to their file)
PATHS = [("plugins", "*", "path"), ("plugins", "*", "data"), ("inference", "*", "prompt_file"),
         ("inference", "*", "schema_file"), ("enrichment_tables", "*", "path")]


def rebase(m, d):
    for p in PATHS:
        for k, v in (m.get(p[0]) or {}).items():
            if isinstance(v, dict) and isinstance(v.get(p[2]), str) and not os.path.isabs(v[p[2]]):
                v[p[2]] = os.path.normpath(os.path.join(d, v[p[2]]))


def merge(a, b):
    """b over a"""
    if not isinstance(a, dict) or not isinstance(b, dict):
        return copy.deepcopy(b)
    out = dict(a)
    for k, v in b.items():
        if v is None:
            out.pop(k, None)
        else:
            out[k] = merge(a.get(k), v) if k in a else copy.deepcopy(v)
    return out


def load(path, seen=None):
    seen = set() if seen is None else seen
    path = os.path.abspath(path)
    m = yaml.safe_load(open(path)) or {}
    rebase(m, os.path.dirname(path))
    tree, dag = {}, []
    for inc in m.pop("include", None) or []:
        p = os.path.normpath(os.path.join(os.path.dirname(path), inc))
        if p in seen:
            continue
        seen.add(p)
        t, d = load(p, seen)
        tree, dag = merge(tree, t), dag + d
    dag += m.pop("dag", None) or []
    return merge(tree, m), dag


def flat(path):
    tree, dag = load(path)
    tree["dag"] = dag
    for p in (tree.get("plugins") or {}).values():
        p.pop("tests", None)
    tree["runtime"]["plugins_path"] = plugins
    return tree


def drop(c, name):
    """`c` without plugin `name`: what fed it feeds what it fed."""
    me = node("plugin", name)
    outs = [t for e in c["dag"] if e["from"] == me for t in e["to"]]
    c["plugins"].pop(name, None)
    dag = []
    for e in c["dag"]:
        to = [x for t in e["to"] for x in (outs if t == me else [t])]
        to = [t for i, t in enumerate(to) if t not in to[:i]]
        if e["from"] != me and to:
            dag.append(dict(e, to=to))
    c["dag"] = dag


# ---- the profile
shutil.copyfile(profile, at("profile.yaml"))

# ---- ingest
i = flat(os.path.join(EX, "ingest.yaml"))
packs = next(e["to"] for e in i["dag"] if e["from"] == node("source", "events"))
i["dag"] = [e for e in i["dag"] if e["from"]["kind"] != "source"]
i["sources"] = {"events": {"type": "tcp", "bind_address": f"127.0.0.1:{port}"}}
i["dag"].insert(0, {"from": node("source", "events"), "to": packs})
i["enrichment_tables"]["profile"]["path"] = at("profile.yaml")
i["runtime"]["state"]["path"] = at("state.sqlite")
i["runtime"]["workers"] = 1
i["runtime"].pop("partition", None)
i["sinks"]["logs"]["path"] = at("logs.sqlite")
if trip:
    name, path = trip
    i["plugins"][name] = {"module_type": "rust", "path": os.path.abspath(path)}
    i["sinks"]["tripwire"] = {"type": "file", "path": at("tripwire.ndjson"), "compression": {"type": "none"}}
    for e in i["dag"]:
        if node("sink", "logs") in e["to"]:
            e["to"].append(node("plugin", name))
    i["dag"].append({"from": node("plugin", name), "to": [node("sink", "tripwire")]})
learn = copy.deepcopy(i)
if arm == "isolated":
    drop(i, "baseline_builder")
put("ingest.yaml", i)

# ---- history.yaml: the learn config replay.py runs per window (isolated only)
if arm == "isolated":
    h = learn
    if trip:  # a leaf: its plugin, its sink and every edge to them go
        gone = (node("plugin", trip[0]), node("sink", "tripwire"))
        h["plugins"].pop(trip[0])
        h["dag"] = [dict(e, to=[t for t in e["to"] if t not in gone]) for e in h["dag"] if e["from"] not in gone]
    h["runtime"]["state"] = {"backend": "sqlite", "path": at("state.window.sqlite"), "default_ttl_ms": 0, "max_ttl_ms": 0}
    h["sources"] = {"learn": {"type": "file", "path": at("learn.ndjson"),
                              "decoding": {"format": {"type": "ndjson"}, "compression": {"type": "none"}}}}
    h["sinks"] = {"none": {"type": "blackhole"}}
    for e in h["dag"]:
        if e["from"]["kind"] == "source":
            e["from"] = node("source", "learn")
        e["to"] = [node("sink", "none") if t == node("sink", "logs") else t for t in e["to"]]
    put("history.yaml", h)

# ---- judge
j = flat(os.path.join(EX, "judge.yaml"))
j["runtime"].update(batch_size=4096, batch_age=200)
j["runtime"].pop("partition", None)
j["runtime"]["state"]["path"] = at("state.window.sqlite" if arm == "isolated" else "state.sqlite")
for s in j["sources"].values():
    s["path"] = at("logs.sqlite")
m = j["inference"]["judge"]
shutil.copyfile(m["prompt_file"], at("judge.md"))
if args.judge == "bedrock":
    aws = {"region": os.environ.get("BEDROCK_REGION") or "us-west-2"}
    if os.environ.get("AWS_PROFILE"):
        aws["profile"] = os.environ["AWS_PROFILE"]
    j["inference"]["judge"] = {
        "backend": "bedrock",
        "model": os.environ.get("BEDROCK_MODEL") or "us.anthropic.claude-haiku-5-5",
        "aws": aws,
        "prompt_file": at("judge.md"),
        "schema_file": m["schema_file"],
        "group_by": m["group_by"],
        "batch_bytes": m["batch_bytes"],
        "max_tokens": 400,
        "timeout_ms": 90000,
        "retries": 6,
        "concurrency": 16,
        "extra": {"thinking": {"type": "disabled"}},
        "encoding": m["encoding"],
    }
else:
    m.update(prompt_file=at("judge.md"), concurrency=1)
for s in j["sinks"].values():
    if "path" in s:
        s["path"] = at(os.path.basename(s["path"]))
j["sinks"]["seen"] = {"type": "file", "path": at("seen.ndjson"), "compression": {"type": "none"}}
for e in j["dag"]:
    if e["from"] == node("inference", "judge"):
        e["to"].append(node("sink", "seen"))
put("judge.yaml", j)

# judge-sim: the same groups with `_baseline` stamped and written down, no model
# (JUDGE=judge-sim.yaml replay.py ...: WORK/headers.ndjson, free)
sim = copy.deepcopy(j)
del sim["inference"]
sim["plugins"] = {"baseline_renderer": j["plugins"]["baseline_renderer"]}
sim["sinks"] = {"headers": {"type": "file", "path": at("headers.ndjson"), "compression": {"type": "none"}}}
src = next(iter(sim["sources"]))
sim["dag"] = [{"from": node("source", src), "to": [node("plugin", "baseline_renderer")]},
              {"from": node("plugin", "baseline_renderer"), "to": [node("sink", "headers")]}]
put("judge-sim.yaml", sim)
print(f"{work}: {arm}, judge {args.judge}; plugins from {plugins}", file=sys.stderr)
