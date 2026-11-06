# demo

One attack, judged on your machine against seven months of the actor's
history, for free. It is tangent-eval's
[`identity/role-change-then-privileged-action`](https://github.com/EthanBlackburn/tangent-eval/blob/main/cases/identity/role-change-then-privileged-action/case.yaml):
at 02:13 dave promotes carol from viewer to admin, and at 02:19 carol starts
an export of all of acme's data from an address acme has never used.
Neither line looks wrong on its own.

```
history.ndjson.zst ──▶ app ──▶ baseline_builder ──▶ state.sqlite          (learn.yaml, no model)
                                                         │
window.ndjson ──▶ app ──┬──▶ baseline_builder            ▼
                        └──▶ baseline_renderer ──▶ judge ──▶ fresh ──┬──▶ flagged ──▶ alerts.ndjson
                                                                     └──▶ verdicts.ndjson   (tangent.yaml)
```

Both configs are [core](/packs/core) and the [app](/packs/app) pack as they
ship, with acme's profile. The judge is core's, at `JUDGE_URL`, with one
change: `temperature: 0`, so a run answers as the last one did (core ships
0.7).

## Run it

```bash
cargo install --path ../../crates/cli --locked     # tangent from this checkout
llama-server -hf unsloth/GLM-4.7-Flash-GGUF:Q4_K_M --port 8080 -np 1 -c 32768 -ngl 99 -fa on --jinja   # the default judge, in another shell: an 18 GB download
make PLUGINS=compile
```

`PLUGINS=compile` builds the plugins here, and installs the Rust WASM
toolchain first; leave it out once a release carries them (v0.1.9 does not).
The first run also fetches GeoLite2 and the cloud ranges into
`../enrich/data` ([examples/enrich](/examples/enrich)). For a hosted model
instead of a local one, `JUDGE_URL`, `JUDGE_MODEL` and `JUDGE_API_KEY` (two
requests, well under a cent).

It prints what `alerts.ndjson` holds. With GLM-4.7-Flash on llama-server,
five runs on 2026-11-06 gave the same two verdicts: dave's promotion passed,
and carol's export was flagged with

```json
{"flag": true, "why": "The actor is using a Telegram Messenger IP address (91.108.4.12) for an org export, which is unusual for this human user and organization."}
```

on a record carrying the `_baseline` the pipeline computed for her:

```
actor   human (profile member); here as app user; privilege used before: read-only
seen    seen 207.4 days (since 2024-09-23), 529 events on 150 days, from app
new     api: org.export.started (1 of 1 in window)
        network: ASN 62041 Telegram Messenger Inc (new to org), country NL (new to org), net 91.108.4.0/24 (new to org)
time    window 02 UTC active on 0 of its 20 active days in 28d; usual 15-20 UTC; last active 5h before
auth    last 72h, person carol@acme.io: 3 logins (app 3), 0 failed; grant role viewer -> admin by dave@acme.io 6m before from 73.162.14.9; last login app sso 9h before from 73.162.14.9 ASN 7922 US, mfa yes
volume  last hour vs p90 of its 57 active hours in 28d: total normal, read normal, write -, iam -, own creds -, delete -
links   ip 91.108.4.12 new for this actor, no other actor used it before
```

The why names the address and the export but not the promotion six minutes
before, which `auth` puts on her record and which the case wants checked
first. This model has not been measured on the corpus; the measured one is
Claude Haiku 5.5 ([tangent-eval's RESULTS.md](https://github.com/EthanBlackburn/tangent-eval/blob/main/RESULTS.md)).

`make clean` removes what a run wrote.

## The data

Everything here is synthetic: acme, its people and its addresses are
tangent-eval's fictional company. From
[tangent-eval](https://github.com/EthanBlackburn/tangent-eval) at 4dcfeca
(`python3 -I stream.py v3 && python3 -I harness/past.py v3`):

| file | what it is |
|---|---|
| `window.ndjson` | the case's two records, as `runs/stream/stream-past-v3.ndjson` placed them |
| `history.ndjson.zst` | every `app` record of that stream before the case's first, that is no case's: 10,132, from 2024-09-23 |
| `profile/profile.yaml` | tangent-eval's `profile/profile.yaml` (MPL-2.0, as tangent-eval is) |
| `learn.yaml` | step 1: the history into `state.sqlite`, no model |
| `tangent.yaml` | step 2: the two records, judged |
