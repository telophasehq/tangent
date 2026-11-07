# cloud-infra

Your own infrastructure's logs, every record read by a model with the rest
of its actor's recent records; a person reads what it flags. Install, point your
logs at it, fill in one file about your company, run.

It is built only from [packs](/packs): `tangent.yaml` includes
[judge](/packs/judge), which brings [enrich-ip](/packs/enrich-ip) and
[behavior-baseline](/packs/behavior-baseline), and four packs, and adds the
sources, the lake, their edges, each pack's edges to the lake, the baseline
and the judge, and the profile. One config, one process.

```
CloudTrail ──▶ aws ────────────────────┐
GitHub audit log ──▶ github ───────────┤
Okta ──▶ okta ─────────────────────────┼──▶ lake/, every record as Parquet
Google Workspace ──▶ google_workspace ─┤
NDJSON on :9400 ──▶ each pack ─────────┼──▶ baseline_builder ──▶ state.sqlite
                                       │                              │
                                       └──▶ baseline_renderer ◀───────┘
                                            an actor's records, a group at a time
                                                 │
                                                 ▼
                                               judge ──▶ fresh ──┬──▶ flagged ──▶ alerts.ndjson
                                                                 └──▶ verdicts.ndjson
```

- **The packs** ([aws](/packs/aws), [github](/packs/github), [okta](/packs/okta),
  [google_workspace](/packs/google_workspace)) each turn their provider's
  records into finished ones with one plugin: every source's shape into one,
  `_norm.{actor, person, org, kind, ip, country, dest, target}`;
  the record trimmed to what it means (ids, hashes, protocol details and bulk
  go); the address described from the runtime's tables (country, network and
  owner, cloud or datacenter, Tor exit, VPN, one of yours), all loaded once for
  the process, nothing leaving the machine; and what your profile knows about
  it in `_facts`: whether the actor is AWS itself, a member, a service, a key
  or nobody known; whether the address, destination and target are usual; which
  kinds you watch. Every record goes on.
- **lake** keeps every record, as Parquet files a directory an hour under
  `lake/`, for DataFusion or DuckDB; delete old hours to trim it
  ([packs](/packs/README.md#the-lake) has what is in it).
- **baseline_builder** folds every record into `state.sqlite` too: when each
  actor first did each thing, from where and with what, how much an hour, and
  each person's sign-ins and credential changes in every source (`people` in
  the profile says which names are one person).
- **baseline_renderer** gets each actor's records a group at a time: thirty,
  or fewer an hour after the first of them, with its latest ten again
  ahead of them, marked `_context`. Groups wait in memory and run on workers
  of their own, so nothing waits on the model; one group per actor is judged
  at a time. Stopping the process ends the model's calls: records still in a
  group or waiting on the model are not judged, though they are in the lake
  and in `state.sqlite`. It puts
  the same `_baseline` on each of an actor's records: what was known of the
  actor before its first record, facts, not verdicts
  ([packs/behavior-baseline](/packs/behavior-baseline#what-it-stamps) has what is in it).
- **judge** reads an actor's records together, never two actors in one
  request, and writes a verdict per record: a pass is
  the flag alone, a flag says why. **fresh** drops the `_context` records,
  judged before; **flagged** keeps the verdicts a person should see.
  [packs/judge](/packs/judge#the-model) has the model's settings and Slack.

## Install and run

Until the release after 0.1.9, two steps come first (the paragraphs after the
commands say why): build tangent from this checkout,
`cargo install --path ../../crates/cli --locked`, and `export PLUGINS=compile`.

```bash
make setup          # GeoLite2, cloud ranges, Tor exits into examples/enrich/data
$EDITOR profile/profile.yaml
make test           # the packs' plugins from the release, then every pack's fixtures; no model needed
make run            # one process: the sources, the packs and the judge; listens on :9400 for NDJSON records
```

Nothing is compiled: `make test` and `make run` take the packs' plugins
prebuilt. `tangent plugin fetch` downloads each from the tangent release of
this tangent's version and has `gh attestation verify` check, before it is
kept, that this repository's release workflow built it from that tag
([Prebuilt plugins](/README.md#prebuilt-plugins)); it needs `gh`, not a
login.

Until the release after 0.1.9 there are none to fetch (`make test` stops at
"v0.1.9 has no aws.component.wasm"), and the packs need a tangent built from
the checkout anyway ([Using packs](/README.md#using-packs)). Build the plugins
here instead, as for a pack you are changing: `make test PLUGINS=compile`, then
`make run PLUGINS=compile`, or `export PLUGINS=compile` once. It installs the
Rust WASM toolchain first (`./setup.sh toolchain`).

The judge defaults to a local model on `:8080`
([packs/judge](/packs/judge#the-model) says how to serve it). For a hosted one:

```bash
JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
```

## Try it

With `make run` going, send it a few of the packs' fixtures, a JSON line a
record, from another terminal in this directory:

```bash
python3 -c 'import json, sys; [print(json.dumps(r)) for f in sys.argv[1:] for r in json.load(open(f))]' \
  ../../packs/okta/tests/input.json ../../packs/google_workspace/tests/input.json | nc -w 1 localhost 9400
```

Each record goes to the lake (`lake/`, a Parquet file each hour and when
the process stops) and into `state.sqlite` at once. Its verdict takes longer:
an actor's records go to the model at thirty, or an hour after the first of
them, so `verdicts.ndjson` fills within the hour, and `alerts.ndjson` with
what was flagged; stopped sooner, they go unjudged. To see verdicts seconds after the records while you try it,
close an actor's group after ten seconds' quiet rather than an hour's, in
`tangent.yaml`, and take it out again before you point real logs at it:

```yaml
plugins:
  baseline_renderer:
    expire_after_ms: 10000
```

## Getting logs in

Four sources read their APIs directly, with nothing in between. Each wakes up
when its credential is exported (the Makefile lists the variables) and, left
empty, says so once and stays idle. Each pack's README says what its
credential needs.

| source | pack | what it reads |
|---|---|---|
| `cloudtrail` | [aws](/packs/aws#credentials) | a trail's files: every event, data events included, every account and region of an organization trail, a few minutes behind |
| `github_audit` | [github](/packs/github#use-it) | the organization's audit log, web and git events |
| `okta_audit` | [okta](/packs/okta#use-it) | the System Log |
| `workspace_audit` | [google_workspace](/packs/google_workspace#use-it) | login, token, drive and admin activity |

Anything else is one JSON line per record on `:9400`, with a `source` field
naming its shape and a `ts`: `gha_ci` and `runner_egress` for the
[github](/packs/github) pack, or the `source` the four above give their
records: `cloudtrail`, `github_audit`, `okta`, `google_workspace`. Every pack
sees the stream and takes its own; a record no pack takes goes no further,
with a warning.

## The profile

[profile/profile.yaml](profile/profile.yaml) is every fact about your company
that no log carries, the `profile` table `tangent.yaml` names; the model reads
it as `_facts`. [packs/enrich-ip](/packs/enrich-ip#the-profile) lists the keys. It ships
filled in for the fictional company the cases in [tangent-eval's cases](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases)
describe.

## What it catches

34 of the sequences in [tangent-eval's cases](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases) are this example's, as the real sources write them, 28 that should flag and 6 that must not. Half follow disclosed compromises of 2025 and 2026 ([tangent-eval's cases README](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases#cases-from-disclosed-incidents-20252026) has the table and sources): TeamPCP's Trivy and LiteLLM, the axios hijack, nx/s1ngularity, Shai-Hulud's second wave, tj-actions/changed-files, Railway's environment-variable breach, Vercel via Context.ai. The rest are the classics: root login, CloudTrail stopped, a bucket made public, MFA off, impossible travel, a Gmail forwarding rule, a branch protection removed.

This shape, an actor's records thirty at a time with ten before them, is not measured yet. The two before it, over tangent-eval's cases and 38 days of synthetic ordinary traffic: a judge reading every record in batches with each org's history as context (Claude Haiku 4.5) flagged 38 to 39 of the 40 positives, 1 of 11 negatives and 13 of 16,199 ordinary records; a rulebook gate in front of a local 30B judge, 38 of 40 and 2 of 11.

The packs' fixtures were cut from eight of the cases; `make test` runs them, needs no model, and runs in CI.

## Files

```
tangent.yaml          judge and the four packs, the sources, the lake, every edge, the profile
profile/profile.yaml  your company (edit this)
```
