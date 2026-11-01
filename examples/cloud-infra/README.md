# cloud-infra

Your own infrastructure's logs, every record read by a model with the rest
of its actor's recent records; a person reads what it flags. Install, point your
logs at it, fill in one file about your company, run.

It is built only from [packs](/packs): `ingest.yaml` and `judge.yaml` include
[core](/packs/core) and five packs, and add the sources, their edges and the
profile.

```
CloudTrail ──▶ aws ──────────────┐
GitHub audit log ──▶ github ─────┤
Okta ──▶ okta ───────────────────┼──▶ logs.sqlite        (ingest.yaml)
Google Workspace ──▶ google_workspace
NDJSON on :9400 ──▶ each pack ───┤   (CI, runner egress, your product's logs)
                                 └──▶ baseline ──▶ state.sqlite

logs.sqlite ──▶ actors ──▶ baseline ──▶ judge ──▶ fresh ──▶ flagged ──▶ alerts.ndjson  (judge.yaml)
state.sqlite ─────────────────┘                    └──▶ verdicts.ndjson
```

- **The packs** ([aws](/packs/aws), [github](/packs/github), [okta](/packs/okta),
  [google_workspace](/packs/google_workspace), [app](/packs/app)) each turn
  their provider's records into finished ones with one plugin: every source's
  shape into one, `_norm.{actor, org, kind, ip, country, dest, target}`; the
  record trimmed to what it means (ids, hashes, protocol details and bulk go);
  the address described from the runtime's tables (country, network and owner,
  cloud or datacenter, Tor exit, VPN, one of yours), all loaded once for the
  process, nothing leaving the machine; and what your profile knows about it
  in `_facts`: whether the actor is AWS itself, a member, a service, a key or
  nobody known; whether the address, destination and target are usual; which
  kinds you watch. Every record goes on.
- **logs** keeps every record once in `logs.sqlite`, at its `ts`, with two
  partitions: the org, and the org and actor.
- **baseline** folds every record into `state.sqlite` too: when each actor
  first did each thing, from where and with what, how much an hour, and each
  person's sign-ins and credential changes in every source (`people` in the
  profile says which names are one person). That ends ingest, one process;
  the judge is another, so nothing in ingest waits on a model.
- **actors** reads `logs.sqlite` back forty records of one actor at a time,
  oldest first. A quiet actor's go once the oldest is an hour older than the
  newest record stored, or once nothing new has been stored for an hour,
  topped up to forty with the actor's latest records already judged, marked
  `_context`. A record that arrives late waits with the actor's others. Rows
  are marked once their verdicts are written, and only then do the actor's
  next go; a restart sends again what the judge had not finished.
- **baseline** again, in the judge, reads that state without its lock and
  puts the same `_baseline` on each of an actor's records: what was known of
  the actor before its first record, facts, not verdicts
  ([packs/core](/packs/core#baseline) has what is in it).
- **judge** reads an actor's records together, as **actors** sends them and
  never two actors in one request, and writes a verdict per record: a pass is
  the flag alone, a flag says why. **fresh** drops the `_context` records,
  judged before; **flagged** keeps the verdicts a person should see.
  [packs/core](/packs/core#judgeyaml) has the model's settings and Slack.

## Install and run

```bash
make setup          # Rust WASM toolchain; GeoLite2, cloud ranges, Tor exits into examples/enrich/data
$EDITOR profile/profile.yaml
make test           # every pack's fixtures; no model needed
make run            # ingest, then the judge once ingest's state.sqlite is there; listens on :9400 for NDJSON records
```

The judge defaults to a local model on `:8080`
([packs/core](/packs/core#judgeyaml) says how to serve it). For a hosted one:

```bash
JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
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
| `okta` | [okta](/packs/okta#use-it) | the System Log |
| `google_workspace` | [google_workspace](/packs/google_workspace#use-it) | login, token, drive and admin activity |

Anything else is one JSON line per record on `:9400`, with a `source` field
naming its shape and a `ts`: `gha_ci` and `runner_egress` for the
[github](/packs/github) pack, `app` and `agent` for [app](/packs/app), or one
of the four above. Every pack sees the stream and takes its own; a record no
pack takes goes no further, with a warning.

## The profile

[profile/profile.yaml](profile/profile.yaml) is every fact about your company
that no log carries, the `profile` table `ingest.yaml` names; the model reads
it as `_facts`. [packs/core](/packs/core#the-profile) lists the keys. It ships
filled in for the fictional company the cases describe.

## What it catches

[cases/](cases/) holds 34 sequences as the real sources write them, 28 that should flag and 6 that must not. Half follow disclosed compromises of 2025 and 2026 ([cases/README.md](cases/README.md) has the table and sources): TeamPCP's Trivy and LiteLLM, the axios hijack, nx/s1ngularity, Shai-Hulud's second wave, tj-actions/changed-files, Railway's environment-variable breach, Vercel via Context.ai. The rest are the classics: root login, CloudTrail stopped, a bucket made public, MFA off, impossible travel, a Gmail forwarding rule, a branch protection removed.

This shape, an actor's latest forty records, is not measured yet. The two before it, over the cases of both examples and 38 days of synthetic ordinary traffic: a judge reading every record in batches with each org's history as context (Claude Haiku 4.5) flagged 38 to 39 of the 40 positives, 1 of 11 negatives and 13 of 16,199 ordinary records; a rulebook gate in front of a local 30B judge, 38 of 40 and 2 of 11.

The packs' fixtures were cut from eight of the cases; `make test` runs them, needs no model, and runs in CI.

## Files

```
ingest.yaml           core and the five packs, the sources and their edges, the profile
judge.yaml            core's judge and the packs' logfmt rows
profile/profile.yaml  your company (edit this)
cases/                the sequences, one directory each: case.yaml + events.ndjson
```
