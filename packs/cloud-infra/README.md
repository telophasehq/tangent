# cloud-infra

Your own infrastructure's logs, read by a model only when context says a
person should look. Install, point your logs at it, fill in one file about
your company, run.

```
CloudTrail, GitHub audit log, Okta, Google Workspace, CI ──▶ norm ──▶ enrich ──▶ gate ──▶ window ──▶ judge ──▶ alerts.ndjson
```

- **norm** puts every source's shape into one: `_norm.{actor, org, kind, ip, country, dest, target}`.
- **enrich** adds what is known about the address: country, ASN and organization, cloud or datacenter, Tor exit, VPN, one of yours. From files on disk; nothing leaves the machine ([examples/enrich](/examples/enrich)).
- **gate** decides, from your profile, which records a model reads: a kind on the always-look list, a hard rule (root credentials, something opened to the world), a person on hosting or abroad, a service outside your cloud, an API key off its networks, a principal you do not know, a host your systems do not talk to, a sensitive asset. Once per reason per hour. No statistics: nothing here counts, averages or learns.
- **window** gives the judge the actor's last day and the org's last hour, read back from the runtime's retained rows.
- **judge** is any OpenAI-compatible model with [judge.md](judge.md) as its brief. It adds a verdict; **flagged** keeps the ones a person should see.

On the 34 cases in [cases/](cases/) plus 38 days of ordinary traffic, the gate sends the model about 0.4% of records; the model's recall on the cases is in [RESULTS](#what-it-catches).

## Install and run

```bash
make setup          # Rust WASM toolchain; GeoLite2, cloud ranges, Tor exits into examples/enrich/data
$EDITOR profile/profile.yaml
make test           # the cases through norm, gate and window; no model needed
make run            # listens on :9400 for NDJSON records
```

The judge defaults to a local model on `:8080` (mlx_lm.server or llama-server). For a hosted one:

```bash
JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
```

### Getting logs in

Every record is one JSON line with a `source` field naming its shape (`cloudtrail`, `github_audit`, `okta`, `google_workspace`, `gha_ci`, `runner_egress`) and a `ts`. Today that means a forwarder: Vector, Fluent Bit, or a script reading your S3 delivery buckets, sending to `:9400`. Direct sources that poll Okta and Workspace and read CloudTrail and GitHub audit streaming from S3 are the next thing this pack gets; the plugins do not change when they land.

## The profile

[profile/profile.yaml](profile/profile.yaml) is every fact about your company that no log carries, and it is the whole gate:

| key | what it is | what it decides |
|---|---|---|
| `org`, `own_sources` | your name; which sources are your own infrastructure | all of them land in one partition, yours |
| `countries` | where your people are | a person signing in from elsewhere reaches the judge |
| `cloud` | where your services run | a service seen on any other hosting reaches the judge |
| `members` | people: IdP emails, GitHub handles | anyone else is a principal you do not know |
| `services` | IAM users and roles, bots | exempt from the "person abroad" rules, held to `cloud` |
| `keys` | API keys and the networks each calls from | a key off its networks reaches the judge |
| `hosts` | hosts your systems talk to (`*.suffix` allowed) | a request anywhere else reaches the judge |
| `sensitive` | buckets, repos, tables | a touch reaches the judge every time |

The file ships filled in for the fictional company the cases describe, so `make test` passes before you touch it.

## What it catches

[cases/](cases/) holds 34 sequences as the real sources write them, 28 that should flag and 6 that must not. Half follow disclosed compromises of 2025 and 2026 ([cases/README.md](cases/README.md) has the table and sources): TeamPCP's Trivy and LiteLLM, the axios hijack, nx/s1ngularity, Shai-Hulud's second wave, tj-actions/changed-files, Railway's environment-variable breach, Vercel via Context.ai. The rest are the classics: root login, CloudTrail stopped, a bucket made public, MFA off, impossible travel, a Gmail forwarding rule, a branch protection removed.

Measured with a local 30B model as judge over the cases and 38 days of synthetic ordinary traffic: 0.4% of records reach the model, 38 of the 40 positives across both packs are flagged, 2 of 11 negatives, 3 ordinary records in 38 days. The two misses are a mass push by a personal access token (a member, a home address, nothing contextual about one push) and a typosquatted dependency (nothing about one PR is new). The negatives that flag are an admin's monthly export, which is on the always-look list, and a key rotation that the local model misreads.

`make test` runs the cases through norm, gate and window and compares with [tests/](tests/); it needs no model and runs in CI.

## Files

```
tangent.yaml          the pipeline
profile/profile.yaml  your company (edit this)
judge.md              the model's brief
norm/ gate/ window/   the plugins, Rust, compiled to WASM by make setup
cases/                the sequences, one directory each: case.yaml + events.ndjson
tests/                fixtures for make test
```
