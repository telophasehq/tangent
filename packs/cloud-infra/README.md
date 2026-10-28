# cloud-infra

Your own infrastructure's logs, every record read by a model in its context,
in batches; a person reads what it flags. Install, point your logs at it, fill in one file about
your company, run.

```
CloudTrail ──▶ cloudtrail ─┐
GitHub audit log ──▶ github_audit ─┤
Okta ──▶ okta ─┼──▶ enrich ──▶ facts ──▶ window ──▶ judge ──▶ alerts.ndjson
Google Workspace ──▶ google_workspace ─┤
CI, anything else (TCP) ──▶ norm ─┘
```

- **A plugin per provider** ([providers/](providers/)) reads its source's records: it puts the shape into `_norm.{actor, org, kind, ip, country, dest, target}` and trims the record to what it means, dropping the provider's ids, hashes, protocol details and bulk, so the judge and the retained history carry a record a reader would keep. **norm** does the `_norm` half for anything else arriving over TCP (CI lines, a product's logs) and leaves those records as they came.
- **enrich** adds what is known about the address: country, ASN and organization, cloud or datacenter, Tor exit, VPN, one of yours. From files on disk; nothing leaves the machine ([examples/enrich](/examples/enrich)).
- **facts** writes what your profile knows about the record, and decides nothing: whether the actor is a member, a service, a key or nobody known; whether the address, destination and target are usual; which kinds you watch. Every record goes on.
- **window** attaches the actor's last day and the org's last hour, one line per event, read back from the runtime's retained rows.
- **judge** reads every record, in batches of as many as fit a request, each with its facts and history, and writes a verdict per record: `flag`, `one_line`, `check_first`. No rule decides what the model reads; the profile is context, not a rulebook.
- The judge is any OpenAI-compatible model with [judge.md](judge.md) as its brief; **flagged** keeps the verdicts a person should see.

On the 34 cases in [cases/](cases/) plus 38 days of ordinary traffic, the judge's recall and false-positive rate are in [RESULTS](#what-it-catches).

## Install and run

```bash
make setup          # Rust WASM toolchain; GeoLite2, cloud ranges, Tor exits into examples/enrich/data
$EDITOR profile/profile.yaml
make test           # the cases through norm, facts and window; no model needed
make run            # listens on :9400 for NDJSON records
```

The judge defaults to a local model on `:8080` (mlx_lm.server or llama-server). For a hosted one:

```bash
JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
```

### Getting logs in

Four sources read their APIs directly, with nothing in between. Each wakes up when its credential is exported (the Makefile lists the variables) and, left empty, says so once and stays idle.

| source | export | what it reads |
|---|---|---|
| `cloudtrail` | AWS credentials: `AWS_PROFILE`, or the SDK's default chain; `AWS_REGIONS` as a comma-separated list, one cursor each, and include `us-east-1`, where IAM, STS and console sign-in events are recorded (`AWS_REGION` for one region); the identity needs `cloudtrail:LookupEvents` | `LookupEvents` on the account, per region: management events (IAM, STS, console sign-ins, bucket policy changes), about fifteen minutes behind, nothing to set up. With `CLOUDTRAIL_QUEUE_URL`, an SQS queue fed by a trail (S3 event notifications on its bucket, or the trail's own SNS notifications): every event, data events included, gzipped files every few minutes; the identity then also needs to receive and delete on the queue and read the bucket |
| `github_audit` | `GITHUB_ORG`, `GITHUB_TOKEN` with `read:audit_log` (the audit log API is a GitHub Enterprise Cloud feature) | the organization's audit log, web and git events |
| `okta` | `OKTA_ORG_URL` (`https://acme.okta.com`), `OKTA_API_TOKEN` from Security → API → Tokens, made by a read-only administrator | the System Log |
| `google_workspace` | `GOOGLE_APPLICATION_CREDENTIALS`, a service account key file with domain-wide delegation for the scope `https://www.googleapis.com/auth/admin.reports.audit.readonly`; `GOOGLE_ADMIN_EMAIL`, the administrator it acts as (the Admin SDK API enabled on the project) | login, token, drive and admin activity |

A source keeps its place in the runtime's state store (the `sources` namespace), so a restart resumes where it stopped; on first start it reads the last hour (`since`). Records arrive as the APIs write them, plus `source` and `ts`; norm knows the four shapes.

Anything else (CI logs, runner egress, a forwarder for a source not listed) is one JSON line per record on `:9400`, with a `source` field naming its shape (`gha_ci`, `runner_egress`, or one of the four above) and a `ts`.

## The profile

[profile/profile.yaml](profile/profile.yaml) is every fact about your company that no log carries; the models read it as `_facts`:

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

Measured 2026-10-28 with Claude Haiku 4.5 (Bedrock) judging **every record** of the 51 cases of both packs inside 38 days of synthetic ordinary traffic, 17,027 records, in batches of about thirty with each batch's history written once as context: recall 39/40, the one miss a typosquatted dependency (nothing about one `npm ci` line is new); 1 of 11 negatives flagged, a key rotation the morning after a vendor's notice, which the judge reads as an admin minting service keys from home; 32 of 16,199 ordinary records flagged, 24 of them one findings poller's routine queries from its own datacenter address. About 14 minutes with 16 requests in flight, a few dollars. The previous design, a rulebook gate in front of the judge, reached 38/40 and 2/11 while judging 68 records; the two it never reached (a mass push by a personal access token, a typosquat) split, one caught, one still missed.

## Files

```
tangent.yaml          the pipeline
profile/profile.yaml  your company (edit this)
judge.md              the model's brief
providers/ norm/ facts/ window/   the plugins, Rust, compiled to WASM by make setup; providers/common is what they share
cases/                the sequences, one directory each: case.yaml + events.ndjson
tests/                fixtures for make test
```
