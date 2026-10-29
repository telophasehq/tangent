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

Four sources read their APIs directly, with nothing in between. Each wakes up when its credential is exported (the Makefile lists the variables) and, left empty, says so once and stays idle.

| source | export | what it reads |
|---|---|---|
| `cloudtrail` | AWS credentials: `AWS_PROFILE`, or the SDK's default chain; `AWS_REGION`, the queue's; `CLOUDTRAIL_QUEUE_URL`, the SQS queue told of each file a trail writes (see below), without which the source stays idle | a trail's files: every event, data events included, every account and region of an organization trail, a few minutes behind |
| `github_audit` | `GITHUB_ORG`, `GITHUB_TOKEN` with `read:audit_log` (the audit log API is a GitHub Enterprise Cloud feature) | the organization's audit log, web and git events |
| `okta` | `OKTA_ORG_URL` (`https://acme.okta.com`), `OKTA_API_TOKEN` from Security → API → Tokens, made by a read-only administrator | the System Log |
| `google_workspace` | `GOOGLE_APPLICATION_CREDENTIALS`, a service account key file with domain-wide delegation for the scope `https://www.googleapis.com/auth/admin.reports.audit.readonly`; `GOOGLE_ADMIN_EMAIL`, the administrator it acts as (the Admin SDK API enabled on the project) | login, token, drive and admin activity |

GitHub, Okta and Workspace keep their place in the runtime's state store (the `sources` namespace), so a restart resumes where it stopped; on first start each reads the last hour (`since`). CloudTrail's place is its queue: a message is deleted once its file's records are kept. Records arrive as the APIs write them, plus `source` and `ts`; norm knows the four shapes.

Anything else (CI logs, runner egress, a forwarder for a source not listed) is one JSON line per record on `:9400`, with a `source` field naming its shape (`gha_ci`, `runner_egress`, or one of the four above) and a `ts`.

#### An organization trail

One trail can record every account and region in the organization into one bucket. The source reads it through an SQS queue that is told of each file the trail writes. Either of these notifications works:

- the trail's own SNS notifications, with the queue subscribed to the topic;
- S3 event notifications on the bucket, straight to the queue or through SNS.

With Control Tower, the trail already notifies the audit account's `aws-controltower-AllConfigNotifications` topic, and its bucket is the log archive account's. Subscribe a queue in the audit account to the topic, and set `CLOUDTRAIL_BUCKET_ROLE` to a role in the log archive account that can read the bucket. AWS Config's messages share the topic and are skipped, as are digests and the bucket's other files.

The source's identity needs `sqs:ReceiveMessage` and `sqs:DeleteMessage` on the queue, and `sts:AssumeRole` on the bucket role. The bucket role, or the identity itself when the bucket is its own account's, needs `s3:GetObject` on the trail's prefix, and `kms:Decrypt` if the trail encrypts its files. A message is deleted once its records are delivered. One whose file cannot be read stays on the queue and comes back in five minutes, so give the queue a redrive policy to set aside what never succeeds. When the queue cannot be read (an expired session, a permission), the source warns once and retries, at most a minute apart.

Reading the trail makes few calls of its own, but some land back in it: the bucket role's `AssumeRole`, about once an hour; a KMS `Decrypt` for each file when the trail encrypts its files with KMS (fewer with an S3 Bucket Key); and, when the trail logs S3 data events for its own bucket, a `GetObject` for each file, each one an event in a later file. Leave the trail's bucket out of its data-event selectors. With a bucket role, these calls carry the session name `tangent`.

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
