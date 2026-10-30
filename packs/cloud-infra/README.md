# cloud-infra

Your own infrastructure's logs, every record read by a model with the rest
of its actor's recent records; a person reads what it flags. Install, point your
logs at it, fill in one file about your company, run.

```
CloudTrail, GitHub audit log, Okta, Google Workspace, CI ──▶ norm ──▶ enrich ──▶ facts ──▶ logs.sqlite   (ingest.yaml)

logs.sqlite ──▶ actors ──▶ judge ──▶ fresh ──▶ flagged ──▶ alerts.ndjson   (judge.yaml)
                                       └──▶ verdicts.ndjson
```

- **norm** puts every source's shape into one, `_norm.{actor, org, kind, ip, country, dest, target}`, and trims the four API sources' records to what they mean: their ids, hashes, protocol details and bulk go. Anything else is left as it came. AWS acting on its own is the actor `aws:<service>`, named only from fields AWS writes.
- **enrich** adds what is known about the address: country, ASN and organization, cloud or datacenter, Tor exit, VPN, one of yours. From files on disk; nothing leaves the machine ([examples/enrich](/examples/enrich)).
- **facts** writes what your profile knows about the record, and decides nothing: whether the actor is AWS itself, a member, a service, a key or nobody known; whether the address, destination and target are usual; which kinds you watch. Every record goes on.
- **logs** keeps every record once in `logs.sqlite`, at its `ts`, with two partitions: the org, and the org and actor. It keeps 30 days before the newest record. That ends ingest, one process; the judge is another, so nothing in ingest waits on a model.
- **actors** reads `logs.sqlite` back forty records of one actor at a time, oldest first. A quiet actor's go once the oldest is an hour older than the newest record stored, or once nothing new has been stored for an hour, topped up to forty with the actor's latest records already judged, marked `_context`. A record that arrives late waits with the actor's others. Rows are marked once their verdicts are written, and only then do the actor's next go; a restart sends again what the judge had not finished.
- **judge** reads an actor's records together, as **actors** sends them and never two actors in one request, and writes a verdict per record: a pass is the flag alone, a flag says why. It is any OpenAI-compatible model with [judge.md](judge.md) as its brief; **fresh** drops the `_context` records, judged before; **flagged** keeps the verdicts a person should see.

## Install and run

```bash
make setup          # Rust WASM toolchain; GeoLite2, cloud ranges, Tor exits into examples/enrich/data
$EDITOR profile/profile.yaml
make test           # the cases through norm and facts; no model needed
make run            # ingest and the judge, two processes; listens on :9400 for NDJSON records
```

The judge defaults to a local model on `:8080`. Serve it with llama.cpp's `llama-server`, which turns the verdict schema into a grammar: replies are always well-formed and their reasons stay short. `mlx_lm.server` accepts the schema and ignores it, and with this model about one reply in five came back unreadable.

```bash
llama-server -m GLM-4.7-Flash-Q4_K.gguf --port 8080 -np 1 -c 32768 -ngl 99 -fa on --jinja --temp 0.7 --top-p 0.95
```

For a hosted one:

```bash
JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
```

### Slack

What **flagged** keeps can go to a channel too. Uncomment the `slack` sink and its edge in judge.yaml, and export `SLACK_WEBHOOK_URL`, an incoming webhook's URL:

```yaml
sinks:
  slack:
    type: slack
    webhook_url: ${SLACK_WEBHOOK_URL}
    text: "*{_norm.actor}* {_norm.kind} from {_norm.ip}: {judge.why|not judged: judge.error}"
dag:
  - from: { kind: plugin, name: flagged }
    to: [{ kind: sink, name: slack }]
```

In `text`, `{a.b}` is the record's field (`-` when it has none) and `{x|words y}` the first of them it has, so a record the model could not judge says why. A field's value is escaped: a log line cannot mention anyone, link or start a line in the message. A post that fails is logged and dropped.

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

[profile/profile.yaml](profile/profile.yaml) is every fact about your company that no log carries; the model reads it as `_facts`:

| key | what it is | what the record gets |
|---|---|---|
| `org`, `own_sources` | your name; which sources are your own infrastructure | all of them are one org, yours |
| `countries` | where your people are | a note when a person acts from elsewhere |
| `cloud` | where your services run | a note when a service acts from any other hosting |
| `members` | people: IdP emails, GitHub handles | `principal: member`; anyone else is `unknown` |
| `services` | IAM users and roles, bots | `principal: service`, held to `cloud` |
| `keys` | API keys and the networks each calls from | `principal: key`, `key_known_network` |
| `about` | who an actor is, when its name does not say | `about`, in place of the "nobody it knows" note |
| `hosts` | hosts your systems talk to (`*.suffix` allowed) | `dest_known` |
| `sensitive` | buckets, repos, tables | `sensitive_target` |
| `watch` | kinds a person wants seen every time | `watch` |

The file ships filled in for the fictional company the cases describe, so `make test` passes before you touch it.

## What it catches

[cases/](cases/) holds 34 sequences as the real sources write them, 28 that should flag and 6 that must not. Half follow disclosed compromises of 2025 and 2026 ([cases/README.md](cases/README.md) has the table and sources): TeamPCP's Trivy and LiteLLM, the axios hijack, nx/s1ngularity, Shai-Hulud's second wave, tj-actions/changed-files, Railway's environment-variable breach, Vercel via Context.ai. The rest are the classics: root login, CloudTrail stopped, a bucket made public, MFA off, impossible travel, a Gmail forwarding rule, a branch protection removed.

This shape, an actor's latest forty records, is not measured yet. The two before it, over the cases of both packs and 38 days of synthetic ordinary traffic: a judge reading every record in batches with each org's history as context (Claude Haiku 4.5) flagged 38 to 39 of the 40 positives, 1 of 11 negatives and 13 of 16,199 ordinary records; a rulebook gate in front of a local 30B judge, 38 of 40 and 2 of 11.

`make test` runs the cases through norm and facts and compares with [tests/](tests/); it needs no model and runs in CI.

## Files

```
ingest.yaml           the sources into logs.sqlite
judge.yaml            logs.sqlite through the model
profile/profile.yaml  your company (edit this)
judge.md              the model's brief, judge.schema.json the verdict's shape
norm/ facts/ fresh/   the plugins, Rust, compiled to WASM by make setup
cases/                the sequences, one directory each: case.yaml + events.ndjson
tests/                fixtures for make test
```
