# cloud-infra

Your own infrastructure's logs, every record kept; a model reads the ones that
matter in their context, in batches, and a person reads what it flags. Install,
point your logs at it, fill in one file about your company, run.

```
CloudTrail ──▶ cloudtrail ─┐
GitHub audit log ──▶ github_audit ─┤
Okta ──▶ okta ─┼──▶ enrich ──▶ facts ──▶ store          (ingest)
Google Workspace ──▶ google_workspace ─┤
CI, anything else (TCP) ──▶ norm ─┘

store ──▶ buffer ──▶ novel ──▶ judge ──▶ flagged ──▶ alerts.ndjson, Slack   (judgment)
```

- **A plugin per provider** ([providers/](providers/)) reads its source's records: it puts the shape into `_norm.{actor, org, kind, ip, country, dest, target}` and trims the record to what it means, dropping the provider's ids, hashes, protocol details and bulk, so the judge and the retained history carry a record a reader would keep. **norm** does the `_norm` half for anything else arriving over TCP (CI lines, a product's logs) and leaves those records as they came.
- **enrich** adds what is known about the address: country, ASN and organization, cloud or datacenter, Tor exit, VPN, one of yours. From files on disk; nothing leaves the machine ([examples/enrich](/examples/enrich)).
- **facts** writes what your profile knows about the record, and decides nothing: whether the actor is a member, a service, a key or nobody known; whether the address, destination and target are usual; which kinds you watch. Every record goes on.
- **store** keeps every normalized, enriched record under its partition for 30 days, and marks a record `_novel` when it has something its actor or the organization has not had in that time: for the actor, a kind, a network (`_facts.ip.org`), a country, a destination, an AWS region or an error code; for the organization, an actor, a destination, a kind or a target. For an hour after an actor's new value, its next records are marked too, at most 50 (`follow`), so the rest of the story is read with it. That ends the ingest pipeline: a source is acknowledged when its records are kept, and nothing waits on a model.
- **buffer** reads the store back five minutes of one partition's activity at a time, once the stream has moved twenty minutes past them (a quiet partition closes on the clock), so a verdict comes about twenty-five minutes after its record. Where each partition stands lives in the store too, so a restart catches up what fell due while it was down.
- **novel** passes what the judge reads: a record that is new or follows a new value, and one the profile says to see every time, however usual (a watched kind, root, a rule opened to `0.0.0.0/0`, a sensitive target, a key off its networks). AWS acting on its own (`aws:<service>`, a service's own calls, as the cloudtrail plugin marks them) is not judged. It takes `_novel` off what it passes: being new chooses what the model reads, and the model judges a record on what it is and its history. Everything else is kept, and is history for the judge, but is never itself judged.
- **judge** reads what novel passes from each buffer together, with the history before it (each actor's latest records and the org's, however long ago) read once from the store, and writes a verdict per record: a pass is the flag alone, a flag says why. No rule decides a verdict; the profile is context, not a rulebook.
- The judge is any OpenAI-compatible model with [judge.md](judge.md) as its brief; **flagged** keeps the verdicts a person should see, for `alerts.ndjson` and, with a `slack` sink, a channel:

  ```yaml
  sinks:
    slack:
      type: slack
      webhook_url: ${SLACK_WEBHOOK_URL}     # an incoming webhook's URL
      text: "*{_norm.actor}* {_norm.kind} from {_norm.ip} ({_facts.ip.org}): {judge.why}"
  dag:
    - from: { kind: plugin, name: flagged }
      to: [{ kind: sink, name: alerts }, { kind: sink, name: slack }]
  ```

Novelty cannot see a stolen credential used from a network its owner already uses, for things its owner already does: a CI role's session taken from one GitHub-hosted runner and used from another, or an instance's role credentials used from another instance in the same cloud and region. The actor, kind, network, country and region are all familiar, so nothing is new, and unless the profile says to see that kind or target every time, the judge never reads it. Keep GuardDuty on for that.

On the 34 cases in [cases/](cases/) plus 38 days of ordinary traffic, the judge's recall and false-positive rate are in [RESULTS](#what-it-catches).

## Install and run

```bash
make setup          # Rust WASM toolchain; GeoLite2, cloud ranges, Tor exits into examples/enrich/data
$EDITOR profile/profile.yaml
make test           # the cases through the provider plugins, norm, enrich, facts and novel; no model needed
make run            # listens on :9400 for NDJSON records
```

The judge defaults to a local model on `:8080`. Serve it with llama.cpp's `llama-server`, which turns the verdict schema into a grammar: replies are always well-formed and their reasons stay short. `mlx_lm.server` accepts the schema and ignores it, and with this model about one reply in five came back unreadable.

```bash
llama-server -m GLM-4.7-Flash-Q4_K.gguf --port 8080 -np 1 -c 32768 -ngl 99 -fa on --jinja --temp 0.7 --top-p 0.95
```

For a hosted one:

```bash
JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
```

### Getting logs in

Four sources read their APIs directly, with nothing in between. Each wakes up when its credential is exported (the Makefile lists the variables) and, left empty, says so once and stays idle.

| source | export | what it reads |
|---|---|---|
| `cloudtrail` | AWS credentials: `AWS_PROFILE`, or the SDK's default chain; `AWS_REGION`, the queue's; `CLOUDTRAIL_QUEUE_URL`, the SQS queue told of each file a trail writes (see below) | a trail's files: every event, data events included, every account and region of an organization trail, a few minutes behind. There is no polling of `LookupEvents`: a poller's own calls are CloudTrail events, and it reads them back |
| `github_audit` | `GITHUB_ORG`, `GITHUB_TOKEN` with `read:audit_log` (the audit log API is a GitHub Enterprise Cloud feature) | the organization's audit log, web and git events |
| `okta` | `OKTA_ORG_URL` (`https://acme.okta.com`), `OKTA_API_TOKEN` from Security → API → Tokens, made by a read-only administrator | the System Log |
| `google_workspace` | `GOOGLE_APPLICATION_CREDENTIALS`, a service account key file with domain-wide delegation for the scope `https://www.googleapis.com/auth/admin.reports.audit.readonly`; `GOOGLE_ADMIN_EMAIL`, the administrator it acts as (the Admin SDK API enabled on the project) | login, token, drive and admin activity |

GitHub, Okta and Workspace keep their place in the runtime's state store (the `sources` namespace), so a restart resumes where it stopped; on first start each reads the last hour (`since`). CloudTrail's place is its queue: a message is deleted once its file's records are kept. Records arrive as the APIs write them, plus `source` and `ts`; norm knows the four shapes.

Anything else (CI logs, runner egress, a forwarder for a source not listed) is one JSON line per record on `:9400`, with a `source` field naming its shape (`gha_ci`, `runner_egress`, or one of the four above) and a `ts`.

#### An organization trail

One trail can record every account and region in the organization into one bucket. The source reads it through an SQS queue that is told of each file the trail writes. Any of these notifications works:

- the trail's own SNS notifications, with the queue subscribed to the topic;
- S3 event notifications on the bucket, straight to the queue or through SNS;
- EventBridge's `Object Created` for the bucket, with the queue as the rule's target.

With Control Tower, the trail already notifies the audit account's `aws-controltower-AllConfigNotifications` topic, and its bucket is the log archive account's. Subscribe a queue in the audit account to the topic, and set `CLOUDTRAIL_BUCKET_ROLE` to a role in the log archive account that can read the bucket. AWS Config's messages share the topic and are skipped, as are digests and the bucket's other files.

The source's identity needs `sqs:ReceiveMessage` and `sqs:DeleteMessage` on the queue, and `sts:AssumeRole` on the bucket role. The bucket role, or the identity itself when the bucket is its own account's, needs `s3:GetObject` on the trail's prefix, and `kms:Decrypt` if the trail encrypts its files. A message is deleted once its records are delivered. One whose file cannot be read stays on the queue and comes back in five minutes, so give the queue a redrive policy to set aside what never succeeds.

Reading the trail makes calls that CloudTrail records too: the source's `AssumeRole` of the bucket role, about once an hour, and KMS `Decrypt` calls when the files are KMS-encrypted. When the trail's S3 data events cover its own bucket, every `GetObject` the source makes is an event as well, which lands in a later file, which is read in turn, and so on for as long as it runs. Leave the trail's bucket out of its data-event selectors.

## The profile

[profile/profile.yaml](profile/profile.yaml) is every fact about your company that no log carries; the models read it as `_facts`:

| key | what it is | what it decides |
|---|---|---|
| `org`, `own_sources` | your name; which sources are your own infrastructure | all of them land in one partition, yours |
| `countries` | where your people are | a person acting from elsewhere is noted for the judge |
| `cloud` | where your services run | a service seen on any other hosting is noted for the judge |
| `members` | people: IdP emails, GitHub handles | anyone else is a principal you do not know |
| `services` | IAM users and roles, bots | exempt from the "person abroad" rules, held to `cloud` |
| `keys` | API keys and the networks each calls from | a key off its networks reaches the judge every time |
| `about` | who an actor is, when its name does not say (`DrataAutopilotRole: "Drata, the compliance vendor: ..."`) | the judge reads it with the actor's records, in place of "nobody you know" |
| `hosts` | hosts your systems talk to (`*.suffix` allowed) | a request anywhere else is noted for the judge |
| `sensitive` | buckets, repos, tables | a touch reaches the judge every time |
| `watch` | kinds you want seen every time (`StopLogging`, `CreateAccessKey`) | each reaches the judge, however usual |

The file ships filled in for the fictional company the cases describe, so `make test` passes before you touch it.

## What it catches

[cases/](cases/) holds 34 sequences as the real sources write them, 28 that should flag and 6 that must not. Half follow disclosed compromises of 2025 and 2026 ([cases/README.md](cases/README.md) has the table and sources): TeamPCP's Trivy and LiteLLM, the axios hijack, nx/s1ngularity, Shai-Hulud's second wave, tj-actions/changed-files, Railway's environment-variable breach, Vercel via Context.ai. The rest are the classics: root login, CloudTrail stopped, a bucket made public, MFA off, impossible travel, a Gmail forwarding rule, a branch protection removed.

Measured 2026-10-28 with Claude Haiku 4.5 (Bedrock) judging **every record** of the 51 cases of both packs inside 38 days of synthetic ordinary traffic, 17,027 records, in batches of about thirty with each batch's history written once as context: recall 38 to 39 of 40 across runs (the typosquatted dependency is always missed, nothing about one `npm ci` line being new; an agent plan with an injected tool result is caught in some runs and not others); 1 of 11 negatives flagged, a key rotation the morning after a vendor's notice, which the judge reads as an admin minting service keys from home; 13 of 16,199 ordinary records flagged, most of them people signing in from an office address the profile did not list as its own. 241 requests of up to 290 records, 16 in flight, under ten minutes, a few dollars. The previous design, a rulebook gate in front of the judge, reached 38/40 and 2/11 while judging 68 records; the two it never reached (a mass push by a personal access token, a typosquat) split, one caught, one still missed.

## Files

```
tangent.yaml          the pipeline
profile/profile.yaml  your company (edit this)
judge.md              the model's brief
providers/ norm/ facts/   the plugins, Rust, compiled to WASM by make setup; providers/common is what they share
cases/                the sequences, one directory each: case.yaml + events.ndjson
tests/                fixtures for make test
```
