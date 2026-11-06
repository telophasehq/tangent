<p align="center">
  <img src="https://github.com/user-attachments/assets/7da02584-0a4e-4e41-af6d-61cb081029c4" alt="Tangent logo" height=170>
</p>
<h1 align="center">Tangent</h1>
<p align="center"><em>A model reads every audit log line next to what its actor did before. You read only what it flags.</em></p>

<p align="center">
  <a href="https://github.com/EthanBlackburn/tangent/actions">
  <img height="20" src="https://img.shields.io/github/actions/workflow/status/EthanBlackburn/tangent/scaffold_test.yml?label=build&logo=github" alt="Build status"></a>
</p>

<p align="center">
  <a href="docs/README.md">Docs</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://discord.gg/ZUHB3BRa8c">Discord</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/EthanBlackburn/tangent/issues/new">Issues</a>
</p>

Tangent is one open-source process. It reads your AWS CloudTrail, GitHub,
Okta, Google Workspace and product audit logs, groups each actor's records,
and gives them to a model together with that actor's history: how long it
has been seen, what it has never done before, its usual hours, and its
person's sign-ins in every source. The pipeline computes that history from
its own state, so the model only reads it, and the model can answer nothing
but a pass, or a flag with its reason. On Claude Haiku 5.5 a person costs
about a cent a day in each source that sees them, and CloudTrail $2.60–26 a
day for a 50-person startup (estimates, [below](#what-it-costs)). By default
the model runs on your own machine, and every number below says where it was
measured, including where it loses.

## It caught this

Two lines from a product's audit log, six minutes apart at 2 a.m. Neither
looks wrong on its own. They are tangent-eval's
[`identity/role-change-then-privileged-action`](https://github.com/EthanBlackburn/tangent-eval/blob/main/cases/identity/role-change-then-privileged-action/case.yaml)
case, set inside 13 months of a fictional company's ordinary traffic:

```json
{"ts":"2025-04-19T02:13:00Z","source":"app","event":"member.role.changed","actor":"dave@acme.io","ip":"73.162.14.9","org":"acme","target":"carol@acme.io","from_role":"viewer","to_role":"admin"}
{"ts":"2025-04-19T02:19:00Z","source":"app","event":"org.export.started","actor":"carol@acme.io","ip":"91.108.4.12","org":"acme","scope":"all_data"}
```

Before a model sees carol's line, tangent writes her history on it as
`_baseline`. The pipeline computed it from the 10,132 product-log records
before it; no model wrote it (four of its seven keys):

```
seen   seen 207.4 days (since 2024-09-23), 529 events on 150 days, from app
new    network: ASN 62041 Telegram Messenger Inc (new to org), country NL (new to org), net 91.108.4.0/24 (new to org)
time   window 02 UTC active on 0 of its 20 active days in 28d; usual 15-20 UTC; last active 5h before
auth   last 72h, person carol@acme.io: 3 logins (app 3), 0 failed; grant role viewer -> admin by dave@acme.io 6m before from 73.162.14.9; last login app sso 9h before from 73.162.14.9 ASN 7922 US, mfa yes
```

The model can answer only `{"flag": false}` or
`{"flag": true, "why": "<at most 400 characters>"}`
([judge.schema.json](packs/core/judge.schema.json)), and its whole brief is
[seven short paragraphs](packs/core/judge.md). Any other reply becomes an
error, and errors reach a person too. Its answer for carol's line, from
`alerts.ndjson`:

```json
{"flag": true, "why": "The actor is using a Telegram Messenger IP address (91.108.4.12) for an org export, which is unusual for this human user and organization."}
```

It passed dave's promotion of her: his first, at an hour he had not been
active in 28 days, made from acme's office address. acme's profile watches
both kinds. And its why does not mention the promotion, which
`_baseline.auth` put on carol's line and which the case wants checked first.

A rule could catch this, if you kept what the rule needs: every network your
org has used, each person's usual hours, and who changed whose role, in every
source. Tangent keeps that for every actor and writes it on the record. The
model reads it, so you don't write the rule.

| | model | cost |
|---|---|---|
| this alert | GLM-4.7-Flash, the free default, on `llama-server` on one Mac Studio (M4 Max), at temperature 0 (the demo sets it; core ships 0.7): the same two verdicts in each of five runs | 2 requests, **$0** |
| the eval corpus | Claude Haiku 5.5 on Bedrock: **45 of 45 attacks caught in each of four runs**; 0.8–0.9% of 6,587 ordinary records flagged; **10–11 of 16 look-alikes flagged anyway** | 921 requests, about 7M tokens in, **about $1 a run** ([RESULTS.md](https://github.com/EthanBlackburn/tangent-eval/blob/main/RESULTS.md)) |
| your logs | Claude Haiku 5.5 | **$15 per million lines + $12 per GB**, an estimate ([What it costs](#what-it-costs)) |

**Before you believe it**

- The free local model wrote this alert. It has not been measured on the
  corpus; the measured model is Haiku 5.5.
- Haiku's numbers come from the layout before this one (a `_hist` header,
  since replaced by `_baseline`), and they are in-sample: the brief was
  revised on these same cases.
- Alerts a day for a real company have not been measured. Flags are per
  record, one actor per request, with no incident grouping yet.

**Check it yourself:** [examples/demo](examples/demo) replays this case on
your machine for free ([Try it](#try-it)). In
[tangent-eval](https://github.com/EthanBlackburn/tangent-eval), `make` runs
the whole corpus on Haiku 5.5, about $1 and 11 minutes on the layout before
this one, or free on a local model in well over an hour.

## What it costs

With Claude Haiku 5.5 as the judge ($0.10 a million tokens in, $0.50 out,
Anthropic's price for prompts up to 100K tokens; Bedrock bills on its own
price list), a day of logs costs about

```
$15 for each million logs  +  $12 for each GB of them
```

A log reaches the model as a trimmed logfmt line, about a token for every 9
bytes, plus its share of the prompt, of `_norm`, `_facts` and `_baseline`,
and an answer: about 11 tokens to pass, 150 to flag. So the count matters
more than the size:

| logs judged a day | 250 B each | 500 B | 1 KB | 2 KB |
|---|---|---|---|---|
| 100k | $1.80 | $2.10 | $2.70 | $3.90 |
| 1M | $18 | $21 | $27 | $39 |
| 10M | $180 | $210 | $270 | $390 |

A request is one actor's records: up to 30 new ones and its 10 latest again
as context. A busy actor is judged as 40 logs for every 30; a person as their
logs plus ten for each hour they are active. For a company of 50 people,
each active about eight hours a day:

| source | a day | judged as | Haiku 5.5, a day |
|---|---|---|---|
| [aws](packs/aws) (CloudTrail) | 50k–500k events, ~2 KB | 67k–667k logs | $2.60–26 |
| [app](packs/app) (your product) | 0.6M–6M lines, ~0.5 KB | 0.8M–8M logs | $17–170 |
| [google_workspace](packs/google_workspace) | 5k–25k events, ~0.5 KB | 9k–34k logs | $0.19–0.70 |
| [okta](packs/okta) | 1k–5k events, ~2 KB | 5k–9k logs | $0.20–0.35 |
| [github](packs/github) | 1k–10k events, ~0.3 KB | 5k–14k logs | $0.09–0.26 |

A person costs about a cent a day or less in each source that sees them.
Send your product's audit log, not its request log: the app row is traffic,
and it is the one that grows past a hundred dollars. It gets expensive where
the volume is machines: a trail that also logs data events (S3 objects,
Lambda invocations) has 10 to 100 times aws's events, and AWS VPC Flow Logs,
5M–30M records a day, would be $80–500 a day, which is why they have no pack.

At 2–10 s a request, core's `concurrency: 4` keeps up with 35k–170k requests
a day, 1.4M–6.8M logs judged. A local model, the default, costs only the
machine it runs on. These are estimates from sampled log sizes and typical
startup volumes; [docs/cost.md](docs/cost.md) has how they were made, the
lake's and state's size, and the arithmetic.

## Try it

**The demo**: one case, free, nothing to sign up for.

```bash
cargo install --path crates/cli --locked      # tangent from this checkout
llama-server -hf unsloth/GLM-4.7-Flash-GGUF:Q4_K_M --port 8080 -np 1 -c 32768 -ngl 99 -fa on --jinja   # the judge, in another shell; an 18 GB download
make -C examples/demo PLUGINS=compile
```

[examples/demo](examples/demo) says what it does and what you should see. A
hosted model instead of a local one is `JUDGE_URL`, `JUDGE_MODEL` and
`JUDGE_API_KEY`, for any OpenAI-compatible endpoint; core's judge also sends
the local model's sampling settings, which some hosted models refuse
([Not done yet](#not-done-yet)).

**Your own logs**, in [examples/cloud-infra](examples/cloud-infra): every pack
for one company.

```bash
cd examples/cloud-infra
make setup                     # GeoLite2, cloud ranges, Tor exits
$EDITOR profile/profile.yaml   # who your people, services, networks and keys are
make test PLUGINS=compile      # every pack's fixtures; no model
make run PLUGINS=compile       # export the sources' credentials first (the Makefile lists them)
```

Each source wakes up when its credential is set; anything else comes in as
NDJSON on `:9400`. Alerts land in `alerts.ndjson`, every verdict in
`verdicts.ndjson`, every record in the Parquet lake, and on Slack if you add
the sink ([packs/core](packs/core/README.md#slack)).
[examples/app-log](examples/app-log) is the same for a product's logs, one
org per customer.

`PLUGINS=compile` builds the packs' plugins from source, which needs the Rust
WASM toolchain (`make compile` installs it). It is needed until a release
carries the plugins: v0.1.9, the latest, came before the packs.

**On Bedrock**, in your own AWS account: `backend: bedrock` takes the AWS
credential chain ([docs/inference.md](docs/inference.md#backends),
[examples/inference-bedrock](examples/inference-bedrock)).
[tangent-eval's harness/maincfg.py](https://github.com/EthanBlackburn/tangent-eval/blob/main/harness/maincfg.py)
writes core's judge for Claude Haiku 5.5 on Bedrock, the measured setup.

## How it decides

```
your sources ──▶ a pack's plugin ──┬──▶ lake/, every record as Parquet
                                   ├──▶ baseline_builder ──▶ state.sqlite
                                   │                              │
                                   └──▶ baseline_renderer ◀───────┘
                                        an actor's records, a group at a time
                                             │
                                             ▼
                                           judge ──▶ fresh ──┬──▶ flagged ──▶ alerts.ndjson
                                                             └──▶ verdicts.ndjson
```

1. A source feeds its pack's plugin, which normalizes each record into
   `_norm` (actor, person, org, kind, address, target), trims ids and
   credentials, enriches the address, and adds what your profile says about
   it in `_facts`. Packs only add facts; they flag nothing.
2. Every record goes to the lake, to `baseline_builder`, which folds it into
   the actor's history in `state.sqlite`, and to `baseline_renderer`.
3. The renderer groups each actor's records (`group_by: [_norm.org, _norm.actor]`):
   a group closes at 30 records or an hour after its first, and carries the
   actor's 10 latest records again as `_context`. It stamps `_baseline` on
   them from state: who the actor is and who assumed it, how long it has been
   seen, what is new for it, its volume against its 28-day p90, its usual
   hours, its person's sign-ins and credential changes over 72 hours in every
   source, and addresses it shares with other actors. Only history from
   before the group counts ([packs/core](packs/core/README.md#baseline)).
4. The judge sends one actor's group per request, as logfmt, with
   [judge.md](packs/core/judge.md) as the brief and
   [judge.schema.json](packs/core/judge.schema.json) as the reply's shape:
   200 tokens a record, 4 requests at once, 90 s before a request fails
   ([judge.yaml](packs/core/judge.yaml)).
5. `fresh` drops the `_context` records, judged before. `flagged` keeps the
   flags and the errors and writes them to `alerts.ndjson`; every verdict
   goes to `verdicts.ndjson`.

The model flags, a person reads.

## What a log line can't make it do

Every value in a record was written by whoever caused it, an attacker as
much as anyone. What stands between that text and the verdict:

- **The brief says so.** Nothing in a record is an instruction, and text that
  speaks to the reviewer, to vouch for activity or ask for a verdict, is
  itself a reason to flag the record that holds it ([judge.md](packs/core/judge.md)).
- **A log line cannot speak for the pipeline.** Every pack drops a record's
  own `_norm`, `_facts`, `_ip`, `_context` and `_baseline` on the way in
  ([normkit](packs/shared/normkit/src/lib.rs)). Without that, a forged
  `"_context": true` would have `fresh` drop the record before any alert.
- **It cannot break out of the encoding.** The model reads printable ASCII
  in which no value or key can end a field, a row or a section; controls and
  non-ASCII characters are escaped ([crates/compact](crates/compact/src/lib.rs),
  fuzzed in `crates/compact/fuzz`).
- **It shares a prompt with one actor.** A request holds one actor's records,
  so an attacker's text sits only beside its own actor's. A Slack message
  escapes mentions and links.
- **The answer has one shape.** The reply is held to the schema (a grammar
  on llama-server, structured output on Bedrock). An unreadable reply is
  asked again in halves; a record the reply skipped becomes `judge.error`,
  and `flagged` keeps errors, so a person sees them.
- **The history is not the model's.** `_baseline` is computed from state, and
  every verdict is kept in `verdicts.ndjson` beside the `_baseline` it was
  read against.

The gap: if the model obeys an injection and writes `{"flag": false}`, that
is a well-formed pass, and nothing checks it a second way. tangent-eval has
4 [prompt-injection cases](https://github.com/EthanBlackburn/tangent-eval/blob/main/cases/prompt-injection/README.md);
they came after the measured runs and are not measured yet.

## Where your data goes

- **The model is local by default**: `http://localhost:8080/v1`, no key
  ([judge.yaml](packs/core/judge.yaml)). The enrichment tables are files on
  the machine, the lake is a directory of Parquet files, and state is one
  SQLite file.
- **A hosted model is your choice**: Bedrock in your own AWS account (the
  measured setup), or any OpenAI-compatible endpoint. `fields` limits what
  it is shown ([docs/inference.md](docs/inference.md#settings)).
- **Packs drop credentials before anything is kept or judged**: issued
  credentials, session tokens and secret keys in CloudTrail
  ([trim.rs](packs/aws/src/trim.rs)), and URL signatures in every source.
- **What uses the network**: the sources polling their providers' APIs; the
  model, if hosted; Slack, if you add it; `examples/enrich/setup.sh`
  downloading GeoLite2 and the cloud ranges; `tangent plugin fetch`. Two
  listeners: metrics on `0.0.0.0:9184`, and cloud-infra's NDJSON source on
  `0.0.0.0:9400`, which takes records from anyone who can reach it; bind it
  to `127.0.0.1` or firewall it.
- **Plugins run in a WASM sandbox**: no filesystem but a read-only `/data`,
  no sockets. Two things it does not hold back yet: a plugin sees the
  process's environment, secrets included, and it can make HTTP calls to any
  host through the runtime's `remote` interface, on unless
  `runtime.disable_remote_calls: true`. No pack uses either
  ([docs/plugins.md](docs/plugins.md#what-a-plugin-can-reach)).
- **Prebuilt plugins are attested**: `tangent plugin fetch` keeps a plugin
  only if GitHub's attestation says this repository's release workflow built
  it from the release's tag, with no flag to skip the check
  ([docs/prebuilt-plugins.md](docs/prebuilt-plugins.md)). An attestation
  proves who built a plugin, not what it can reach.

## How well it works, and where it loses

[tangent-eval](https://github.com/EthanBlackburn/tangent-eval) is the
corpus: 66 cases (49 that should flag, 17 that must not) in 10 categories,
from cloud, identity, GitHub and supply chain to AI agents and prompt
injection; Datadog's 35 recorded Stratus Red Team techniques, each with a
control; and, with the macOS pack ([#85](https://github.com/EthanBlackburn/tangent/pull/85)),
90 synthetic laptop cases. Every case sits inside 13 months of a fictional
company's ordinary traffic, 207,886 records, so the judge sees it against
the history of the people and systems involved.

What has been measured: Claude Haiku 5.5 on Bedrock, 201 cases, four runs on
2026-10-31 and 11-01, two without a history header and two with one
([RESULTS.md](https://github.com/EthanBlackburn/tangent-eval/blob/main/RESULTS.md)):

| | without the header | with it |
|---|---|---|
| our 45 attacks caught | 45/45, 45/45 | 45/45, 45/45 |
| ordinary records flagged, of 6,587 | 1.9%, 2.0% | 0.8%, 0.9% |
| run-to-run agreement, per record | 98.2% | 99.3% |
| cost per run | about $0.98 | about $1.02 |

Where it loses: of our 16 must-not-flag cases, the ones written to look
alarming and be fine, it flagged 10 or 11 in every run. And what these
numbers cannot say:

- They were measured on the layout before this one, with `_hist`, which
  `_baseline` has since replaced; this layout has not been run yet.
- They are in-sample: the brief and the profile were revised on these cases.
- The Stratus numbers are withdrawn: the splice could be told from acme's
  traffic by things that had nothing to do with the attack.
- The local default model has never been measured, nor the 4 injection cases.
- Nothing here estimates alerts a day for a real company: only the hours
  around cases are judged, and acme has nine people.

To run it: `make` in tangent-eval (Haiku 5.5 on Bedrock, about $1), or
`make JUDGE=local`, free and well over an hour.

## Not done yet

- **No spend cap.** Spend is bounded in rate (4 requests in flight, 200 tokens
  a record), not in total, and the runtime's metrics count no requests,
  tokens or errors. An unreadable reply, asked again in halves, can take 79
  requests for a 40-record group at worst.
- **No provenance on a verdict.** It carries the model's answer and nothing
  else: no model id, prompt hash or request id, and the request's text and
  token use are not logged.
- **Alerts are per record.** No grouping into incidents, and no way to mark
  one as fine.
- **Per-person monitoring, by design.** Each person's records come with their
  sign-ins across sources, their hours and their volume. Names, emails and
  addresses reach the model as they are, no retention is enforced, and
  nothing controls who reads `alerts.ndjson`. Keep the model local, or know
  what your provider keeps.
- **A stop or a crash loses the open groups' verdicts.** On a stop
  (SIGTERM) the open groups close and go to the judge, but the signal ends
  the model calls under way, so those records are never judged; a crash
  loses the groups outright. Either way that is up to an hour of each active
  actor's records unjudged. They are in the lake.
- **Core's judge is set for the local model.** `judge.yaml` sends
  `temperature: 0.7`, `top_p` and `chat_template_kwargs`, which Haiku 5.5
  does not take, and which other hosted models may refuse. An include can
  null `extra`'s keys but not drop them, so a hosted judge means overriding
  `extra` from two files, or writing the node out as tangent-eval does.
- **No release carries the packs yet**, so every path above compiles them.

## What it reads

| pack | logs |
|---|---|
| [aws](packs/aws) | CloudTrail, read as a trail's files through SQS, organization trails included; adds `root` and `world_open` |
| [github](packs/github) | the organization audit log (Enterprise Cloud, `read:audit_log`), Actions job logs and runner egress |
| [okta](packs/okta) | the System Log, with Okta's own risk signals |
| [google_workspace](packs/google_workspace) | login, token, Drive and admin events, through a service account with domain-wide delegation |
| [app](packs/app) | your product's audit log, with the customer's tenant as the org, and an AI agent's run log |
| macOS laptops (eslogger) | in review: [#85](https://github.com/EthanBlackburn/tangent/pull/85), with a 90-case suite waiting |
| VPC Flow Logs | no pack ([What it costs](#what-it-costs)) |

[packs/README.md](packs/README.md) says what every pack does to a record, how
to put packs in one config, and how to write one;
[packs/core](packs/core/README.md) has the record, the profile's keys and the
judge's settings.

## Install

Until the next release, build tangent from this checkout:

```bash
cargo install --path crates/cli --locked
cargo install --git https://github.com/EthanBlackburn/tangent tangent-cli --locked   # or without a checkout, from main
```

The release installers (the shell script on the
[releases page](https://github.com/EthanBlackburn/tangent/releases), the
Homebrew tap `telophasehq/telophase`, the Docker image
`ghcr.io/telophasehq/tangent-toolchain`) still serve 0.1.9, which predates
`include:`, `group_by`, the packs and the edges these docs use.

## The runtime underneath

The packs are configs over a general stream processor whose nodes are
sources, plugins, inference nodes and sinks, named in one YAML file and joined
by edges (`from: cloudtrail`, `to: [aws, lake]`), which `tangent run` checks
before it starts. One process runs it all.

- **Sources**: `tcp`, `socket`, `file`, `sqs`, `msk`, `sqlite`, the API
  pollers `cloudtrail`, `github_audit`, `okta` and `google_workspace`, and
  `github_webhook` and `npm_registry` ([docs/sources.md](docs/sources.md)).
- **Plugins** in Rust, Go or Python, compiled to WASM: `tangent plugin
  scaffold`, `compile`, `test`, `fetch`, and `tangent bench`. Any plugin can
  take its records a group at a time with `group_by`, one group per key,
  closed by count, idle time or age ([docs/plugins.md](docs/plugins.md)).
- **Inference nodes**: one answer per record from any OpenAI-compatible model
  or Bedrock, as JSON lines or logfmt, with a schema, concurrency, a timeout
  and retries; a node never drops a record ([docs/inference.md](docs/inference.md)).
- **Sinks**: `file`, `rolling_file` (Parquet or NDJSON through a write-ahead
  log: an acked write survives a crash of tangent), `s3`, `sqlite`, `slack`,
  `blackhole` ([docs/sinks.md](docs/sinks.md)).
- **State**: SQLite or an in-memory LRU, with TTLs, transactions, and
  `tangent state` to read it while it runs ([docs/state.md](docs/state.md)).
- **Enrichment tables**: MaxMind DBs, CIDR lists and YAML documents, loaded
  once per process; a lookup takes tens of nanoseconds to about a
  microsecond ([docs/enrichment-tables.md](docs/enrichment-tables.md)).
- **One config from many files**: `include:`, `${VAR:-default}`, and
  `tangent config` to print the merged result with secrets left out
  ([docs/configuration.md](docs/configuration.md)).

| tcp → blackhole | Rust | Go | Python |
|---|---|---|---|
| throughput | 483.35 MB/s | 399.54 MB/s | 76.91 MB/s |

_Measured in November 2025 on a dedicated 16-core AMD machine, one simple
transformation plugin each, with `tangent run` and `tangent bench` pinned to
separate cores ([docs/benchmarks.md](docs/benchmarks.md))._

## License

Apache-2.0. See [LICENSE](LICENSE).
