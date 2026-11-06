# What it costs

What a pack costs to run is what its judge's model charges: tangent moves
hundreds of MB a second ([benchmarks](benchmarks.md)), and a model is paid by
the token. A local model, the default, costs only the machine it runs on.
With Claude Haiku 5.5 as the judge ($0.10 a million tokens in, $0.50 out,
Anthropic's price for prompts up to 100K tokens; Bedrock bills on its own
price list), a day of logs costs about

```
$15 for each million logs  +  $12 for each GB of them
```

| logs judged a day | 250 B each | 500 B | 1 KB | 2 KB |
|---|---|---|---|---|
| 100k | $1.80 | $2.10 | $2.70 | $3.90 |
| 1M | $18 | $21 | $27 | $39 |
| 10M | $180 | $210 | $270 | $390 |

A month is thirty times as much. The count matters more than the size. A log
reaches the model as a trimmed logfmt line, about a token for every 9 bytes
it had, but each one also carries its `_norm` and `_facts`, a share of the
prompt and of `_baseline`, and gets an answer (about 11 tokens to pass, 150
to flag): below 1 KB, that is most of what a log costs. The rule was fitted
to private samples of real CloudTrail, GitHub and Google Workspace logs
([below](#how-these-were-made)), and overstates Okta's, which trim further.

## What the count includes

- **Context.** A request is one actor's logs: up to thirty new ones, and its
  latest ten already judged again (`_context`), which the model answers too
  ([packs/core/judge.yaml](../packs/core/judge.yaml): `max_events: 30`,
  `history: 10`). A busy actor, a service role or an app server, fills one
  with new logs, so it is judged as 40 for every 30. A quiet one, a person,
  goes an hour after its first new log. Count a person as their logs plus ten
  for each hour they are active (or for each thirty of their logs, when that
  is more).
- **Throughput.** At 2–10 s a Haiku 5.5 request (an estimate, not measured
  here), core's `concurrency: 4` keeps up with 35k–170k requests a day
  (86,400 s × 4 ÷ 10 s, and ÷ 2 s), 1.4M–6.8M logs judged, 1M–5.2M of them
  new; raise `inference.judge.concurrency` past that. A local model on one
  machine answers one request at a time (`-np 1`), at about five seconds a
  request ([tangent-eval's README](https://github.com/EthanBlackburn/tangent-eval#run-it)):
  about 17k requests a day, at most 0.7M logs. That covers the people
  sources and the low end of aws's row below, not its top or the app row;
  past it, use a hosted model or more model servers.

## Per provider

Rough ranges for a company of 50 people, each active about eight hours a day
(400 person-hours), priced by the rule above:

| pack | grows with | a log | a day | judged as | Haiku 5.5, a day |
|---|---|---|---|---|---|
| [aws](../packs/aws) | services, scheduled jobs and deploys, more than people | ~2 KB | 50k–500k events, 0.1–1 GB | 67k–667k logs (40 for every 30) | $2.60–26 |
| [app](../packs/app) | traffic, background jobs and log level | 0.2–1 KB | 0.6M–6M lines, 0.3–3 GB | 0.8M–8M logs (40 for every 30) | $17–170 at 0.5 KB |
| [google_workspace](../packs/google_workspace) | people: 100–500 events each, most of them Drive | ~0.5 KB | 5k–25k events, 3–13 MB | 9k–34k logs (ten for each of 400 to 850 requests) | $0.19–0.70 |
| [okta](../packs/okta) | people: 20–100 events each, sign-ins, MFA and app launches | ~2 KB | 1k–5k events, 2–10 MB | 5k–9k logs (ten for each person-hour) | $0.20–0.35 |
| [github](../packs/github) | engineers: 20–200 events each, pushes, reviews and Actions runs | ~0.3 KB | 1k–10k events, 0.3–3 MB | 5k–14k logs (ten for each person-hour) | $0.09–0.26 |

A person costs about a cent a day or less in each source that sees them,
however little they do. At 10 people, Okta, GitHub and Google Workspace come to
$0.10–0.26 a day; aws grows with services more than people, and if a
10-person startup's trail writes a fifth of the events above (10k–100k a
day), it adds $0.52–5.20, about $1–5 a day in all. Send your product's audit log, not its request log:
the app row is traffic, and it is the one that grows past a hundred dollars.
A trail that also logs data events (S3 objects, Lambda invocations) can have
ten to a hundred times aws's events. AWS VPC Flow Logs have no pack: a
startup writes 5M–30M flow records a day of about 0.1 KB, $80–500 a day by
the rule for the flows alone and more than one judge keeps up with, so a pack
for them would roll flows up before the judge.

## Storage

Core keeps every finished log once in its lake, Parquet files in a directory
an hour, about 20 bytes a log on a day of CloudTrail (a private sample),
which deleting old hours trims; the judge's groups wait in memory;
`state.sqlite` grows with actors, their active hours, role sessions and
sign-ins, and each actor's ten latest records the judge saw, not with logs.

## How these were made

These are estimates. Log sizes and token counts were measured on private
samples of real logs (CloudTrail on 20k events, the others on tens to
hundreds), not on anything in this repository or tangent-eval, with Claude
Haiku 4.5's token count scaled to 5.5's; the volumes are typical of
startups, not yours: a day of your own logs in the lake says what yours are.
One public check on the sizes: Datadog's Stratus Red Team recordings in
[tangent-eval](https://github.com/EthanBlackburn/tangent-eval/tree/main/stratus/logs),
310 CloudTrail events Datadog recorded in its own test accounts, average
1.3 KB as compact JSON (median 1.0 KB), under the ~2 KB the aws row
assumes.

The rule's two terms: the GB term is a token for every 9 bytes at $0.10 a
million tokens in, about $11 a GB, which the rule rounds to $12. The per-log term is what a
log costs whatever its size: its `_norm` and `_facts`, its share of the
prompt and of `_baseline`, and its answer at $0.50 a million out (11 tokens
to pass is $5.50 a million alone); it was fitted to the samples above, so it
cannot be rederived from public data.

One measured cost, which the rule comes in under: the corpus the packs are
evaluated on, synthetic logs of a few hundred bytes that repeat less than
real ones, came to $0.98 for 921 requests on Haiku 5.5 on Bedrock
([tangent-eval's RESULTS.md](https://github.com/EthanBlackburn/tangent-eval/blob/main/RESULTS.md)),
measured through an earlier harness and plugin set. A request holds at most
40 records, context included, so that is at least $27 a million records
read (921 × 40 is the most it could have read); the rule says about $19 a
million for logs of 300 bytes. Until this layout is measured, read the
tables as estimates that may be low.

Per request, logfmt keeps an actor's forty records at a median of 6,817
tokens, `_baseline` included, against 20,262 as JSON lines
([packs/core](../packs/core/README.md#judgeyaml) has the table).
