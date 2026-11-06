# What it costs

What a pack costs to run is what its judge's model charges: tangent moves
hundreds of MB a second ([benchmarks](benchmarks.md)), and a model is paid by
the token. A local model, the default, costs only the machine it runs on.
With Claude Haiku 5.5 as the judge ($0.10 a million tokens in, $0.50 out),
a day of logs costs about

```
$15 for each million logs  +  $12 for each GB of them
```

| logs a day | 250 B each | 500 B | 1 KB | 2 KB |
|---|---|---|---|---|
| 100k | $1.80 | $2.10 | $2.70 | $3.90 |
| 1M | $18 | $21 | $27 | $39 |
| 10M | $180 | $210 | $270 | $390 |

A month is thirty times as much. The count matters more than the size. A log
reaches the model as a trimmed logfmt line, about a token for every 9 bytes
it had, but each one also carries its `_norm` and `_facts`, a share of the
prompt and of `_baseline`, and gets an answer (about 11 tokens to pass, 150
to flag): below 1 KB, that is most of what a log costs. The rule fits real
CloudTrail, GitHub and Google Workspace logs, and overstates Okta's, which
trim further.

## What the count includes

- **People.** A request is one actor's logs: up to thirty new ones, and its
  latest ten already judged again (`_context`), which the model answers too.
  A busy actor, a service role or an app server, fills one with new logs. A
  quiet one, a person, goes an hour after its first new log. Count a person
  as their logs plus ten for each hour they are active.
- **Throughput.** At 2–10 s a Haiku 5.5 request, core's `concurrency: 4`
  keeps up with 35k–170k requests a day, 1.4M–6.8M logs; raise
  `inference.judge.concurrency` past that.

## Per provider

Rough ranges for a company of 50 people, each active about eight hours a day,
priced by the rule above:

| pack | grows with | a log | a day | judged as | Haiku 5.5, a day |
|---|---|---|---|---|---|
| [aws](../packs/aws) | services, scheduled jobs and deploys, more than people | ~2 KB | 50k–500k events, 0.1–1 GB | the same | $2–20 |
| [app](../packs/app) | traffic, background jobs and log level | 0.2–1 KB | 0.6M–6M lines, 0.3–3 GB | the same | $13–130 at 0.5 KB |
| [google_workspace](../packs/google_workspace) | people: 100–500 events each, most of them Drive | ~0.5 KB | 5k–25k events, 3–13 MB | 16k–32k logs | $0.35–0.70 |
| [okta](../packs/okta) | people: 20–100 events each, sign-ins, MFA and app launches | ~2 KB | 1k–5k events, 2–10 MB | 16k logs | $0.60 |
| [github](../packs/github) | engineers: 20–200 events each, pushes, reviews and Actions runs | ~0.3 KB | 1k–10k events, 0.3–3 MB | 16k logs | $0.30 |

A person costs about a cent a day or less in each source that sees them,
however little they do. A trail that also logs data events (S3 objects, Lambda invocations) can have
ten to a hundred times aws's events. AWS VPC Flow Logs have no pack: a
startup writes 5M–30M flow records a day of about 0.1 KB, $80–500 a day by
the rule and more than one judge keeps up with, so a pack
for them would roll flows up before the judge.

## Storage

Core keeps every finished log once in its lake, Parquet files in a directory
an hour, about 20 bytes a log on a day of CloudTrail, which deleting old
hours trims; the judge's groups wait in memory; `state.sqlite` grows with
actors, their active hours, role sessions and sign-ins, and each actor's ten
latest records the judge saw, not with logs.

## How these were made

These are estimates. Log sizes are measured on samples (CloudTrail on 20k
events, the others on tens to hundreds), tokens with Claude Haiku 4.5's count
scaled to 5.5's, and the volumes are typical of startups, not yours: a day of
your own logs in the lake says what yours are.

The corpus the packs are evaluated on, synthetic logs of a
few hundred bytes that repeat less than real ones, came to $27 a million
(921 requests, $0.98).
