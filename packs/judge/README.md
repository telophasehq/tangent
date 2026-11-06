# judge

Not a provider: a model reads each actor's records a group at a time, with
what [behavior-baseline](../behavior-baseline) knew of the actor before them,
and writes a verdict per record; a person reads the ones it flagged.
[ingest.yaml](ingest.yaml) includes [enrich](../enrich) and
behavior-baseline, so a config that includes it has the tables and the
baseline too, and names nothing more than its packs.

```
your sources ──▶ a pack's plugin ──┬──▶ a sink of yours: lake/, every record as Parquet     (your config)
                   reads the       ├──▶ baseline_builder ──▶ state.sqlite                     (behavior-baseline)
                   tables geo,     │                              │
                   asn, networks   └──▶ baseline_renderer ◀───────┘
                   and profile          an actor's records, a group at a time               (judge)
                   (enrich)                  │
                                             ▼
                                           judge ──▶ fresh ──┬──▶ flagged ──▶ alerts.ndjson
                                                             └──▶ verdicts.ndjson
```

One config holds all of it, in one process: the judge's groups run on
workers of their own, so ingest never waits on the model. A pack defines
only its plugin, and your config draws its edges, to `baseline_builder`,
`baseline_renderer` and the sink that keeps its records
([the lake](../README.md#the-lake) in the examples).

## ingest.yaml

- **baseline_renderer**, behavior-baseline's, which your config routes each
  pack's plugin to: this file groups its records an actor at a time
  (`group_by: [_norm.org, _norm.actor]`): a group closes at 30 records
  (`max_events`) or an hour after its first record (`max_age_ms`), however
  busy the actor; an hour's quiet (`expire_after_ms`) closes it no sooner.
  It reaches the renderer whole, on workers of its own, with the actor's
  latest ten records again ahead of it, marked `_context` (`history: 10`):
  forty records a request, which `batch_bytes` is sized for. Groups wait in
  memory; the end of input closes every open one, and so does a signal
  (Ctrl-C, a restart). After a signal, though, the model calls have ended:
  the groups it closes and the requests the model has not answered get no
  verdict, their records in your sink and the baseline, and nothing asks
  about them again. The renderer puts the same `_baseline` on each of an
  actor's records
  ([behavior-baseline](../behavior-baseline#what-it-stamps)); when it
  fails, the records go to the model without it.
- **judge** reads an actor's records together and writes a verdict per record,
  with [judge.md](judge.md) as its brief and
  [judge.schema.json](judge.schema.json) as the verdict's shape.
- **fresh** drops the `_context` records; **flagged** keeps the verdicts a
  person should see, in `alerts.ndjson`. Every verdict goes to
  `verdicts.ndjson`.

## The model

The judge is any OpenAI-compatible model, set by the environment:

| variable | default |
|---|---|
| `JUDGE_URL` | `http://localhost:8080/v1` |
| `JUDGE_MODEL` | `glm-4.7-flash` |
| `JUDGE_API_KEY` | `none` |

Serve the default with llama.cpp's `llama-server`, which turns the verdict
schema into a grammar, so replies are always well-formed and their reasons
stay short (`mlx_lm.server` accepts the schema and ignores it, and with this
model about one reply in five came back unreadable):

```bash
llama-server -m GLM-4.7-Flash-Q4_K.gguf --port 8080 -np 1 -c 32768 -ngl 99 -fa on --jinja --temp 0.7 --top-p 0.95
```

For a hosted one: `JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY`.

The records go to the model as logfmt, a line each, each source written as
its row under `encoding.sources` says: about half the tokens of JSON lines, and an
actor's `_baseline` written once per request. Your config can send JSON
lines instead, where `_baseline` is on every record; give it half the
`batch_bytes` then, so a request fits a 32k context:

```yaml
inference:
  judge:
    batch_bytes: 48000
    encoding:
      type: ndjson
```

Measured on the requests of the corpus the packs are evaluated on, an
actor's forty records each, with GLM-4.7-Flash's tokenizer, `judge.md` and
the schema included:

| encoding | `batch_bytes` | requests | tokens, median | 90th percentile | largest |
|---|---|---|---|---|---|
| logfmt, without `_baseline` | 48000 | 487 | 6,508 | 8,298 | 15,928 |
| logfmt (the default) | 96000 | 485 | 6,817 | 8,604 | 18,560 |
| JSON lines, without `_baseline` | 48000 | 487 | 9,089 | 10,323 | 21,086 |
| JSON lines | 96000 | 485 | 20,262 | 22,620 | 33,303 |
| JSON lines | 48000 | 872 | 12,738 | 15,689 | 20,200 |

Logfmt was measured with Claude Haiku 5.5, where it caught what JSON caught;
the default model has not been measured with either.

## Slack

What **flagged** keeps can go to a channel too: add a `slack` sink and its
edge to your config, and export `SLACK_WEBHOOK_URL`, an incoming
webhook's URL.

```yaml
sinks:
  slack:
    type: slack
    webhook_url: ${SLACK_WEBHOOK_URL}
    text: "*{_norm.actor}* {_norm.kind|eventName|eventType|action|event|step|events.0.name} from {_norm.ip|sourceIPAddress|client.ipAddress|ipAddress|actor_ip|ip|client_ip}: {judge.why|not judged: judge.error}"
dag:
  - from: flagged
    to: [slack]
```

In `text`, `{a.b}` is the record's field (`-` when it has none) and `{x|words y}`
the first of them it has, so a record the model could not judge says why. A
field's value is escaped: a log line cannot mention anyone, link or start a
line in the message. A post that can work later (a 429, after its
`retry-after`, a 5xx, a timeout) is tried again, and its record is acked
only once Slack has it; one Slack refuses (another 4xx) is dropped and
counted in `tangent_sink_records_rejected_total`, and the first is logged.

## What include cannot do

- **Take a stage out of, or put one into, the judge's chain.** Edges add up
  and none can be removed, so `baseline_renderer -> judge` stays. Another
  plugin between them, or a judge without `baseline_renderer`, means copying
  `ingest.yaml` instead of including it, or a change here that every config
  gets, as baseline was. Each pack's edges are your config's own, so where a
  pack's records go is yours to say.
- **Keep a batch's order.** A source routed to several packs hands each its own
  records in turn, and several workers hand an actor's records to its group as
  they finish, so a group holds them in the order they arrived, not by
  `ts`.
