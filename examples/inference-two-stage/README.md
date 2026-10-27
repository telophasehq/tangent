# inference-two-stage

Two models. A small one on this machine answers **yes or no** to every record;
only the yeses reach a stronger hosted one, which writes the verdict.

```
events ──▶ filter (local, answer: yes_no) ──▶ judge (Bedrock, JSON) ──▶ flagged.ndjson
```

```yaml
inference:
  filter:
    backend: openai
    url: http://localhost:8080/v1
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: filter.md
    answer: yes_no                  # one word: yes forwards, no drops, anything else is an error
    output: _filter
    concurrency: 8
    max_tokens: 8
    extra: { chat_template_kwargs: { enable_thinking: false } }
  judge:
    backend: bedrock
    model: us.anthropic.claude-haiku-4-5-20251001-v1:0
    region: us-west-2
    prompt_file: judge.md
    output: _judge
    forward_if: flag
    concurrency: 8
```

`answer: yes_no` makes the small model a pure filter: [filter.md](filter.md)
asks for one word, the node reads the first word of the reply (`Yes.`,
`**no**`, a `<think>` block first — all fine) and forwards on yes. Anything
that is neither yes nor no is a failure, handled by `on_error` (pass by
default, so an odd reply reaches the judge rather than vanishing). With
thinking off and one output token, a verdict costs only the prompt pass.

## Why a filter, measured

Sixty synthetic events (ten of each kind in [events.ndjson](events.ndjson)),
eight in flight, on an M4 Max with mlx_lm.server:

| local model, mode | events/s | p50 | recall (20 planted) | false positives (40 benign) |
|---|---|---|---|---|
| GLM-4.7-Flash, JSON verdict | 2.4 | 0.84 s | 20/20 | 5–6 (Okta's 429) |
| GLM-4.7-Flash, yes/no | **14.4** | **0.30 s** | 20/20 | 1 (a scanner probe) |
| gpt-oss-20b, JSON verdict | 2.0 | 1.0 s | 20/20 | 0 |
| gpt-oss-20b, yes/no | 3.1 | 0.5 s | 15/20 | 0 |

GLM as a one-word filter is six times faster than GLM writing JSON, and more
accurate. gpt-oss-20b is the better judge (`reasoning_effort: low`, perfect on
this set) but a worse filter: denied room to reason, it said no to half of the
admin-key events. Reasoning models also need `max_tokens` large enough for the
reasoning that precedes the one word (about 100 for gpt-oss), where GLM with
thinking off needs 8.

The judge then only pays for the yeses: here Claude Haiku 4.5 confirmed the
`transfer.sh` upload and the admin key from a new country and never saw the
other four events. In a production stream the yeses are a small fraction, and
the judge's cost follows them, not the stream.

## Run

```bash
mlx_lm.server --model mlx-community/GLM-4.7-Flash-4bit --port 8080   # or any OpenAI-compatible local server
aws sso login --profile <your-profile>                                # or any AWS credential source
tangent run --config tangent.yaml
```

```bash
nc localhost 9000 < events.ndjson
cat flagged.ndjson      # each record carries _filter.answer and _judge.{flag,one_line,why}
```
