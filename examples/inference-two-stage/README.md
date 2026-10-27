# inference-two-stage

The same `inference` node twice. A small model on this machine answers **one
word** about every record; only the yeses reach a stronger model, which writes
the full verdict. Different model, different prompt, different output — one
node, configured twice.

```
events ──▶ first_pass (local, answer: yes_no) ──▶ verdict (hosted, answer: json) ──▶ flagged.ndjson
```

```yaml
inference:
  first_pass:
    backend: openai
    url: http://localhost:8080/v1
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: first_pass.md
    answer: yes_no                  # one word: yes forwards, no drops, anything else is an error
    concurrency: 8
    max_tokens: 8
    extra: { chat_template_kwargs: { enable_thinking: false } }
  verdict:
    backend: bedrock
    model: us.anthropic.claude-haiku-4-5-20251001-v1:0
    region: us-west-2
    prompt_file: verdict.md
    forward_if: flag
    concurrency: 8
```

`answer` is the only difference in kind between the two nodes. With `yes_no`,
[first_pass.md](first_pass.md) asks for one word; the node reads the first word
of the reply (`Yes.`, `**no**`, a `<think>` block first — all fine) and forwards
on yes. Anything that is neither yes nor no is a failure handled by `on_error`
(pass by default, so an odd reply reaches the second node rather than
vanishing). With `json`, the second node's object is merged into the record (under
`_verdict`, the node's name) and `forward_if: flag` decides what reaches the
sink.

## Why one word first, measured

Sixty synthetic events (ten of each kind in [events.ndjson](events.ndjson)),
eight in flight, on an M4 Max with mlx_lm.server:

| local model, output | events/s | p50 | recall (20 planted) | false positives (40 benign) |
|---|---|---|---|---|
| GLM-4.7-Flash, `json` | 2.4 | 0.84 s | 20/20 | 5–6 (Okta's 429) |
| GLM-4.7-Flash, `yes_no` | **14.4** | **0.30 s** | 20/20 | 1 (a scanner probe) |
| gpt-oss-20b, `json` | 2.0 | 1.0 s | 20/20 | 0 |
| gpt-oss-20b, `yes_no` | 3.1 | 0.5 s | 15/20 | 0 |

With thinking off and one output token, a one-word answer costs only the prompt
pass, so GLM reads six times more records per second than when it writes JSON —
and on single records it is also more accurate that way. gpt-oss-20b is the
opposite: precise with `json` (`reasoning_effort: low`) and poor with one
word, and it needs `max_tokens` large enough for the reasoning that precedes
the word (about 100), where GLM with thinking off needs 8.

One word is a per-record trick. Asked for one word about a whole *window* of
events, GLM dropped 11 of 27 real cases that it caught when writing JSON; the
reasons are in the sentry repo's `cases/RESULTS.md`.

The second node only pays for the yeses: here it confirmed the `transfer.sh`
upload and the admin key from a new country and never saw the other four
events. Any backend works for it — the hosted model here is just the one that
was measured at 100% on the corpus.

## Run

```bash
mlx_lm.server --model mlx-community/GLM-4.7-Flash-4bit --port 8080   # or any OpenAI-compatible local server
aws sso login --profile <your-profile>                                # or any AWS credential source, for the second node
tangent run --config tangent.yaml
```

```bash
nc localhost 9000 < events.ndjson
cat flagged.ndjson      # each record carries _first_pass.answer and _verdict.{flag,one_line,why}
```
