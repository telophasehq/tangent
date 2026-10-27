# inference-two-stage

The same `inference` node twice. A small model on this machine answers **one
word** about every record; a plugin keeps the yeses; a stronger model writes the
full verdict on those; a plugin keeps what it flagged. Different model,
different prompt, different output — one node, configured twice, and the
decisions in between are plugins.

```
events ──▶ first_pass (answer: yes_no) ──▶ keep_yes ──▶ judge (answer: json) ──▶ flagged ──▶ flagged.ndjson
              adds _first_pass              plugin       adds _judge               plugin
```

```yaml
inference:
  first_pass:
    backend: openai
    url: http://localhost:8080/v1
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: first_pass.md
    answer: yes_no                  # one word, landing as _first_pass.answer
    concurrency: 8
    max_tokens: 8
    extra: { chat_template_kwargs: { enable_thinking: false } }
  judge:
    backend: bedrock
    model: us.anthropic.claude-haiku-4-5-20251001-v1:0
    region: us-west-2
    prompt_file: judge.md           # the full verdict, landing as _judge
    concurrency: 8
plugins:
  keep_yes: { module_type: rust, path: keep_yes }
  flagged:  { module_type: rust, path: ../inference-local/flagged }
```

An inference node adds its reply to the record and forwards everything. What
goes on is a plugin's call, and a plugin's `probe()` is that filter:
[keep_yes](keep_yes/src/lib.rs) selects `_first_pass.answer == "yes"` (and
records the first pass could not answer), [flagged](../inference-local/flagged/src/lib.rs)
selects `_judge.flag == true` (and records the judge could not read). Both pass
records through unchanged. A record only reaches the plugin its DAG edge names:
the judge's output goes to `flagged`, and `keep_yes` never sees it again even
though its `_first_pass.answer` still says yes.

`answer` is the only difference in kind between the two nodes. With `yes_no`,
[first_pass.md](first_pass.md) asks for one word and the node reads the first
word of the reply (`Yes.`, `**no**`, a `<think>` block first — all fine);
anything that is neither yes nor no lands as `{"error": ...}`. With `json`, the
model's object lands as is.

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
upload and the admin key from a new country and never saw the other events.
Any backend works for it — the hosted model here is the one that was measured
at 100% on the corpus.

## Run

```bash
mlx_lm.server --model mlx-community/GLM-4.7-Flash-4bit --port 8080   # or any OpenAI-compatible local server
aws sso login --profile <your-profile>                                # or any AWS credential source, for the second node
tangent plugin compile --config tangent.yaml
tangent run --config tangent.yaml
```

```bash
nc localhost 9000 < events.ndjson
cat flagged.ndjson      # each record carries _first_pass.answer and _judge.{flag,one_line,why}
```
