# inference

A model reads every record; only what it flags goes on.

`tangent.yaml` has no plugins. Records go from a TCP source to an `inference`
processor, which sends each one to an OpenAI-compatible chat endpoint with
[judge.md](judge.md) as the system prompt, adds the model's JSON reply to the
record under `_judge`, and forwards the record only when `_judge.flag` is true.

```yaml
processors:
  judge:
    type: inference
    url: http://localhost:8080/v1
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: judge.md
    output: _judge
    forward_if: flag
    extra:
      chat_template_kwargs: { enable_thinking: false }
```

## A model on your Mac

```bash
pip install mlx-lm
mlx_lm.server --model mlx-community/GLM-4.7-Flash-4bit --port 8080
```

Any OpenAI-compatible endpoint works: change `url` and `model`, add
`api_key: "${OPENAI_API_KEY}"` for a hosted one, and drop `extra` (the
`chat_template_kwargs` setting is mlx_lm's way of skipping the thinking phase,
which otherwise makes a verdict take minutes instead of seconds).

## Run

```bash
tangent run --config tangent.yaml
```

In another shell:

```bash
nc localhost 9000 < events.ndjson
cat flagged.ndjson
```

[events.ndjson](events.ndjson) holds six synthetic events. The plan step posting
48 KB to `transfer.sh` and the admin API key used from a new country come out in
`flagged.ndjson`, each with the model's one-line summary and reasoning; the
scanner probe, the deploy and the finished step pass quietly. A small model may
also flag Okta's 429 rate limit, which is what the prompt's "do not flag" list
is there to tune. With thinking off, GLM-4.7-Flash takes about a second per
event on an M-series Mac.

When the model cannot be reached, records are forwarded with
`{"error": ...}` under `_judge` (`on_error: pass`, the default), so nothing is
lost silently; `on_error: drop` drops them instead.
