# inference-local

A model on this machine reads every record; a plugin keeps what it flagged.
Nothing leaves the box.

```
events ──▶ judge (inference: adds judge) ──▶ flagged (plugin: keeps judge.flag) ──▶ flagged.ndjson
```

The `judge` node sends the records it is handed, one per line, to a local
OpenAI-compatible server with [judge.md](judge.md) as the system prompt and
adds the model's answer for each to the record as `judge` (the node's name).
That is all an inference node does: it adds and forwards, it never drops. The
[flagged](flagged/src/lib.rs) plugin is the filter — its `probe()` selects
records with `judge.flag == true`, or with `judge.error` (the judge could not
read the reply, so a person should), and its body passes them through
unchanged.

```yaml
inference:
  judge:
    backend: openai
    url: http://localhost:8080/v1
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: judge.md
    batch_bytes: 4000
    concurrency: 8
    max_tokens: 300
    extra:
      chat_template_kwargs: { enable_thinking: false }
plugins:
  flagged:
    module_type: rust
    path: flagged
```

## A model on your machine

Any server that speaks the OpenAI chat API works; pick one:

```bash
# Apple silicon
pip install mlx-lm
mlx_lm.server --model mlx-community/GLM-4.7-Flash-4bit --port 8080

# llama.cpp, any OS (downloads and caches the GGUF itself)
brew install llama.cpp
llama-server -hf unsloth/GLM-4.7-Flash-GGUF:Q4_K_M --port 8080

# Ollama: set url to http://localhost:11434/v1 and model to e.g. gpt-oss:20b
ollama serve
```

`chat_template_kwargs.enable_thinking: false` is understood by mlx_lm and
llama-server; without it a reasoning model spends its whole token budget
thinking and a verdict takes minutes instead of seconds. Ollama ignores it.
`batch_bytes: 4000` keeps a request to about twenty of these events, so its
reply comes back within `timeout_ms`; `concurrency: 8` runs several such
requests at once, and mlx_lm.server batches their decodes (llama-server needs
`--parallel 8` for the same; Ollama, `OLLAMA_NUM_PARALLEL`). `max_tokens: 300`
is per record and bounds the occasional runaway reply. See
[../inference-bedrock](../inference-bedrock) for the same pipeline on a hosted
model.

## Run

```bash
tangent plugin compile --config tangent.yaml    # builds flagged/ into plugins/
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
also flag Okta's 429 rate limit or a routine step on some runs, which is what
the prompt's "do not flag" list is there to tune.

When the model cannot be reached or its reply cannot be read, the record still
goes on with `{"error": ...}` as `judge`; the plugin keeps those too, so
nothing is lost silently.
