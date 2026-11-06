# Inference

An `inference` node asks a model about each record. Nodes are configured
under `inference:`, each with its own model and prompt, and sit in the DAG by
name, as every node does:

```yaml
inference:
  judge:
    url: http://localhost:8080/v1          # any OpenAI-compatible chat endpoint
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: judge.md                  # the system message
    extra: { chat_template_kwargs: { enable_thinking: false } }
```

## Backends

The model is reached through [rig](https://github.com/0xPlaygrounds/rig):
`backend: openai` (the default) for any OpenAI-compatible chat endpoint such
as OpenAI, mlx_lm.server, llama-server, vLLM or Bedrock's OpenAI endpoint,
and `backend: bedrock` for Amazon Bedrock's Converse API with the AWS
credential chain (`aws: { region, profile }`). See
[examples/inference-local](../examples/inference-local) and
[examples/inference-bedrock](../examples/inference-bedrock).

## What the model reads and answers

The records the node is handed are the user message, one JSON object per line
(the record, or its `fields`) with its index `i`, or with
`encoding: {type: logfmt}` a logfmt line each, and the reply is
`{"answers": [...]}`: each answer, a JSON object, lands on its record under
the node's name (`judge` above); a failure lands there as `{"error": ...}`.
The node adds and forwards, never drops. What goes on from there is a
plugin's decision, and a plugin's `probe()` is already that filter:
[examples/inference-local/flagged](../examples/inference-local/flagged/src/lib.rs)
keeps `judge.flag == true` (and anything the judge could not read) with a
pass-through body.

The reply is read as JSON and nothing else, and the backend is asked to make
it so: with `schema_file` (a JSON Schema next to the prompt) through
`response_format` on an OpenAI-compatible server or structured output on
Bedrock; without one, an `openai` server is put in JSON mode. Holding a reply
to the schema is the server's work (llama-server compiles it to a grammar);
tangent does not check an answer against it. A server that ignores both
(mlx_lm.server does) is held to the prompt. A reply that is not a JSON object, or has no `answers` array,
is the error; an answer that is a JSON object of another shape is attached
as it is, so core's `{"flag": "true"}` or `{}` is not a flag, and `flagged`
passes it over as it would a pass.

## Requests

A request holds the records the node is handed, or with
`group_by: [_norm.actor]` those of one value of these fields, and is split
past `batch_bytes` (32 KiB of JSON) and, with logfmt, past `budget_tokens`
(90k). A plugin's `group_by` ahead of the node ([plugins](plugins.md#group_by))
hands it a key's records together, so with the same fields a request is one
group, as in core's judge; a `sqlite` source does the same from a table
([sinks](sinks.md#sqlite)), a partition value's records `batch_size` at a
time. A schema is wrapped around each answer, `max_tokens` is per record, and
a reply that cannot be read is asked again in halves, down to single records;
a record the reply skipped gets an error.

## logfmt

`encoding: {type: logfmt, keep: [...], sources: {<source>: {principal, time, keep, ids}}}`
writes the records as logfmt lines in about half the tokens of JSON, and the
text decodes back to the records: a principal's records are grouped, the
fields they share written once above their rows, the `keep` fields on every
row, a day's timestamps as times of day; `sidecar: true` swaps a source's
`ids` for references the model never sees. Core's judge has a row for each
pack's sources ([packs/core/judge.yaml](../packs/core/judge.yaml)). The text
is printable ASCII, with controls and anything else escaped, so no value or
key can end a field, a row or a section ([crates/compact](../crates/compact/src/lib.rs),
fuzzed in `crates/compact/fuzz`).

## Running

Inference nodes run inside the workers, in DAG order with the plugins: a
worker hands a model the records that reached the node, waits for the
replies, and carries them on, so a batch's records stay in order and nothing
else moves while the model thinks. `concurrency` bounds requests in flight to
the node, every worker's together (1 for a laptop GPU, 8 or more for a hosted
model); `timeout_ms` (30 s) frees a slot a hung request would otherwise hold;
`retries` (2) tries again, backing off, when a request got no reply at all.
`temperature` is sent only when set.

A signal (SIGINT or SIGTERM) ends the calls under way rather than wait out
`timeout_ms` and `retries`: records waiting on one go to no sink and are not
acked, so a source that resends what was not acked (a `sqlite` source, at its
next start) sends them again. Records a plugin's `group_by` held are acked
already, so behind a grouped plugin, as in core's judge, they are judged by
no one: a stop closes every open group, sends it on, and ends its call
("stopped before the model answered" in the log). Another signal, a second
or more after the first, exits at once.

## Settings

| key | default | |
|---|---|---|
| `backend` | `openai` | or `bedrock` |
| `url` | `http://localhost:8080/v1` | `openai` |
| `api_key` | | `openai`; left out of `tangent config` |
| `model` | | as the backend names it |
| `aws` | | `bedrock`: `{ region, profile }`, the SDK's defaults when unset |
| `prompt_file` | | the system message, relative to the file that names it |
| `schema_file` | | a JSON Schema the backend is asked to hold each answer to (tangent does not check it), relative to the file that names it |
| `fields` | the whole record | the fields the model is shown |
| `encoding` | `{ type: ndjson }` | or `{ type: logfmt, keep, sources, budget_tokens: 90000, sidecar: false }` |
| `group_by` | | a request holds one value of these fields |
| `batch_bytes` | `32768` | a request past this many bytes of JSON is split |
| `max_tokens` | `2000` | per record |
| `concurrency` | `1` | requests in flight, every worker's together |
| `timeout_ms` | `30000` | |
| `retries` | `2` | after a request that got no reply; backing off 1 s, 2 s, 4 s, ... |
| `temperature` | | sent only when set |
| `extra` | | parameters merged into the request body, winning over the node's: `chat_template_kwargs: { enable_thinking: false }` for llama-server or mlx_lm, `reasoning_effort: low` for gpt-oss |
