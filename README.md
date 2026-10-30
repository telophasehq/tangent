<p align="center">
  <img src="https://github.com/user-attachments/assets/7da02584-0a4e-4e41-af6d-61cb081029c4" alt="Tangent logo" height=170>
</p>
<h1 align="center">Tangent</h1>
<p align="center"><em>Stream processing without DSLs. Shareable, sandboxed, fast.</em></p>


<p align="center">
  <a href="https://github.com/telophasehq/tangent/actions">
  <img height="20" src="https://img.shields.io/github/actions/workflow/status/telophasehq/tangent/scaffold_test.yml?label=build&logo=github" alt="Build status"></a>
  <a href="https://twitter.com/telophasehq">
  <img src="https://img.shields.io/twitter/follow/telophasehq?style=social" alt="X">
  </a>

</p>

<p align="center">
  <a href="https://docs.telophasehq.com">Documentation</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://discord.gg/ZUHB3BRa8c">Discord</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/telophasehq/tangent/issues/new">Issues</a>
</p>

## What is Tangent?
Tangent is a stream‑processing toolkit that treats **plugins** (user‑defined functions) as first‑class citizens – exceptionally easy to write and share. Perfect for vibe-coding those pesky log transformations.


Plugins run in a lightweight WASM sandbox with near-native speed and full language flexibility — no DSLs, no vendor-locked runtimes. Plugins are designed to be shareable, so common transformations (e.g. GuardDuty findings → OCSF) can be written once and shared with the [community](https://github.com/telophasehq/tangent-plugins).

Tangent ships with everything you need to develop, test, and benchmark your own transforms:
* `tangent plugin scaffold` – generate plugin boilerplate
* `tangent plugin compile` – compile plugins to WASM
* `tangent plugin test` – run plugin tests
* `tangent bench` – measure throughput and latency before deploying
* `tangent run` – start the Tangent runtime

## Why use Tangent?
1. **Use real languages, not DSLs** – Real code > DSL. Reviewable, testable, LLM‑friendly.

2. **Catch breakage before prod** — `tangent plugin test` for correctness; `tangent bench` for throughput/latency.

3. **Shareable and secure** – Data transformations are easy to write and _share_. Publish and discover open-source plugins in the [Tangent Plugins library](https://github.com/telophasehq/tangent-plugins). Each plugin runs in its own lightweight sandbox.

## State and partitioning
Plugins keep state through the `state` interface: `state.Get`, `state.Set`, `state.Incr`, `state.Del`, and `state.Scan` for a key range, each write with an optional TTL. That state lives in the runtime's **state store**, configured under `runtime.state`:

```yaml
runtime:
  state:
    backend: sqlite        # or lru (in-process, bounded); distributed backends slot in here
    path: state.sqlite
    default_ttl_ms: 600000 # 0 = never expire
    max_ttl_ms: 3600000    # cap on any TTL; 0 = no cap
    max_entries: 100000    # lru only: evict the least recently used past this
  partition:
    key: [org_id]          # fields that identify a record's partition
    time: ts               # the record's own time (RFC 3339 or epoch); arrival time without it
    retain_ms: 432000000   # 5d: keep every record under its partition, keyed by its time
```

Entries are keyed by `(namespace, partition, key)`. Plugins share one namespace, `plugins`, so what one plugin writes the next can read; within a partition all of it runs on one worker, so there is nothing to race against (below). Give a plugin a `state_namespace` of its own when it must not share, such as two instances of one plugin with different configs:

```yaml
plugins:
  detect_login:  { module_type: rust, path: ./detect, state_namespace: login }
  detect_export: { module_type: rust, path: ./detect, state_namespace: export }
```

A plugin may read files too: `data: <dir>` mounts a directory, relative to the config file, read-only at `/data` in the guest, for reference data too big for `config` such as an IP database or published address ranges. [examples/enrich](/examples/enrich) reads MaxMind GeoLite2 and the cloud providers' ranges that way, and its `setup.sh` fetches them.

With `runtime.partition` set, a partition's records all go to one worker, which hands them to a plugin one partition at a time with its state calls scoped to that partition, so "first time this tenant did X" is a `state.Get` away with no change to the plugin and no race between workers. Without it, everything is in the empty partition. [examples/partition](/examples/partition) counts events per org with a single `count` key.

With `retain_ms` as well, the runtime keeps every partitioned record in the store, in the plugins' namespace, as `ev:<epoch ms, 13 digits>:<seq>` with that TTL, so a partition's history is a key range: `state.Scan("ev:<from>", "ev:<to>", limit)` returns the last hour in order, however it arrived, and nothing is ever read back to be rewritten. The store is the partition's log; the TTL is its retention. A record is stored once, where it enters its partition, whether the key was on it at the source or a plugin added it. What a record means in the light of that history is a plugin's decision, and the hour of context it attaches for a model is one `Scan`. Each backend answers a range its own way: SQLite walks the primary-key index, a DynamoDB backend would query the sort key.

The store is readable while the pipeline runs:

```bash
tangent state --config tangent.yaml ls                          # keys per namespace and partition
tangent state --config tangent.yaml get seen --partition acme   # one entry (--ns defaults to the only namespace)
tangent state --config tangent.yaml scan --prefix user: --limit 50
tangent state --config tangent.yaml sql "select partition, count(*) from state group by 1"   # sqlite backend only
```

## Keeping records and reading them back
A `sqlite` sink keeps every record once, in its `table`, with a column per partition holding its fields' values joined with `|`. A `sqlite` source, in the same process or another, reads one partition back in batches, each of one partition value:

```yaml
sinks:
  logs:
    type: sqlite
    path: logs.sqlite
    table: logs
    time: ts               # the record's own time (RFC 3339 or epoch); arrival time without it
    partitions:
      org: [_norm.org]
      actor: [_norm.org, _norm.actor]

sources:
  actors:
    type: sqlite
    path: logs.sqlite
    table: logs
    partition: actor
    batch_size: 40         # every batch is a partition value's latest 40 records
    timeout_secs: 3600     # send by then even if fewer than 40 are new
```

A partition value's records not yet sent go oldest first, `batch_size` to a batch, one batch of a value at a time. Fewer wait until the oldest is `timeout_secs` older than the newest record stored, so a replay of old logs goes as live ones do, or until nothing new has been stored for `timeout_secs`; then they go topped up to `batch_size` with the value's latest records already sent, which carry `"_context": true`. A batch's new records get the time it is through the DAG in `emitted`, NULL until then, and the value's next batch goes then; a restart sends again what was not. Nothing is deleted: the table grows, and trimming it is the operator's job.

## Inference
An `inference` node asks a model about each record. Nodes are configured under `inference:`, each with its own model and prompt, and sit in the DAG as `kind: inference`:

```yaml
inference:
  judge:
    url: http://localhost:8080/v1          # any OpenAI-compatible chat endpoint
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: judge.md                  # the system message
    extra: { chat_template_kwargs: { enable_thinking: false } }
```

The model is reached through [rig](https://github.com/0xPlaygrounds/rig): `backend: openai` (the default) for any OpenAI-compatible chat endpoint such as OpenAI, mlx_lm.server, llama-server, vLLM or Bedrock's OpenAI endpoint, and `backend: bedrock` for Amazon Bedrock's Converse API with the AWS credential chain (`aws: { region, profile }`).

The records the node is handed are the user message, one JSON object per line (the record, or its `fields`) with its index `i`, and the reply is `{"answers": [...]}`: each answer, a JSON object, lands on its record under the node's name (`judge` above); a failure lands there as `{"error": ...}`. The node adds and forwards, never drops. What goes on from there is a plugin's decision, and a plugin's `probe()` is already that filter: [examples/inference-local/flagged](/examples/inference-local/flagged/src/lib.rs) keeps `judge.flag == true` (and anything the judge could not read) with a pass-through body.

The reply is read as JSON and nothing else, and the backend is asked to make it so: with `schema_file` (a JSON Schema next to the prompt) through `response_format` on an OpenAI-compatible server or structured output on Bedrock; without one, an `openai` server is put in JSON mode. A server that ignores both (mlx_lm.server does) is held to the prompt, and a reply that is not a JSON object is the error.

A request holds the records the node is handed, or with `group_by: [_norm.actor]` those of one value of these fields, and is split past `batch_bytes` (32 KiB). A `sqlite` source hands over a partition value's records `batch_size` at a time, so with `group_by` on the partition's fields a request is one of its batches. A schema is wrapped around each answer, `max_tokens` is per record, and a reply that cannot be read is asked again in halves.

Inference nodes run inside the workers, in DAG order with the plugins: a worker hands a model the records that reached the node, waits for the replies, and carries them on, so a partition's records stay in order and nothing else moves while the model thinks. `concurrency` bounds requests in flight per worker (1 for a laptop GPU, 8 or more for a hosted model); `timeout_ms` (30 s) frees a slot a hung request would otherwise hold; `retries` (2) tries again, backing off, when a request got no reply at all. `temperature` is sent only when set. See [examples/inference-local](/examples/inference-local) and [examples/inference-bedrock](/examples/inference-bedrock).

## Packs
A pack is a pipeline a company runs as is: `ingest.yaml` and `judge.yaml`, two processes over one `logs.sqlite`, the plugins it needs, a `profile.yaml` describing the company, the model's brief, the cases it is measured against, and fixtures for `make test`. [packs/cloud-infra](/packs/cloud-infra) reads your own infrastructure's logs straight from their APIs (the `cloudtrail`, `github_audit`, `okta` and `google_workspace` sources; CI over TCP), keeps every record in SQLite, and has a model read each actor's records, forty at a time, and flag what a person should see; [packs/app-log](/packs/app-log) does the same for a product's logs, partitioned per customer; [packs/edge](/packs/edge) is the plan for developer laptops.

## Benchmarks
Benchmarks show end-to-end throughput with simple log transformation plugins implemented in [Rust](/examples/rust/src/lib.rs), [Go](/examples/golang/main.go), and [Python](/examples/python/mapper.py). Each plugin is compiled to WASM and run inside Tangent.

<!-- BENCH_START -->
| Source → Sink | Rust | Go | Python |
|---|---|---|---|
| tcp → blackhole | `483.35 MB/s` | `399.54 MB/s` | `76.91 MB/s` |
<!-- BENCH_END -->

_Benchmarks are run nightly on a dedicated 16-core AMD runner. See [nightly benchmarks](https://github.com/telophasehq/tangent/actions/workflows/bench.yml)._

## Install
```bash
# Homebrew
brew tap telophasehq/telophase
brew install tangent-cli
tangent --version

# with install script
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/telophasehq/tangent/releases/latest/download/tangent-cli-installer.sh | sh
tangent --version

# cargo
cargo install --git https://github.com/telophasehq/tangent tangent-cli

# docker
docker pull ghcr.io/telophasehq/tangent-toolchain
docker run --rm --init ghcr.io/telophasehq/tangent-toolchain <command>

```

## Quick links
* Intro
  * [What is Telophase?](https://docs.telophasehq.com/index)
  * [Installation](https://docs.telophasehq.com/installation)
  * [Quickstart](https://docs.telophasehq.com/quickstart)
  * [Runtime Deepdive](https://docs.telophasehq.com/runtime)
* WASM Plugins
  * [Authoring](https://docs.telophasehq.com/plugins/authoring)
  * [Community](https://docs.telophasehq.com/plugins/community)
* CLI
  * [Overview](https://docs.telophasehq.com/cli/overview)
  * [`tangent run`](https://docs.telophasehq.com/cli/run)
  * [`tangent plugin scaffold`](https://docs.telophasehq.com/cli/plugin/scaffold)
  * [`tangent plugin compile`](https://docs.telophasehq.com/cli/plugin/compile)
  * [`tangent plugin test`](https://docs.telophasehq.com/cli/plugin/test)
  * [`tangent bench`](https://docs.telophasehq.com/cli/bench)
* Configuration
  * [`tangent.yaml`](https://docs.telophasehq.com/configuration/tangent-yaml)
  * [Runtime](https://docs.telophasehq.com/configuration/runtime)
  * [Plugins](https://docs.telophasehq.com/configuration/plugins)
  * [DAG](https://docs.telophasehq.com/configuration/dag)
  * Sources
    * [Overview](https://docs.telophasehq.com/configuration/sources/overview)
    * [SQS](https://docs.telophasehq.com/configuration/sources/sqs)
    * [MSK](https://docs.telophasehq.com/configuration/sources/msk)
    * [Socket](https://docs.telophasehq.com/configuration/sources/socket)
    * [File](https://docs.telophasehq.com/configuration/sources/file)
  * Sinks
    * [Overview](https://docs.telophasehq.com/configuration/sinks/overview)
    * [S3](https://docs.telophasehq.com/configuration/sinks/s3)
    * [File](https://docs.telophasehq.com/configuration/sinks/file)
    * [Blackhole](https://docs.telophasehq.com/configuration/sinks/blackhole)


## License

Apache-2.0. See `LICENSE`.
