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
Plugins keep state through the `state` interface: `state.Get`, `state.Set`, `state.Incr` and `state.Del`, each write with an optional TTL. That state lives in the runtime's **state store**, configured under `runtime.state`:

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
```

Entries are keyed by `(namespace, partition, key)`. Plugins share one namespace, `plugins`, so what one plugin writes the next can read; within a partition all of it runs on one worker, so there is nothing to race against (below). Give a plugin a `state_namespace` of its own when it must not share, such as two instances of one plugin with different configs:

```yaml
plugins:
  detect_login:  { module_type: rust, path: ./detect, state_namespace: login }
  detect_export: { module_type: rust, path: ./detect, state_namespace: export }
```

With `runtime.partition` set, a partition's records all go to one worker, which hands them to a plugin one partition at a time with its state calls scoped to that partition, so "first time this tenant did X" is a `state.Get` away with no change to the plugin and no race between workers. Without it, everything is in the empty partition. [examples/partition](/examples/partition) counts events per org with a single `count` key.

The store is readable while the pipeline runs:

```bash
tangent state --config tangent.yaml ls                          # keys per namespace and partition
tangent state --config tangent.yaml get seen --partition acme   # one entry (--ns defaults to the only namespace)
tangent state --config tangent.yaml scan --prefix user: --limit 50
tangent state --config tangent.yaml sql "select partition, count(*) from state group by 1"   # sqlite backend only
```

## Inference
An `inference` node asks a model about each record. Nodes are configured under `inference:`, each with its own model and prompt, and sit in the DAG as `kind: inference`:

```yaml
inference:
  judge:
    url: http://localhost:8080/v1          # any OpenAI-compatible chat endpoint
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: judge.md                  # the system message
    output: _judge                         # the model's JSON reply lands here
    forward_if: flag                       # pass only records the model flags
    extra: { chat_template_kwargs: { enable_thinking: false } }
```

The model is reached through [rig](https://github.com/0xPlaygrounds/rig): `backend: openai` (the default) for any OpenAI-compatible chat endpoint such as OpenAI, mlx_lm.server, llama-server, vLLM or Bedrock's OpenAI endpoint, and `backend: bedrock` for Amazon Bedrock's Converse API with the AWS credential chain (`region`, `profile`).

The record (or its `fields`) is the user message. `forward_if` names a field of the reply: only records where it is true go on, and a reply without it counts as a failure. With `answer: yes_no` the model is asked for one word instead of JSON: yes forwards, no drops, anything else is a failure. A small local model is six times faster that way and makes a good first pass in front of a hosted judge ([examples/inference-two-stage](/examples/inference-two-stage)). `on_error: pass` (the default) forwards a failed record with `{"error": ...}` under `output` so nothing is lost silently; `on_error: drop` drops it. `concurrency` bounds requests in flight (1 for a laptop GPU, 8 or more for a hosted model); `timeout_ms` (30 s) frees a slot a hung request would otherwise hold. See [examples/inference-local](/examples/inference-local) and [examples/inference-bedrock](/examples/inference-bedrock).

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
