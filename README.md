<p align="center">
  <img src="https://github.com/user-attachments/assets/7da02584-0a4e-4e41-af6d-61cb081029c4" alt="Logo" height=170>
</p>
<h1 align="center">Tangent</h1>
<div align="center">
  <a href="https://docs.telophasehq.com">Documentation</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://discord.gg/ZUHB3BRa8c">Discord</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/telophasehq/tangent/issues/new">Issues</a>
</div>

### [Read the docs →](https://docs.telophasehq.com)

## What is Tangent?
Tangent is a WebAssembly-powered log pipeline and toolkit for developers. It ships as a single executable, `tangent`.

It ingests data from sources, runs transformations inside WASM plugins written in Go or Python, and ships to any sink.

Unlike typical ETL or log shippers, Tangent makes it easy to transform data using WASM. Each transform runs in a lightweight sandbox with near-native speed and full language flexibility — no DSLs, no vendor-locked runtimes.

Tangent ships with everything you need to develop, test, and benchmark your own transforms:
* `tangent plugin scaffold` – generate a plugin boilerplate in Go or Python
* `tangent plugin compile` – compile wasm plugin
* `tangent plugin test` – run plugin tests
* `tangent bench` – measure throughput and latency before deploying
* `tangent run` – to start the tangent runtime

## Why use Tangent?
Tangent is designed for teams that need real transformations at scale, not just log forwarding.

1. **Use real languages, not DSLs** – Transform complex schemas like OCSF using full programming languages with type safety, imports, and tests. It’s easier to debug, review, and co-develop with LLMs.

2. **Built-in performance and correctness checks** – With tangent test and tangent bench, you can verify both logic and throughput before your plugin hits production. No more regressions that silently slow your pipeline.

3. **Composable and shareable** – We want data transformations to be easy to write and _share_. You can publish and discover open-source transforms in the [Tangent Plugins library](https://github.com/telophasehq/tangent-plugins).

4. **Warehouse-native output** – Tangent lands data directly in object storage like S3. We are actively working on Iceberg and Delta Lake support.

## Install
```bash
# with install script
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/telophasehq/tangent/releases/download/latest/tangent-cli-installer.sh | sh

# Homebrew
brew tap telophashq/telophase
brew install tangent-cli

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
* WASM Plugins
  * [Authoring](https://docs.telophasehq.com/plugins/authoring)
  * [Examples](https://docs.telophasehq.com/plugins/examples)
  * [Community](https://docs.telophasehq.com/plugins/community)
* CLI
  * [`tangent run`](https://docs.telophasehq.com/cli/run)
  * [`tangent plugin scaffold`](https://docs.telophasehq.com/cli/plugin/scaffold)
  * [`tangent plugin compile`](https://docs.telophasehq.com/cli/plugin/compile)
  * [`tangent plugin test`](https://docs.telophasehq.com/cli/plugin/test)
  * [`tangent bench`](https://docs.telophasehq.com/cli/bench)
  * [`tangent.yaml`](https://docs.telophasehq.com/cli/tangent-yaml)
* Sources
  * [SQS](https://docs.telophasehq.com/sources/sqs)
  * [MSK](https://docs.telophasehq.com/sources/msk)
  * [Socket](https://docs.telophasehq.com/sources/socket)
  * [File](https://docs.telophasehq.com/sources/file)
* Sinks
  * [S3](https://docs.telophasehq.com/sinks/s3)
  * [File](https://docs.telophasehq.com/sinks/file)
  * [Blackhole](https://docs.telophasehq.com/sinks/blackhole)


## License

Apache-2.0. See `LICENSE`.

