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

## Install
```bash
# Homebrew
brew tap telophasehq/telophase
brew install tangent-cli
tangent --version

# with install script
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/telophasehq/tangent/releases/download/latest/tangent-cli-installer.sh | sh
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