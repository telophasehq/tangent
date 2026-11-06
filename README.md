<p align="center">
  <img src="https://github.com/user-attachments/assets/7da02584-0a4e-4e41-af6d-61cb081029c4" alt="Tangent logo" height=170>
</p>
<h1 align="center">Tangent</h1>
<p align="center"><em>Stream processing without DSLs. Shareable, sandboxed, fast.</em></p>


<p align="center">
  <a href="https://github.com/EthanBlackburn/tangent/actions">
  <img height="20" src="https://img.shields.io/github/actions/workflow/status/EthanBlackburn/tangent/scaffold_test.yml?label=build&logo=github" alt="Build status"></a>
  <a href="https://twitter.com/telophasehq">
  <img src="https://img.shields.io/twitter/follow/telophasehq?style=social" alt="X">
  </a>

</p>

<p align="center">
  <a href="https://docs.telophasehq.com">Documentation</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://discord.gg/ZUHB3BRa8c">Discord</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/EthanBlackburn/tangent/issues/new">Issues</a>
</p>

## What is Tangent?
Tangent is a stream‑processing toolkit that treats **plugins** (user‑defined functions) as first‑class citizens – exceptionally easy to write and share. Perfect for vibe-coding those pesky log transformations.


Plugins run in a lightweight WASM sandbox with near-native speed and full language flexibility — no DSLs, no vendor-locked runtimes. Plugins are designed to be shareable, so common transformations (e.g. GuardDuty findings → OCSF) can be written once and shared with the [community](https://github.com/EthanBlackburn/tangent-plugins).

Tangent ships with everything you need to develop, test, and benchmark your own transforms:
* `tangent plugin scaffold` – generate plugin boilerplate (`--setup` also installs the toolchain)
* `tangent plugin compile` – compile plugins to WASM
* `tangent plugin fetch` – download plugins a release built, their build's attestation checked; no toolchain
* `tangent plugin test` – run plugin tests
* `tangent bench` – measure throughput and latency before deploying
* `tangent run` – start the Tangent runtime
* `tangent config` – print a config as the runtime reads it, its includes merged

## Why use Tangent?
1. **Use real languages, not DSLs** – Real code > DSL. Reviewable, testable, LLM‑friendly.

2. **Catch breakage before prod** — `tangent plugin test` for correctness; `tangent bench` for throughput/latency.

3. **Shareable and secure** – Data transformations are easy to write and _share_. Publish and discover open-source plugins in the [Tangent Plugins library](https://github.com/EthanBlackburn/tangent-plugins). Each plugin runs in its own lightweight sandbox.

## Docs

The reference is in [docs/](docs/README.md): [configuration](docs/configuration.md) (`include:`, edges, `tangent config`), [sources](docs/sources.md), [sinks](docs/sinks.md), [plugins](docs/plugins.md), [prebuilt plugins](docs/prebuilt-plugins.md), [state](docs/state.md), [enrichment tables](docs/enrichment-tables.md), [inference](docs/inference.md), [what it costs](docs/cost.md) and [benchmarks](docs/benchmarks.md).

## Packs

A pack is what tangent knows about one provider's logs, and it is one plugin, named after the provider: [aws](/packs/aws), [github](/packs/github), [okta](/packs/okta), [google_workspace](/packs/google_workspace) and [app](/packs/app), with [core](/packs/core), the judge. [packs/README.md](packs/README.md) says what every pack does, how to use them and how to write one.

### Volume and cost

With Claude Haiku 5.5 as the judge ($0.10 a million tokens in, $0.50 out), a day of logs costs about

```
$15 for each million logs  +  $12 for each GB of them
```

[docs/cost.md](docs/cost.md) has the table, the rule's basis and each provider's range.

## How well does it work?

The security packs are measured against [tangent-eval](https://github.com/EthanBlackburn/tangent-eval): 66 cases of attacks and look-alikes across cloud, identity, GitHub, supply chain, SaaS and AI-agent logs, Datadog's 35 recorded AWS attack techniques, and, with the macOS pack, a working week of a developer's Mac, each inside months of ordinary traffic. Its README says what the cases are, what has been measured, and how to run it yourself (`make` there, about $1 on Bedrock or free with a local model).

## Benchmarks

| Source → Sink | Rust | Go | Python |
|---|---|---|---|
| tcp → blackhole | `483.35 MB/s` | `399.54 MB/s` | `76.91 MB/s` |

_Measured in November 2025 on a dedicated 16-core AMD machine._ [docs/benchmarks.md](docs/benchmarks.md) says how to run one.

## Install
```bash
# Homebrew
brew tap telophasehq/telophase
brew install tangent-cli
tangent --version

# with install script
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/EthanBlackburn/tangent/releases/latest/download/tangent-cli-installer.sh | sh
tangent --version

# cargo
cargo install --git https://github.com/EthanBlackburn/tangent tangent-cli

# docker
docker pull ghcr.io/telophasehq/tangent-toolchain
docker run --rm --init ghcr.io/telophasehq/tangent-toolchain <command>

```

## License

Apache-2.0. See `LICENSE`.
