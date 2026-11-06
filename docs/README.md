# Tangent docs

The [README](../README.md) says what tangent is for and what it costs. These
pages are the reference.

**The pipeline**

- [Configuration](configuration.md): one config, `include:`, `${VAR:-default}`, `tangent config`, edges, the runtime block, metrics
- [Sources](sources.md): `tcp`, `socket`, `file`, `sqs`, `msk`, `sqlite`, the API sources, `github_webhook`, `npm_registry`
- [Sinks](sinks.md): `file`, `rolling_file` (Parquet), `s3`, `sqlite`, `slack`, `blackhole`
- [Plugins](plugins.md): the toolkit, Rust, Go and Python, `group_by`, `drop_on_error`, what a plugin can reach
- [Prebuilt plugins](prebuilt-plugins.md): `tangent plugin fetch` and attestations
- [State](state.md): the state store, TTLs, transactions, `tangent state`
- [Enrichment tables](enrichment-tables.md): `mmdb`, `cidr`, `document`, loaded once per process
- [Inference](inference.md): model nodes, backends, schemas, logfmt, retries

**Watching logs with a model**

- [Packs](../packs/README.md): what each provider's pack does, and how to use and write one
- [packs/core](../packs/core/README.md): the record, the profile, the baseline and the judge
- [What it costs](cost.md): the rule, per provider, and how the estimates were made
- [tangent-eval](https://github.com/EthanBlackburn/tangent-eval): what the packs are measured against

**Speed**

- [Benchmarks](benchmarks.md)
