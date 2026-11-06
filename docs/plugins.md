# Plugins

A plugin is a user-defined function over records, written in Rust, Go or
Python and compiled to a WASM component. It runs in a lightweight sandbox at
near-native speed, with the language's own libraries: no DSL, no
vendor-locked runtime. Plugins are designed to be shared, so a common
transformation (GuardDuty findings to OCSF, say) is written once and shared
in the [community library](https://github.com/EthanBlackburn/tangent-plugins).

1. **Real languages, not DSLs.** Real code beats a DSL: reviewable, testable,
   and easy for a model to write.
2. **Catch breakage before prod.** `tangent plugin test` for correctness,
   `tangent bench` for throughput and latency.
3. **Shareable and sandboxed.** Publish and discover open-source plugins in
   the [Tangent Plugins library](https://github.com/EthanBlackburn/tangent-plugins).
   Each plugin runs in its own sandbox ([below](#what-a-plugin-can-reach)).

## The toolkit

| command | what it does |
|---|---|
| `tangent plugin scaffold --name NAME --lang rust\|go\|python` | a plugin's boilerplate; `--setup` also installs the language's toolchain |
| `tangent plugin compile --config FILE` | every plugin the config names, compiled to WASM into `runtime.plugins_path` (`--plugin NAME` for one; `--wit DIR` against another WIT) |
| `tangent plugin fetch --config FILE` | the plugins a release built, their build's attestation checked; no toolchain ([prebuilt plugins](prebuilt-plugins.md)) |
| `tangent plugin test --config FILE` | each plugin's fixtures (`--plugin NAME` for one; `--enable-http` lets its HTTP calls out) |
| `tangent bench --config FILE --payload FILE` | throughput and latency before deploying ([benchmarks](benchmarks.md)) |
| `tangent run --config FILE` | the runtime (`--once`: exit after one drain, for tests and replays) |
| `tangent config --config FILE` | the config as the runtime reads it, its includes merged ([configuration](configuration.md#printing-a-config)) |
| `tangent state --config FILE ls\|get\|scan\|sql` | the state store, read-only, while the pipeline runs ([state](state.md#reading-the-store)) |

Go plugins compile with TinyGo and Python plugins with componentize-py. The
examples [rust](../examples/rust), [golang](../examples/golang) and
[python](../examples/python) are one small plugin in each.

## What a plugin is

A plugin exports three functions ([assets/wit/processor.wit](../assets/wit/processor.wit), the `mapper` interface):

- `metadata()`: its name and version.
- `probe()`: which records it takes, as selectors over their fields (`has`,
  `eq`, `prefix`, `in`, `gt`, `regex`, combined as `any`, `all` and `none`).
  A record no target of a node takes goes no further
  ([edges](configuration.md#edges)), so a probe is also a filter.
- `process_logs(records)`: the records it took, as views it reads fields
  from, and the NDJSON it returns in their place.

It imports what the host gives it: `log` (the record views), `state` ([state](state.md)),
`config` (its own `config:` from the YAML), `enrichment`
([enrichment tables](enrichment-tables.md)) and `remote` (HTTP calls,
[below](#what-a-plugin-can-reach)).

`tangent plugin compile` writes this tangent's WIT ([assets/wit](../assets/wit))
into each plugin's `.tangent/wit/` before building it, so a plugin builds
against the interface of the tangent that compiles it and keeps no copy of
its own (`--wit <dir>` builds against another). A plugin built before an
interface existed does not import it and runs as before; to call it from an
existing plugin, compile the plugin again. A plugin that imports an interface
needs a runtime that provides it.

## Settings

```yaml
plugins:
  detect:
    module_type: rust            # rust, go or python
    path: ./detect               # the source, relative to the file that names it, for `tangent plugin compile`
    config: { threshold: 5 }     # handed to the plugin as written, read through `config.get`
    tests:
      - { input: tests/input.json, expected: tests/expected.json }
```

| key | default | what it does |
|---|---|---|
| `module_type` | | `rust`, `go` or `python` |
| `path` | | the plugin's source, relative to the file that names it; a plugin with `prebuilt` and no `path` is fetched only |
| `prebuilt` | | where a release publishes it compiled ([prebuilt plugins](prebuilt-plugins.md)) |
| `config` | `{}` | the plugin's own settings, handed on as written |
| `tests` | `[]` | fixtures: `input`, `expected`, and `enrichment_tables` of the test's own ([enrichment tables](enrichment-tables.md#in-tests)) |
| `state_namespace` | `plugins` | its state keys' namespace ([state](state.md#namespaces)) |
| `data` | | a directory mounted read-only at `/data` ([below](#files-data)) |
| `drop_on_error` | `true` | `false` passes a failed call's records on unchanged ([below](#when-a-call-fails)) |
| `group_by`, `max_events`, `expire_after_ms`, `max_age_ms`, `history` | | records a group at a time ([below](#group_by)) |

### Files: `data`

A plugin may read files too: `data: <dir>` mounts a directory, relative to
the file that names it, read-only at `/data` in the guest, for reference data
too big for `config`. [examples/enrich](../examples/enrich) reads MaxMind
GeoLite2 and the cloud providers' ranges that way, and its `setup.sh` fetches
them. Each plugin instance on each worker reads such files into its own
memory; reference data that several plugins read, or that is large, belongs
in an [enrichment table](enrichment-tables.md), which the process loads once.

### `group_by`

A plugin can take its records a group at a time:

```yaml
plugins:
  renderer:
    group_by: [_norm.org, _norm.actor]   # a group per value of these fields
    max_events: 40                       # closes at 40 records,
    expire_after_ms: 30000               # or once nothing has joined it for 30 s (the default),
    max_age_ms: 600000                   # or 10 minutes after it opened, however busy its key (unset by default)
    history: 39                          # the key's latest records, sent again ahead of its next group
```

Records reaching the plugin wait in their key's group, in memory, and are
acked once there. A closed group reaches the plugin whole, in one call, on
workers of the plugin's own, so a slow plugin, or a model after it, never
holds up what feeds it, and one group per key is out at a time. At the end of
input (`--once`, a shutdown) every open group closes; a crash loses the open
ones. A group closed by a shutdown's signal goes on into a
DAG that is stopping: a model after the plugin does not answer it
([inference](inference.md#running)). With `history`, a key's latest records are kept in the
state store (namespace `groups`) and go again ahead of its next group, marked
`"_context": true`, so the plugin sees what came before.

`max_events` unset, only time closes a group; `max_age_ms` unset, only
`max_events` and `expire_after_ms` do. Both clocks are the wall clock.

### When a call fails

When a plugin's call fails (it traps or returns an error), the records it was
given are logged and dropped. `drop_on_error: false` passes them on unchanged
instead, as if the plugin were not there: right for a plugin that only adds
to records, such as context for a model, wrong for a filter.
`tangent plugin test` always drops, so a failed call fails the test. A plugin
instance that traps is made again; `tangent_plugin_traps_total` counts them.

## What a plugin can reach

A plugin is code that reads every record it takes, and a prebuilt one is code
off the network, so this is what it is given:

- **Files:** only its `data` directory, read-only at `/data`. Nothing else on
  the host's filesystem is mounted.
- **Sockets:** none. WASI's socket and name-lookup interfaces are linked but
  every address is denied (wasmtime's default, which tangent does not
  change).
- **HTTP, through the host:** the `remote` interface's `call-batch` makes
  GET, POST, PUT, DELETE and PATCH requests to any URL, on by default.
  `runtime.disable_remote_calls: true` answers every such call with an empty
  `204` instead, so nothing leaves the process; `tangent bench` sets it, and
  `tangent plugin test` keeps calls in unless `--enable-http`. None of the
  packs in [packs/](../packs) calls `remote`.
- **The environment:** the plugin sees the process's environment variables,
  all of them (`inherit_env` in [crates/tangent/src/wasm/engine.rs](../crates/tangent/src/wasm/engine.rs)),
  so it can read any secret exported to `tangent run`: `JUDGE_API_KEY`,
  `OKTA_API_TOKEN`, `AWS_*`. None of the packs reads it.
- **stdout and stderr:** the process's.
- **State and tables:** its own state namespace, and every enrichment table.

Together the last two points mean a plugin can read a secret from the
environment and send it anywhere over `remote`. Until the defaults change,
run third-party plugins with `disable_remote_calls: true`, or with no secrets
in the environment they can see. An attested plugin ([prebuilt plugins](prebuilt-plugins.md))
proves who built it, not what it can reach.
