# Configuration

A pipeline is one YAML file, or several merged into one ([below](#including-files)),
run by one process: `tangent run --config tangent.yaml`. Its top-level keys:

| key | what it holds | page |
|---|---|---|
| `runtime` | workers, batching, where compiled plugins live, the state store | [below](#runtime), [state](state.md) |
| `sources` | where records come from, each by a name | [sources](sources.md) |
| `plugins` | WASM plugins, each by a name | [plugins](plugins.md) |
| `inference` | model nodes, each by a name | [inference](inference.md) |
| `sinks` | where records go, each by a name | [sinks](sinks.md) |
| `enrichment_tables` | reference data loaded once for the process | [enrichment tables](enrichment-tables.md) |
| `dag` | the edges between those names | [below](#edges) |
| `include` | other files merged into this one | [below](#including-files) |

A top-level key that starts `x-` is the file's own, a place for YAML anchors
(`x-prompt: &prompt judge.md`), and is neither read nor merged. Any other key
is an error: `includes:` at the top, `bind_adress` in a source.

## Runtime

```yaml
runtime:
  workers: 8                   # one per CPU when unset
  batch_size: 256              # KiB: a batch moves on at this size...
  batch_age: 5                 # ...or this many milliseconds after it began
  plugins_path: plugins/       # where `tangent plugin compile` and `fetch` put plugins, and `tangent run` reads them
  disable_remote_calls: false  # true: a plugin's HTTP calls get an empty 204, and nothing leaves the process
  state:                       # the state store: state.md
    backend: sqlite
    path: state.sqlite
```

`runtime` may be left out, for its defaults: the values above, with `workers`
one per CPU. `runtime` paths are relative to the config you run, even when an
included file sets them. What a plugin can reach, and what
`disable_remote_calls` turns off, is in [plugins](plugins.md#what-a-plugin-can-reach).

## Including files

A config can span several files. `include:` lists files merged into the one
that names it, each path relative to that file:

```yaml
include:
  - packs/core/ingest.yaml
  - packs/core/judge.yaml
  - packs/app/ingest.yaml
plugins:
  app: { config: { extra_sources: [billing] } }   # one setting of a plugin an included file defines
```

- Maps merge key by key at any depth, so a file can add a source, a plugin,
  or one setting of another file's plugin.
- A file wins over the files it includes, and its `key: null` (or `key:`
  alone, or an unset `${VAR}` alone) removes what they set there; the field's
  default applies. A null where no file set anything is no value either, so
  `model:` alone is a missing `model`, not an empty one. But in a plugin's
  `config` and an inference node's `extra`, which tangent hands on as
  written, a null is a value like any other: `extra: { response_format: ~ }`
  sends the null.
- Two files that don't include each other must agree on any value that is
  not a map, and on whether a key holds a map at all. When they don't, a file
  that includes both settles it; without one, loading fails naming the key
  and both files.
- A value reads as its file wrote it: `model: 1.10` is `"1.10"` to a string
  field, with `include:` or without, and two files agree only when they wrote
  the same.
- A plugin two files define the same way is one plugin, loaded once per
  worker however many files define it; an enrichment table two files define
  the same way is one table, loaded once for the process.
- Lists are values, except `dag`: every file's edges add up, and an edge given
  twice counts once.
- A plugin's `path`, `data` and `tests`, an inference node's `prompt_file`
  and `schema_file`, and an enrichment table's `path` are relative to the
  file that names them. `runtime` paths stay relative to the config you run,
  and source and sink paths to the working directory, as before.
- A file included twice (by two packs, say) is read once, and what each
  includer changes in it holds; a cycle is an error. `${VAR}` works in every
  file, in `include` paths too, and `${VAR:-default}` is `default` when the
  variable is unset or empty.
- `runtime` may be left out, for its defaults. An unknown key is an error:
  `includes:` at the top, `bind_adress` in a source. A top-level key that
  starts `x-` is the file's own, a place for YAML anchors
  (`x-prompt: &prompt judge.md`), and is neither read nor merged.

Unknown keys aside, a config without `include:` reads as it always has.

## Printing a config

`tangent config --config FILE` prints the merged config, every default
filled in, after the files it came from. Secret fields (tokens, `api_key`,
`webhook_url`, passwords) are left out, so it is for reading, not for
running.

## Edges

An edge names its nodes and nothing else, `from: cloudtrail` and
`to: [aws, lake]`:

```yaml
dag:
  - from: cloudtrail
    to: [aws]
  - from: aws
    to: [lake, baseline_builder, baseline_renderer]
```

A name is one node, so no source, plugin,
inference node and sink share one, and a config where two do fails to load,
naming both. `tangent run` checks the DAG before it starts: an edge to a node
no file defines is an error, and so is one out of a sink or into a source. It
warns about a source with no edge out, and names once a source whose records
none of its targets take, as when no included plugin's probe claims a
record's `source`; `tangent_unrouted_records_total` counts every record
dropped that way, per node.

## Metrics

`tangent run` serves Prometheus metrics at `0.0.0.0:9184/metrics`: counts
and sizes, never records. Among them:

| metric | what it counts |
|---|---|
| `tangent_consumer_bytes_total`, `tangent_consumer_objects_total` | what the sources read |
| `tangent_guest_seconds`, `tangent_guest_bytes_total`, `tangent_plugin_traps_total` | plugin calls: their time, the bytes fed to them, the instances that trapped and were made again |
| `tangent_unrouted_records_total` | records no target of a node took ([above](#edges)) |
| `tangent_sink_bytes_total`, `tangent_sink_bytes_uncompressed_total`, `tangent_sink_objects_total` | what the sinks wrote ([sinks](sinks.md#metrics)) |
| `tangent_sink_records_unfit_total` | records a Parquet schema could not read, `fate="kept"` or `"dropped"` ([sinks](sinks.md#rolling_file)) |
| `tangent_wal_pending_bytes`, `tangent_wal_pending_files`, `tangent_wal_sealed_bytes_total`, `tangent_wal_sealed_files_total` | the write-ahead log behind `s3` and `rolling_file` |
| `tangent_inflight` | batches handed to the sinks and not yet kept |

`tangent bench` reads them from there ([benchmarks](benchmarks.md)).
