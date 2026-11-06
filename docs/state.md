# State

Plugins keep state through the `state` interface: `state.Get`, `state.Set`,
`state.Incr`, `state.Del`, and `state.Scan` for a key range, each write with
an optional TTL. That state lives in the runtime's **state store**,
configured under `runtime.state`:

```yaml
runtime:
  state:
    backend: sqlite        # or lru (in-process, bounded); distributed backends slot in here
    path: state.sqlite
    default_ttl_ms: 600000 # 0 = never expire
    max_ttl_ms: 3600000    # cap on any TTL; 0 = no cap
    max_entries: 100000    # lru only: evict the least recently used past this
```

| key | default | what it does |
|---|---|---|
| `backend` | `sqlite` | `sqlite`: durable, queryable with any SQLite client, shared by every worker. `lru`: in-process, bounded by `max_entries`, gone on exit; for tests, benchmarks and pipelines whose state may start over |
| `path` | `state.sqlite` | the SQLite file, relative to the config you run, even when an included file sets it |
| `default_ttl_ms` | `600000` (10 min) | the TTL of a write that gives none; `0`, entries never expire |
| `max_ttl_ms` | `3600000` (1 h) | the longest TTL a write may ask for; `0`, no cap |
| `max_entries` | `100000` | `lru` only: the entries it holds before evicting the least recently used |
| `lock_timeout_ms` | `30000` | how long `tangent run` waits for another process's lock on the file (`state.sqlite.lock` beside `state.sqlite`) before it stops, asking whether another tangent is running |
| `read_only` | `false` | open another pipeline's file without its lock ([below](#another-pipeline-reading-it)) |

A write's `ttl-ms` of none takes `default_ttl_ms`, and `0` never expires.
[packs/core](../packs/core/ingest.yaml) sets both TTLs to `0`: what baseline
learns is kept.

## Namespaces

Entries are keyed by namespace and key. Plugins share one namespace,
`plugins`, so what one plugin writes the next can read; a read, and a write
that depends on it, go in a transaction when several workers run the plugin
([below](#transactions)). Give a plugin a `state_namespace` of its own when
it must not share, such as two instances of one plugin with different
configs:

```yaml
plugins:
  detect_login:  { module_type: rust, path: ./detect, state_namespace: login }
  detect_export: { module_type: rust, path: ./detect, state_namespace: export }
```

A plugin's `group_by` history is kept under the namespace `groups`
([plugins](plugins.md#group_by)), and the `github_audit`, `okta` and
`google_workspace` sources keep their place (a cursor) in the store too.

## Transactions

Each state call stands alone: two workers running a plugin can interleave
one's read with the other's write. A plugin that reads state and writes what
depends on it makes them one transaction: `state.begin()`, its reads and
writes, then `state.commit()`, or `state.rollback()`. They see each other's
writes and land together, or not at all, however many workers run the
plugin. A plugin that keeps state per tenant puts the tenant in its keys:
[examples/counter](../examples/counter) counts events per org under
`count:<org>`, and marks "first time this org did X" in a transaction. No
other state call runs while one is open, so begin late, after the parsing,
and commit early. A second `begin`, or a remote call, inside one fails, and a
call that returns with one open has it rolled back, with a warning: only what
a plugin commits is kept.

## Reading the store

The store is readable while the pipeline runs:

```bash
tangent state --config tangent.yaml ls                          # keys per namespace
tangent state --config tangent.yaml get count:acme              # one entry (--ns defaults to the only namespace)
tangent state --config tangent.yaml scan --prefix user: --limit 50
tangent state --config tangent.yaml sql "select ns, count(*) from state group by 1"   # sqlite backend only
```

## Another pipeline reading it

Another pipeline can read it too: `read_only: true` under `runtime.state`
opens the same SQLite file without the lock and creates nothing, so one
pipeline learns while another reads what it learned. The file must exist and
hold the writer's table, so the writer starts first. Its writes fail, and an
expired entry reads as missing; a pipeline whose runtime writes state (a
source that polls an API) is refused.
