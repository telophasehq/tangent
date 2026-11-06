# counter

Per-org counts in plugin state, right however many workers run the plugin.

The plugin ([src/lib.rs](src/lib.rs)) keeps two kinds of keys in a SQLite
state store, each with the org in it: `count:<org>`, the org's events so far,
and `seen:<org>:<event>`, whether the org has done this before. `acme` and
`globex` each get their own count, and `first: true` means "first time *this
org* did this". The entries live in `plugins`, the namespace every plugin in a
pipeline shares.

The count is a single `incr`, atomic on its own. "Seen before?" reads a key
and then writes it, and another worker could write it in between, so the plugin
does both in one transaction: `state::begin()`, the read and the write, then
`state::commit()`, or `state::rollback()` if a call fails.

```yaml
runtime:
  state:
    backend: sqlite            # or lru
    path: state.sqlite
    default_ttl_ms: 86400000   # a day; 0 = never expire
    max_ttl_ms: 0              # no cap on TTLs (the default caps them at an hour)
```

## Setup
```bash
./setup.sh
```

It adds the `wasm32-wasip2` Rust target and installs `cargo-component` and
`wasm-tools` if they are missing.

## Test
```bash
make test
```

That is `./setup.sh`, then:

```bash
tangent plugin compile --config tangent.yaml
tangent plugin test --config tangent.yaml
```

`tests/input.json` interleaves two orgs; `tests/expected.json` shows each org
counting on its own, records in the order they came. Tests run against a fresh
in-process `lru` store with the config's TTLs.

## Run
```bash
make run          # setup and compile, then: tangent run --config tangent.yaml
```

The pipeline reads NDJSON, one record per line, on TCP port 9000 and appends
what the plugin emits to `out.ndjson`. From a second terminal:

```bash
printf '%s\n' \
  '{"org_id":"acme","event":"login"}' \
  '{"org_id":"acme","event":"login"}' \
  '{"org_id":"globex","event":"login"}' \
  '{"org_id":"acme","event":"export"}' | nc -w 1 127.0.0.1 9000
cat out.ndjson
```

```
{"org_id":"acme","event":"login","n":1,"first":true}
{"org_id":"acme","event":"login","n":2,"first":false}
{"org_id":"globex","event":"login","n":1,"first":true}
{"org_id":"acme","event":"export","n":3,"first":true}
```

Yours can differ: `runtime.workers` defaults to one per CPU core and records
are spread across them, so they can be handled in another order than sent.
The lines can come out in another order, and an `n` can go to another record
(the `export` can get `n: 1`). What holds: each org's `n` runs 1, 2, 3, ...
with no gaps or repeats (the `incr`), and exactly one record per org and event
has `first: true` (the transaction).

The entries are in `state.sqlite`, so the counts carry on after a restart
(Ctrl-C stops the pipeline). Read them while the pipeline runs:

```bash
tangent state --config tangent.yaml scan --prefix count:
```

```
{"ns":"plugins","key":"count:acme","value":{"kind":"int","value":3},"expires_at_ms":...,"updated_at_ms":...}
{"ns":"plugins","key":"count:globex","value":{"kind":"int","value":1},"expires_at_ms":...,"updated_at_ms":...}
```

Entries expire a day after their last write (`default_ttl_ms`). An org with
no events for a day starts again at `n: 1`, and an event first seen a day ago
is `first: true` again (its `seen:` key is written once). Set
`default_ttl_ms: 0` to keep them for good.

Delete `state.sqlite*` (and `out.ndjson`) to start counting from zero.
