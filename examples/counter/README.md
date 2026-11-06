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

## Test
```bash
tangent plugin compile --config tangent.yaml
tangent plugin test --config tangent.yaml
```

`tests/input.json` interleaves two orgs; `tests/expected.json` shows each org
counting on its own, records in the order they came. Tests run against a fresh
in-process `lru` store with the config's TTLs.
