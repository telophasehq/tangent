# partition

Per-tenant state without tenant handling in the plugin.

`tangent.yaml` partitions the stream by `org_id` and keeps plugin state in a
SQLite state store. The plugin ([src/lib.rs](src/lib.rs)) counts events under
one key, `count`, and remembers each event name under `seen:<event>`. Because
the runtime scopes every `state` call to the current record's partition, those
keys are per org: `acme` and `globex` each get their own `count`, and
`first: true` means "first time *this org* did this".
The plugin is named
`counter` in `tangent.yaml`, so its entries live in the `counter` namespace;
another plugin in the pipeline would only see them if both set the same
`state_namespace`.

```yaml
runtime:
  state:
    backend: sqlite            # or lru
    path: state.sqlite
    default_ttl_ms: 86400000   # a day; 0 = never expire
    max_ttl_ms: 0              # no cap on TTLs (the default caps them at an hour)
  partition:
    key: [org_id]
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
counting on its own. The plugin is handed one org at a time, so a batch comes
out grouped by org in the order the orgs first appeared. Tests run against a
fresh in-process `lru` store with the config's TTL and partition settings.

## Run
```bash
tangent run --config tangent.yaml
```

In another shell, send a few events and watch the output and the store:

```bash
printf '{"org_id":"acme","event":"login"}\n{"org_id":"globex","event":"login"}\n{"org_id":"acme","event":"export"}\n' | nc localhost 9000
cat out.ndjson

tangent state --config tangent.yaml ls
tangent state --config tangent.yaml get count --partition acme
tangent state --config tangent.yaml scan --partition acme --prefix seen:
tangent state --config tangent.yaml sql "select partition, key, value from state order by 1, 2"
```

The store is read while the pipeline holds it; restart the pipeline and the
counts carry on from where they were.

## Using Makefile
```bash
# build and test
make test

# build and run
make run
```
