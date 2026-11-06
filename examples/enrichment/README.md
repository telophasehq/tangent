# enrichment

A Go plugin that adds each record's country, looked up over HTTP from
[ipinfo.io](https://ipinfo.io):

```
{"service": "myservice", "ip_address": "185.220.101.4"}
  ──▶ enrichment ──▶
{"ip_address": "185.220.101.4", "country": "DE", "service": "myservice"}
```

The plugin ([main.go](main.go)) reads `ip_address` and `service` from every
record in a batch, then makes one request per distinct address,
`https://ipinfo.io/<ip>`, through the runtime's `remote` interface
(`http.CallBatch` in the [Go SDK](https://github.com/EthanBlackburn/tangent-sdk-go)):
the host makes the calls, and the plugin, which has no network of its own,
gets the responses back. Records with the same address share a lookup.

- Only records with `"service": "myservice"` reach the plugin (its selector).
  Others match none of the source's targets: the runtime drops them, warns
  once, and counts them in `tangent_unrouted_records_total`.
- The plugin writes a record of its own, `ip_address`, `country` and `service`,
  not the input with a field added. A record without `ip_address` comes out
  with both empty.
- A lookup that fails (no network, ipinfo's rate limit, a body that is not
  JSON) leaves that address's `country` empty; the record still goes on.
  ipinfo.io answers without a token, up to its free limit.

For lookups that never leave the machine, see [examples/enrich](/examples/enrich)
(GeoLite2 and the cloud providers' ranges, read from disk) and the runtime's
[enrichment tables](/README.md#enrichment-tables).

## Setup

You need Go 1.26 or newer, TinyGo 0.43 or newer (the Go plugin compiler) and
`wasm-tools`.

```bash
./setup.sh
```

installs `wasm-tools` and TinyGo if they are missing. Installing TinyGo puts
it under `~/.local/opt/tinygo/` and adds `~/.local/bin` to `PATH` and
`TINYGOROOT` to your shell profile, which only a new shell reads: open a new
terminal before you compile. Until then the compile cannot find TinyGo, and a
`make test` that has just installed it fails the same way. With both on `PATH`
already, `setup.sh` changes nothing.

If an older `setup.sh` installed TinyGo 0.39, your profile's `TINYGOROOT`
may still name it, and the compile fails with
`open ~/.local/opt/tinygo/0.39.0/src/...: no such file or directory`. Point
it at the new one: `export TINYGOROOT=~/.local/opt/tinygo/0.43.0`.

## Test

```bash
tangent plugin compile --config tangent.yaml   # → plugins/enrichment.cwasm
tangent plugin test --config tangent.yaml --enable-http
```

`tests/expected.json` holds ipinfo.io's answers for the two addresses in
`tests/input.json`, so the test makes its two calls: `--enable-http`.
Without it, `tangent plugin test` answers every call with an empty `204`,
the countries come back empty, and the test fails on them.

## Run

```bash
tangent run --config tangent.yaml              # listens on :9000, writes out.ndjson
```

In a second terminal, send records, one JSON object per line (the `tcp`
source reads NDJSON, so the fixture's array goes through `jq -c`):

```bash
jq -c '.[]' tests/input.json | nc -w 1 127.0.0.1 9000
echo '{"service": "myservice", "ip_address": "8.8.8.8"}' | nc -w 1 127.0.0.1 9000
cat out.ndjson
```

```
{"ip_address":"46.17.46.213","country":"RU","service":"myservice"}
{"ip_address":"185.220.101.4","country":"DE","service":"myservice"}
{"ip_address":"8.8.8.8","country":"US","service":"myservice"}
```

Lines come out in no fixed order: the workers run in parallel.

## Using the Makefile

```bash
make test    # ./setup.sh, compile, then the test with --enable-http
make run     # ./setup.sh, compile, then tangent run
```
