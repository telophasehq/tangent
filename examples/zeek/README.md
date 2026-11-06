# zeek

A Go plugin that maps Zeek `conn` records to [OCSF](https://schema.ocsf.io/1.5.0/classes/network_activity)
1.5.0 Network Activity (class 4001): the two endpoints, protocol, direction
(from `local_orig`/`local_resp`), bytes and packets each way, duration,
community ID and connection state, with the rest of the record (Corelight's
`spcap`, `suri_ids`, `vlan`, ...) under `unmapped`.

It reads Zeek's JSON logs, one record per line, that name their log in
`_path`, as Corelight sensors write them. `ts` may be ISO 8601 text or epoch
seconds (Zeek's JSON default). A record that is not `_path: conn`, or has no
`uid` or `ts`, is dropped and counted in `tangent_unrouted_records_total`.

`main.go` is the mapping, `ocsf.go` the OCSF types it writes, and
`tests/conn.json` the sample: a full Corelight record, a sparse one, and a
`dns` record the plugin drops.

## What you need

- `tangent`
- Go 1.26 or newer and TinyGo: `tangent plugin compile` runs `go run` (to
  generate the output type's JSON encoder) and then `tinygo build`. Tested
  with Go 1.27.3 and TinyGo 0.43.0.
- [wasm-tools](https://github.com/bytecodealliance/wasm-tools)
  (`brew install wasm-tools` or `cargo install wasm-tools`): TinyGo calls it
  to make the component. Without it the build fails with
  `` `wasm-tools component embed` failed ``.

If `TINYGOROOT` is set, it must be the root of the `tinygo` on your `PATH`;
one left from another TinyGo fails the build on a missing standard-library
package under that root, such as
`open .../tinygo/0.39.0/src/crypto/internal/entropy: no such file or directory`.

## Test

```bash
make test
```

This compiles the plugin to `plugins/zeek.cwasm` and runs `tests/conn.json`
through it, expecting `tests/conn_out.json`. It ends with
`✅ test passed: output matches expected`, and logs once that the `dns`
record matched nothing and was dropped. The records it wrote are in
`test_out.ndjson`.

## Run

```bash
make run
```

This compiles the plugin and serves the pipeline in `tangent.yaml`: NDJSON
in on TCP port 9000, through `zeek`, out to a `blackhole` sink, which keeps
nothing (`type: file` with a `path` keeps the records). In a second terminal,
send the sample and read the counters:

```bash
jq -c '.[]' tests/conn.json | nc -w 1 127.0.0.1 9000
sleep 1   # records are counted on arrival, and routed a few ms later
curl -s 127.0.0.1:9184/metrics | grep -E '^tangent_(consumer_objects|unrouted_records)'
```

```
tangent_consumer_objects_total 3
tangent_unrouted_records_total{node="source:network_input"} 1
```

Three records in; the two `conn` records went through `zeek` to the sink,
and the `dns` one was dropped (`tangent run` logs that once). Stop it with
Ctrl-C.

## Benchmark

With `make run` going, in a second terminal:

```bash
tangent bench --config tangent.yaml --seconds 30 --payload tests/bench.json
```

It sends `tests/bench.json`, the full Corelight record, over TCP, leaves the
first 5 seconds out as warmup, and prints what it sent (`recs/s`) and the
end-to-end throughput it read from the pipeline's metrics.

## Files

| | |
|---|---|
| `main.go` | the mapping: selectors, `ZeekMapper`, `Wire` |
| `ocsf.go` | the OCSF 1.5.0 Network Activity fields it writes |
| `tangent.yaml` | the pipeline, and the plugin's test fixtures |
| `tests/` | `conn.json` in, `conn_out.json` expected; `bench.json` for `tangent bench` |
| `Agents.md` | how a Go mapper is written, for a coding agent |
