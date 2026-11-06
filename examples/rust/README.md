# rust

The Rust plugin in the [benchmarks](../../docs/benchmarks.md), and the
smallest whole Rust plugin to copy: a mapper that renames the fields of one
service's records.

[src/lib.rs](src/lib.rs): `probe()` asks for the records whose `source.name` is
`myservice`. `process_logs` reads `msg`, `msg.level`, `seen`, `duration`, `tags`
and `source.name` from each one and writes one JSON line:

```
{"msg":"my log","msg.level":"info","seen":5,"duration":6.3,"tags":["tag1"],"source":{"name":"myservice"}}
→ {"message":"my log","level":"info","seen":5,"duration":6.3,"service":"myservice","tags":["tag1"]}
```

[tangent.yaml](tangent.yaml) wires it in: TCP on `:9000` → `rust` →
`blackhole`, a sink that counts what reaches it and keeps none of it.

## Setup
Rust must come from [rustup](https://rustup.rs): `tangent plugin compile`
runs `rustup target add wasm32-wasip2`, so a Rust without rustup (Homebrew's
`rust`) cannot build this.
```bash
./setup.sh
```
It adds the `wasm32-wasip2` target and installs `cargo-component` and
`wasm-tools` when they are missing.

## Compile and test
```bash
tangent plugin compile --config tangent.yaml
tangent plugin test --config tangent.yaml
```
Compile writes this tangent's WIT to `.tangent/wit`, generates
`src/bindings.rs` from it (both are gitignored; `src/lib.rs` reads the WIT
through `wit_bindgen::generate!`) and leaves the plugin in `plugins/rust.cwasm`.
The test feeds `tests/input.json` to the plugin, writes what comes out to
`test_out.ndjson` and compares it with `tests/expected.json`; it ends with
`✅ test passed: output matches expected`.

`make test` does both.

## Run
```bash
tangent run --config tangent.yaml
```
`make run` does the same after compiling. From another terminal, send it one
record it wants and one it doesn't, then read the counters on the metrics
endpoint (`:9184`):
```bash
printf '%s\n' \
  '{"msg":"hello","msg.level":"warn","source":{"name":"myservice"}}' \
  '{"msg":"other","source":{"name":"elsewhere"}}' | nc -w 1 127.0.0.1 9000
curl -s 127.0.0.1:9184/metrics | grep -E '^tangent_(consumer_objects|sink_objects|unrouted_records)'
```
The `myservice` record goes through the plugin to the sink. The other matches
no probe: `tangent run` warns that it was dropped and
`tangent_unrouted_records_total` counts it.

## Benchmark
With `tangent run` going, from another terminal:
```bash
tangent bench --config tangent.yaml --seconds 20 --connections 16 --payload tests/bench.json --synthesize
```
[tests/bench.json](tests/bench.json) is a template: with `--synthesize` every
record sent is made from it, with random values in its fields. Without
`--synthesize` the payload's records are sent as they are (`--payload
tests/input.json` repeats the fixture's two). It leaves the first 5 seconds
out as warmup, then prints what it sent, what the sink took, and the plugin's
time per batch:
```
sent_bytes=… sent_records=… MB/s=… MiB/s=… recs/s=…
end-to-end: uploaded=… MB (… MB uncompressed) over …s → … MB/s (… MB/s uncompressed) (amplification x…)
producer bytes (consumed): … MB → … MB/s
guest: bytes_in=… MB, avg_latency=… ms (over … calls)
```
The root README's numbers are this command on a dedicated 16-core machine,
with `tangent run` and `tangent bench` pinned to separate cores; on a laptop
doing other work expect far less.
