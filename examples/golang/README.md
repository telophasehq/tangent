# golang

The Go plugin in the [benchmarks](../../docs/benchmarks.md), and the
smallest whole Go plugin to copy: a mapper, built with TinyGo on the
[Go SDK](https://github.com/EthanBlackburn/tangent-sdk-go), that renames the
fields of one service's records.

[main.go](main.go): `selectors` asks for the records whose `source.name` is
`myservice`. `ExampleMapper` reads `msg`, `msg.level`, `seen`, `duration`,
`source.name` and `tags` from each one and returns an `ExampleOutput`, which
goes out as one JSON line:

```
{"msg":"my log","msg.level":"info","seen":5,"duration":6.3,"tags":["tag1"],"source":{"name":"myservice"}}
→ {"message":"my log","level":"info","seen":5,"duration":6.3,"service":"myservice","tags":["tag1"]}
```

[tangent.yaml](tangent.yaml) wires it in: TCP on `:9000` → `golang` →
`blackhole`, a sink that counts what reaches it and keeps none of it.

## Prerequisites
- The `tangent` CLI ([Install](../../README.md#install)).
- Go 1.26 or later.
- [TinyGo](https://tinygo.org/getting-started/install/) that supports your Go:
  a TinyGo release builds only with the Go versions it lists (0.39 stops at
  Go 1.25; 0.43 builds with Go 1.27).
- [wasm-tools](https://github.com/bytecodealliance/wasm-tools)
  (`brew install wasm-tools` or `cargo install wasm-tools`): TinyGo calls it
  to make the component.

`TINYGOROOT`, when set, must be the root of the `tinygo` on your `PATH`; one
left from another TinyGo fails the build with
`package internal/itoa is not in std`. The `setup.sh` that
`tangent plugin scaffold --lang go` writes links `~/.local/bin/tinygo` to the
release it unpacks and sets `TINYGOROOT` in your shell profile: TinyGo started
through a link can't find its root without it
(`could not autodetect root directory`).

## Compile and test
```bash
tangent plugin compile --config tangent.yaml
tangent plugin test --config tangent.yaml
```
Compile writes this tangent's WIT to `.tangent/wit`, runs the SDK's generator,
which writes `json_generated.go` (the JSON encoder for `ExampleOutput`),
builds with `tinygo build -target=wasip2`, deletes `json_generated.go` again
and leaves the plugin in `plugins/golang.cwasm`. The first compile downloads
the Go modules, and every compile needs the network: the generator runs
`go run github.com/mailru/easyjson/easyjson@latest`. The test feeds `tests/input.json` to the plugin, writes what
comes out to `test_out.ndjson` and compares it with `tests/expected.json`; it
ends with `✅ test passed: output matches expected`. Of the fixture's three
records, one has every field, one has only `msg` and `source.name` (the rest
come out as zero values, `tags` as `null`), and one is another service's,
which no selector takes: the test warns that it was dropped, and the expected
output has no line for it.

`make test` does both.

## Run
```bash
tangent run --config tangent.yaml    # or `make run`, which compiles first
```
From another terminal, send it one record it wants and one it doesn't, then
read the counters on the metrics endpoint (`:9184`):
```bash
printf '%s\n' \
  '{"msg":"hello","msg.level":"warn","source":{"name":"myservice"}}' \
  '{"msg":"other","source":{"name":"elsewhere"}}' | nc -w 1 127.0.0.1 9000
curl -s 127.0.0.1:9184/metrics | grep -E '^tangent_(consumer_objects|sink_objects|unrouted_records)'
```
```
tangent_consumer_objects_total 2
tangent_sink_objects_total 1
tangent_unrouted_records_total{node="source:network_input"} 1
```
The `myservice` record goes through the plugin to the sink. The other matches
no selector: `tangent run` warns that it was dropped and
`tangent_unrouted_records_total` counts it.

## Benchmark
With `tangent run` going, from another terminal:
```bash
tangent bench --config tangent.yaml --seconds 20 --connections 16 --payload tests/bench.json --synthesize
```
[tests/bench.json](tests/bench.json) is a template: with `--synthesize` every
record sent is made from it, with random values in its fields. Without
`--synthesize` the payload's records are sent as they are (`--payload
tests/input.json` repeats the fixture's three). It leaves the first 5 seconds
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

## Writing your own
[Agents.md](Agents.md) is the guide to the SDK this plugin uses: what `Wire`
takes, how selectors match, how to read fields and what the output type may
hold.
