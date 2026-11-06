# Benchmarks

End-to-end throughput with simple log transformation plugins implemented in
[Rust](../examples/rust/src/lib.rs), [Go](../examples/golang/main.go), and
[Python](../examples/python/mapper.py). Each plugin is compiled to WASM and
run inside Tangent.

| Source → Sink | Rust | Go | Python |
|---|---|---|---|
| tcp → blackhole | `483.35 MB/s` | `399.54 MB/s` | `76.91 MB/s` |

_Measured in November 2025 on a dedicated 16-core AMD machine, with
`tangent run` and `tangent bench` pinned to separate cores._ To run one
yourself (`examples/golang` and `examples/python` the same way):

```sh
tangent plugin compile --config examples/rust/tangent.yaml
tangent run --config examples/rust/tangent.yaml &
tangent bench --config examples/rust/tangent.yaml --seconds 20 --connections 16 --payload examples/rust/tests/bench.json --synthesize
```

`tangent bench` sends the payload to the config's source, a `tcp` or
`socket` one (an `sqs` one through `--bucket`), for `--seconds` on
`--connections` connections (`--synthesize` makes varied records from it), and reads the runtime's metrics
([configuration](configuration.md#metrics)) at `--metrics-url`
(`http://127.0.0.1:9184/metrics`) for what came through.
