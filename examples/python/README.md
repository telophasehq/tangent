# python

A mapper plugin written in Python and compiled to a WASM component with [componentize-py](https://github.com/bytecodealliance/componentize-py). [`mapper.py`](mapper.py) takes the records whose `source.name` is `myservice` and maps each to a flat record. [`tangent.yaml`](tangent.yaml) runs it between a TCP source on `:9000` and a `blackhole` sink, which discards what it gets: a pipeline for measuring throughput.

## Setup
You need `tangent`, Python 3.10+ and `componentize-py` on your PATH:
```bash
pipx install componentize-py   # pipx itself: `brew install pipx`
python3 -m venv .venv && source .venv/bin/activate
```
`tangent plugin compile` installs `requirements.txt` into `.venv`. componentize-py bundles the packages of the venv that is active, so a plugin with dependencies (pure-Python ones) has to be compiled from a shell where `.venv` is activated. This example has none.

## Compile
```bash
tangent plugin compile --config tangent.yaml
```
It writes `plugins/python.cwasm`, plus `.tangent/wit`, the WIT it compiled against. It takes about a minute, most of it spent by componentize-py.

## Test
```bash
tangent plugin test --config tangent.yaml
```
This runs [`tests/input.json`](tests/input.json) through the plugin and compares the output with [`tests/expected.json`](tests/expected.json). The output is left in `test_out.ndjson`. The third input record comes from another service, so the probe doesn't match it and the runtime drops it: that is the `WARN ... matched none of its targets` line.

## Run
```bash
tangent run --config tangent.yaml
```
From another terminal, send it one record the probe wants and one it doesn't, then read the counters on the metrics endpoint (`:9184`):
```bash
printf '%s\n' \
  '{"msg":"hi","seen":1,"source":{"name":"myservice"}}' \
  '{"msg":"other","source":{"name":"otherservice"}}' | nc -w 1 127.0.0.1 9000
curl -s 127.0.0.1:9184/metrics | grep -E '^tangent_(consumer_objects|sink_bytes|unrouted_records)_total'
```
```
tangent_consumer_objects_total 2
tangent_sink_bytes_total 97
tangent_unrouted_records_total{node="source:network_input"} 1
```
The sink is `blackhole`, which keeps nothing, so the counters are how you see the plugin work: both records arrived, and the 97 bytes are the one line `mapper.py` wrote for the `myservice` record. The other matched no probe: `tangent run` warns that it was dropped and `tangent_unrouted_records_total` counts it.

## Benchmark
With `tangent run` still going (`tangent bench` reads its metrics on `:9184`), from another terminal:
```bash
tangent bench --config tangent.yaml --seconds 20 --connections 16 --payload tests/bench.json --synthesize
```
[`tests/bench.json`](tests/bench.json) is a template, and `--synthesize` makes each record it sends from that template, the way the [benchmarks](../../README.md#benchmarks) in the main README were run. Without `--synthesize`, `--payload tests/input.json` sends the test records over and over, as they are. The probe drops one of the three.

The first 5 seconds are warmup and not counted. It prints what it sent, what reached the sink (`end-to-end`) and the plugin's time per batch (`guest`). The main README's figure is this command on a dedicated 16-core machine; a laptop doing other work gets far less.

## Makefile
```bash
make test   # compile, then test
make run    # compile, then run
```

## Editor bindings
`mapper.py` imports `wit_world`, a module that componentize-py generates when it compiles. To let your editor resolve that import, generate the module once after compiling, and again after upgrading tangent:
```bash
componentize-py -d .tangent/wit -w processor bindings .
```
The generated files are gitignored.

[Agents.md](Agents.md) is the guide to writing a mapper, for people and for models.
