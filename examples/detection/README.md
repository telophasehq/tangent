# detection

A Go plugin that flags a service's repeat records: the first record from
`myservice` comes out `{"triggered":false}`, every later one
`{"triggered":true}`, and with a Slack token each repeat is also posted to
Slack.

```
TCP :9000 ──▶ detection (Go plugin) ──▶ out.ndjson
```

The plugin ([main.go](main.go)) asks for records with
`source.name == "myservice"`; a record no plugin asks for is dropped, with a
warning, and counted in `tangent_unrouted_records_total`. For each record it
looks up `seen:<source.name>` in tangent's state store (`state.sqlite`): found
means a repeat. A name that is not there yet is marked with `state.Incr`,
which is atomic, so when several workers get the same new name at once exactly
one of them counts it as the first. An entry expires after 10 minutes, the
store's default TTL, and the name is new again.

## Setup
```bash
./setup.sh
```

It installs `wasm-tools` and TinyGo if they are missing. Installing TinyGo puts
it in `~/.local/opt/tinygo/<version>`, links `~/.local/bin/tinygo`, and adds
`PATH` and `TINYGOROOT` lines to your shell profiles (`~/.zshrc`, `~/.bashrc`,
`~/.profile` and the like); open a new shell afterwards. You also need Go 1.26
or newer and the `tangent` CLI (see [Install](../../README.md#install)).

## Test
```bash
make test
```

That is `./setup.sh`, then:

```bash
tangent plugin compile --config tangent.yaml   # → plugins/detection.cwasm
tangent plugin test --config tangent.yaml
```

```
✅ test passed: output matches expected
```

[tests/input.json](tests/input.json) is three `myservice` records with an
`otherservice` one among them, and [tests/expected.json](tests/expected.json)
is `false`, `true`, `true`: the `otherservice` record is dropped (the test
prints the warning). A test runs on a fresh in-memory store with remote calls
off, so leave `SLACK_ACCESS_TOKEN` unset for it: with a token the plugin tries
to post, gets no answer, and the test fails.

Compiling generates `json_generated.go`, the output's JSON encoder, builds
with TinyGo, and deletes the file again.

## Run
```bash
make run          # compiles, then: tangent run --config tangent.yaml
```

The pipeline reads NDJSON, one record per line, on TCP port 9000 and appends
what the plugin emits to `out.ndjson`. From a second terminal:

```bash
printf '%s\n' \
  '{"msg":"deploy started","source":{"name":"myservice"}}' \
  '{"msg":"deploy started","source":{"name":"myservice"}}' \
  '{"msg":"hello","source":{"name":"otherservice"}}' | nc -w 1 127.0.0.1 9000
cat out.ndjson
```

```
{"triggered":false}
{"triggered":true}
```

The two lines can come out in either order: `runtime.workers` defaults to one
per CPU core and a batch is spread across them. Either way exactly one is
`false`. Send the same lines again and both are `true`. Read what the plugin
has seen while it runs:

```bash
tangent state --config tangent.yaml scan --prefix seen:
```

```
{"ns":"plugins","key":"seen:myservice","value":{"kind":"int","value":1},"expires_at_ms":...,"updated_at_ms":...}
```

The value is how many workers met the name while it was still new: 1, or 2
when the two `myservice` lines landed on two workers at once, which they often
do. Each of them counted, and only the one that got 1 called it new. Delete
`state.sqlite*` (and `out.ndjson`) to start over.

## Post to Slack
```bash
SLACK_ACCESS_TOKEN=xoxb-... make run
```

With a bot token in the environment, each repeat is posted with
`chat.postMessage` to the channel `slack-app-testing`; change `Channel` in
[main.go](main.go) to yours, and invite the bot to it. A post that fails fails
the plugin's call, and the records it was given are logged and dropped, with
Slack's reason in the warning: `failed to post to slack: HTTP 200: "invalid_auth"`.

## Benchmark
```bash
tangent run --config tangent.yaml          # one terminal
tangent bench --config tangent.yaml --seconds 30 --payload tests/bench.json --synthesize
```

`--synthesize` makes each record from the template in
[tests/bench.json](tests/bench.json), all of them `myservice`. The bench sends
over two connections and prints the rate it sent at (`recs/s`); the
`end-to-end` line is read from the pipeline's metrics on `127.0.0.1:9184` and
leaves out the first 5 seconds, a warm-up. On a 14-core Mac:

```
sent_bytes=178819539 sent_records=1399271 MB/s=5.84 MiB/s=5.57 recs/s=45733
end-to-end: uploaded=21.29 MB (21.29 MB uncompressed) over 25.58s → 0.83 MB/s ...
```

Every record is a state read, which bounds the rate. All but the first come
out `{"triggered":true}`, so `out.ndjson` grows by 19 bytes a record; delete it
afterwards.

## Troubleshooting
- `requires go version 1.19 through 1.25, got go1.27`: your TinyGo is older
  than your Go supports. TinyGo 0.43 builds this with Go 1.27; `setup.sh`
  installs 0.43.0 (`TINYGO_VERSION` overrides it) but leaves a TinyGo that is
  already on `PATH` alone.
- `open ~/.local/opt/tinygo/0.39.0/src/...: no such file or directory`,
  `package internal/itoa is not in std` or `could not autodetect root
  directory`: `TINYGOROOT` (`echo $TINYGOROOT`) names another TinyGo than the
  `tinygo` on `PATH` (`tinygo version`). Open a new shell, or set it to the
  installed one, e.g. `export TINYGOROOT=~/.local/opt/tinygo/0.43.0`. A failed
  compile leaves `json_generated.go` behind; the next one replaces it.
