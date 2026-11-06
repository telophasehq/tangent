# Agents (Mappers) — Go authoring quickstart

> **Audience:** LLMs writing Go mapper plugins for Tangent.
> **Goal:** A deterministic, fast WASM component, built with TinyGo on the
> [Go SDK](https://github.com/EthanBlackburn/tangent-sdk-go), that turns the
> records it asks for into JSON lines.

[main.go](main.go) is the whole pattern; copy it.

## Golden rules
1. **Pure, deterministic code**: no network, filesystem, timers, randomness or goroutines. The plugin is built with `-scheduler=none`.
2. **Read fields through `tangent_sdk.Log`**: do not parse `lv.Log()`, the raw JSON, in the hot path.
3. **A typed output struct**: one named struct, every field JSON-tagged. Do not encode JSON yourself.
4. **Narrow selectors**: ask only for the records you can map; a record no plugin asks for is dropped and counted (`tangent_unrouted_records_total`).
5. **No panics**: a panic traps the call, and the runtime logs and drops every record in that batch.

## Plugin contract
Everything goes through one call in `init()`, with an empty `main()`:

```go
import tangent_sdk "github.com/ethanblackburn/tangent-sdk-go"

func init() {
	tangent_sdk.Wire[ExampleOutput](
		tangent_sdk.Metadata{Name: "myplugin", Version: "0.1.0"},
		selectors,     // which records reach the plugin
		ExampleMapper, // func(tangent_sdk.Log) (ExampleOutput, error)
		nil,           // or a batch handler, with nil in place of ExampleMapper
	)
}

func main() {}
```

- **Handler** `func(tangent_sdk.Log) (T, error)`: called once per record; every record it is given becomes exactly one output line. It cannot skip a record, so filter with selectors.
- **Batch handler** `func([]tangent_sdk.Log) ([]T, error)`: the whole batch at once; it must return exactly one `T` per input.
- **An error** from either fails the batch: the runtime logs `guest error` and drops its records (`drop_on_error: false` under the plugin in `tangent.yaml` passes them on unchanged instead).

## Selectors
```go
var selectors = []tangent_sdk.Selector{
	{
		All:  []tangent_sdk.Predicate{tangent_sdk.EqString("source.name", "myservice")},
		Any:  nil,
		None: []tangent_sdk.Predicate{tangent_sdk.Has("debug")},
	},
}
```
A record reaches the plugin when any selector matches it. A selector matches when every `All` predicate holds, at least one `Any` predicate holds (when it lists any), and no `None` predicate holds.

Predicates: `Has(path)`, `EqString`, `EqInt`, `EqFloat`, `EqBool` `(path, value)`, `Prefix(path, prefix)`, `Regex(path, pattern)`, `InStrings(path, values...)`.

## Reading fields
| Call | Returns |
|---|---|
| `lv.GetString(path)`, `GetInt64`, `GetFloat64`, `GetBool` | a pointer, `nil` when the field is missing or of another type |
| `lv.GetStringList(path)`, `GetInt64List`, `GetFloat64List` | `(list, ok)` |
| `lv.Has(path)`, `lv.Keys(path)` | presence; an object's keys |
| `lv.Log()` | the record as JSON text |

Do not call `lv.Len`: in the SDK version the scaffold pins it calls itself and never returns. Take `len()` of a list instead.

A path is first read as one key of the record, so `"msg.level"` finds a key with a dot in it. Without such a key it walks into objects (`source.name`), and `[n]` takes an item of a list (`tags[0]`).

```go
if s := lv.GetString("source.name"); s != nil {
	out.Service = *s
}
```
The pointer tells a missing field from a zero value.

## Output type
- `T` is a named struct declared in the plugin's package. Nested structs, slices, maps with string keys, pointers and `omitempty` are fine.
- `tangent plugin compile` runs the SDK's generator, which writes `json_generated.go` (an [easyjson](https://github.com/mailru/easyjson) encoder for `T` and the same-package types it uses), builds, and deletes the file after a successful build. Do not write it, edit it or commit it.
- The SDK writes one JSON line per output, in the order returned.

## Tests and fixtures
- `tests/input.json` and `tests/expected.json`, named under the plugin's `tests:` in `tangent.yaml`. The input is a JSON array of records. The expected is a JSON array of the output records in order, or a single object when the plugin emits one record. Neither is NDJSON: an expected file with a record per line fails to parse.
- `tangent plugin test --config tangent.yaml` feeds the input to the plugin, writes what comes out to `test_out.ndjson` and compares it with the expected as parsed JSON (key order and whitespace do not matter); it ends with `✅ test passed: output matches expected`.
- Cover a record with every field, one with fields missing (their zero values in the expected, or nothing for an `omitempty` field), and one the selectors leave out (absent from the expected). A `float64` zero is written `0`, and the test does not take `0.0` for it.

## Make targets
- `make build`: `./setup.sh`, then `tangent plugin compile`, which leaves the plugin in `plugins/<name>.cwasm`.
- `make test`: build, then the fixture test.
- `make run`: build, then `tangent run`, the pipeline in `tangent.yaml`: it listens on tcp 0.0.0.0:9000 and does not read the fixtures.

## Definition of done
- [ ] `make test` passes (compile, then test).
- [ ] `gofmt -l .` prints nothing.
- [ ] `Metadata.Version` is SemVer, bumped when the output changes.
- [ ] Selectors are as narrow as the mapping allows.
- [ ] No `encoding/json` for the output, no goroutines, no network or file I/O.
