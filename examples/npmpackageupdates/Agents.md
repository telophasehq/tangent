# Agents — changing this Go plugin

> **Audience:** language models (and people) editing `main.go`.
> **Goal:** a plugin that stays a pure, fast transform: one alert per npm version it is given.

## Contract

`main.go` registers itself with the Go SDK, `github.com/ethanblackburn/tangent-sdk-go` (imported as `tangent_sdk`):

```go
func init() {
	tangent_sdk.Wire[Alert](Metadata, selectors, Detect, nil)
}

func main() {}
```

* `Metadata` is `tangent_sdk.Metadata{Name, Version}`; bump `Version` (SemVer) when the output changes.
* `selectors` is a `[]tangent_sdk.Selector`, each with `All`, `Any` and `None` lists of predicates:
  `Has`, `EqString`, `EqInt`, `EqFloat`, `EqBool`, `Prefix`, `Regex`, `InStrings`.
  A record that matches no selector never reaches the plugin; when no other target takes it, the
  runtime drops it, counts it in `tangent_unrouted_records_total` and logs a warning.
* `Detect` is `func(tangent_sdk.Log) (Alert, error)`. It is called once per record and `Wire` writes
  each `Alert` it returns as one JSON line. There is no way to skip a record: to say "nothing here",
  return an `Alert` that says so (`triggered: false`). An error fails the whole batch.
* `Alert` is a plain struct with `json` tags (strings, bools, numbers, slices, `map[string]…`,
  nested structs). `tangent plugin compile` generates its JSON encoder (`json_generated.go`) and
  deletes the file once the build succeeds; do not commit it.

## Reading fields

Paths are dotted: `npm.dist.shasum`, `npm._npmUser.trustedPublisher.id`.

* `lv.GetString`, `lv.GetInt64`, `lv.GetFloat64`, `lv.GetBool` return a pointer: `nil` when the
  field is missing or holds another type.
* `lv.GetStringList`, `lv.GetInt64List`, `lv.GetFloat64List` return the list and whether it exists.
* `lv.Has(path)` and `lv.Keys(path)` for presence and an object's keys.

## Rules

* No network, filesystem, goroutines, timers or randomness: the plugin runs in a WASM sandbox and
  the runtime does the I/O (here the `npm_registry` source fetches the registry).
* Keep selectors narrow, so the plugin sees only the records it understands.

## Tests

`tests/input.json` and `tests/expected.json` are JSON arrays. `tangent plugin test --config tangent.yaml`
runs the input through the compiled plugin and compares its output with the expected records
(key order does not matter; a single output record is compared as an object, not a one-item array).
Keep a record the selector drops, a record that triggers and one that does not.
