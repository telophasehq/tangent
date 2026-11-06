# Agents: writing a Go mapper for Tangent

For a coding agent changing this plugin, or writing one like it. A mapper is a
Go `package main` compiled to WASM by TinyGo: Tangent hands it the records its
selectors match, and it returns one output record for each.

## The contract

Everything goes through `tangent_sdk.Wire`, called from `init()`:

```go
import tangent_sdk "github.com/ethanblackburn/tangent-sdk-go"

var metadata = tangent_sdk.Metadata{Name: "zeek-conn → ocsf.network_activity", Version: "0.2.0"}

var selectors = []tangent_sdk.Selector{{
	All: []tangent_sdk.Predicate{
		tangent_sdk.Has("uid"),
		tangent_sdk.Has("ts"),
		tangent_sdk.EqString("_path", "conn"),
	},
}}

func Map(lv tangent_sdk.Log) (*NetworkActivity, error) { /* ... */ }

func init() { tangent_sdk.Wire[*NetworkActivity](metadata, selectors, Map, nil) }
func main() {}
```

- **Metadata**: a name and a SemVer version. Bump the version when the output changes.
- **Selectors**: a record reaches the plugin when it matches any selector; a
  selector matches when all of `All`, one of `Any` and none of `None` hold.
  Predicates: `Has`, `EqString`, `EqInt`, `EqFloat`, `EqBool`, `Prefix`,
  `Regex`, `InStrings`. A record no selector matches is dropped, logged once
  and counted in `tangent_unrouted_records_total`.
- **Handler**: `func(tangent_sdk.Log) (T, error)`, one call per record (or a
  batch handler `func([]tangent_sdk.Log) ([]T, error)` as the fourth argument).
  An error fails the whole batch, so select narrowly enough that every record
  you are handed maps: this plugin selects on `ts` because it cannot map a
  record without one.

## Reading fields

`lv.GetString`, `GetInt64`, `GetFloat64`, `GetBool` return a pointer, `nil`
when the field is missing or has another type; `GetStringList`,
`GetInt64List`, `GetFloat64List` return `(values, ok)`; `Has` and `Keys` too.
A JSON number is an int when written without a fraction and a float with one,
and `GetFloat64` returns `nil` for an int: read a number that can be either
with both (see `number` in `main.go`).

## Writing the output

The output is the handler's type `T`, a struct with a `json` tag on every
field. You do not encode it: `tangent plugin compile` generates an easyjson
encoder for it (`json_generated.go`, deleted after the build), so the usual
tags hold: a field with `omitempty` is left out when empty, and one without it
is always written (a nil pointer as `null`). The plugin itself needs no
`encoding/json`.

## Rules

- A pure transform: no goroutines, network, files, clocks or randomness.
- Keep dependencies small. TinyGo 0.43 cannot build `hash/maphash` with Go
  1.27, so a dependency that imports it (apache/arrow-go, for one) fails the
  build; that is why `ocsf.go` declares the OCSF types instead of importing
  go-ocsf.
- `gofmt` clean.

## Tests

`tangent.yaml` lists fixtures under `plugins.zeek.tests`: `input` is a JSON
array of records, `expected` the output (an object for one record, an array
for several). Key order does not matter. Keep a full record, a sparse one and
one the selectors drop; `tests/conn.json` has all three.

```bash
make test   # compile, then run the fixtures
make run    # compile, then listen for NDJSON on port 9000
```
