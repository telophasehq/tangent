# Agents (Mappers) — Authoring Guide for LLMs

> **Audience:** This document is for language models contributing mapper plugins (a.k.a. “agents”).
> **Goal:** Produce deterministic, fast, and correct WASM mappers that subscribe to specific logs, map them to a target schema, and emit NDJSON.

* [Golden rules](#golden-rules)
* [Mapper contract (what you must implement)](#mapper-contract-what-you-must-implement)
* [Output model (struct) rules](#output-model-struct-rules)
* [Probe design (subscribe narrowly)](#probe-design-subscribe-narrowly)
* [Processing pattern (batch transform)](#processing-pattern-batch-transform)
* [Reading fields safely](#reading-fields-safely)
* [Encoding & output](#encoding--output)
* [Error handling policy](#error-handling-policy)
* [Tests & fixtures](#tests--fixtures)
* [Performance requirements](#performance-requirements)
* [Style & structure](#style--structure)
* [PR checklist (Definition of Done)](#pr-checklist-definition-of-done)
* [Anti‑patterns](#anti-patterns)
* [Minimal template (fill‑in skeleton)](#minimal-template-fill-in-skeleton)
* [Reference example](#reference-example)

---

## Golden rules

1. **Deterministic & pure:** No network, filesystem, timers, randomness, or goroutines. Treat the mapper as a pure transform.
2. **Typed > dynamic:** Use typed structs for output; avoid `map[string]any` in hot paths.
3. **Segment JSON only:** Use `github.com/segmentio/encoding/json` for all encoding.
4. **Own your data:** `cm.List` is a view; copy to an owned slice before iterating.
5. **Narrow probes:** Subscribe only to records you can transform; this saves CPU and reduces branching.
6. **NDJSON out:** Emit exactly one JSON line per input record you accept.
7. **WASM‑safe performance:** Reuse buffers, minimize allocations, no reflection in hot loops.
8. **Tests drive correctness:** Provide `tests/input.json` and `tests/expected.json` (NDJSON). Tests must pass locally and in CI.

---

## Mapper contract (you MUST implement)

Implement these exports in Go:

* **Metadata** → `mapper.Exports.Metadata`: returns `mapper.Meta{Name, Version}`

  * Version with SemVer (`MAJOR.MINOR.PATCH`).
* **Probe** → `mapper.Exports.Probe`: returns a list of `mapper.Selector` with `All/Any/None` predicates (use `mapper.PredEq`).
* **ProcessLogs** → `mapper.Exports.ProcessLogs`: input `cm.List[log.Logview]` → output `cm.Result[cm.List[uint8], cm.List[uint8], string]`.

Also include:

* `func Wire()` wiring the above.
* `func init() { Wire() }` and an empty `main()`.

**Required imports used in this repo:**

```go
"golang/internal/tangent/logs/log"
"golang/internal/tangent/logs/mapper"
"golang/tangenthelpers"
"github.com/segmentio/encoding/json"
"go.bytecodealliance.org/cm"
```

---

## Output model (struct) rules

* Define a single `struct` representing the normalized record you emit.
* All fields **must** have JSON tags and stable names.
* Optional fields are represented by zero values (e.g., `""`, `0`) or omitted depending on your schema—be consistent with tests.

**Example:**

```go
type ExampleOutput struct {
  Msg      string   `json:"message"`
  Level    string   `json:"level"`
  Seen     int64    `json:"seen"`
  Duration float64  `json:"duration"`
  Service  string   `json:"service"`
  Tags     []string `json:"tags"`
}
```

At the top of your file, include a **mapping spec block** (comment) for humans and tooling:

```go
// MappingSpec:
// source.name         -> service        (required)
// msg                 -> message        (optional; default "")
// msg.level           -> level          (optional; default "")
// seen                -> seen           (optional; default 0)
// duration            -> duration       (optional; default 0.0)
// tags[]              -> tags[]         (optional; may be null or omitted)
// Drop conditions: if source.name != "myservice", record is not encoded.
```

---

## Probe design (subscribe narrowly)

Use `mapper.PredEq` within `All/Any/None`. Prefer **narrow** filters to reduce work in `ProcessLogs`.

**Pattern:**

```go
mapper.Exports.Probe = func() cm.List[mapper.Selector] {
  return cm.ToList([]mapper.Selector{
    {
      All: cm.ToList([]mapper.Pred{
        mapper.PredEq(cm.Tuple[string, mapper.Scalar]{
          F0: "source.name",
          F1: log.ScalarStr("myservice"),
        }),
      }),
      Any:  cm.ToList([]mapper.Pred{}),
      None: cm.ToList([]mapper.Pred{}),
    },
  })
}
```

---

## Processing pattern (batch transform)

* Copy `input` to an owned slice (don’t retain views).
* Reuse a pooled buffer for NDJSON.
* For each record:

  * Read fields via `tangenthelpers`.
  * Populate your typed output.
  * `json.NewEncoder(buf).Encode(out)` (one line).
* On encode failure: **fail the batch** by `res.SetErr(err.Error())`.
* After the loop: `res.SetOK(cm.ToList(buf.Bytes()))` and return the buffer to the pool.

**Buffer pool (required):**

```go
var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
```

---

## Reading fields safely

Use **only** these helpers (available in this repo):

* `tangenthelpers.GetString(lv, "path") *string`
* `tangenthelpers.GetInt64(lv, "path") *int64`
* `tangenthelpers.GetFloat64(lv, "path") *float64`
* `tangenthelpers.GetStringList(lv, "path") ([]string, bool)`

**Pattern:**

```go
if s := tangenthelpers.GetString(lv, "msg.level"); s != nil {
  out.Level = *s
}
```

> The pointer return lets you distinguish missing vs zero values.

---

## Encoding & output

* Always use **`github.com/segmentio/encoding/json`**.
* Encode **one JSON object per input record** (NDJSON).
* Return the entire NDJSON buffer for the batch via `SetOK`.

**Do NOT:**

* Use `encoding/json` (stdlib) unless explicitly required.
* Manually concatenate JSON strings.
* Emit partial lines or trailing commas.

---

## Error handling policy

Default policy is **strict**:

* If any encoding or transform error occurs, set `res.SetErr(err.Error())` and return.
* If you intentionally skip a record (e.g., it doesn’t meet your mapping contract), simply **don’t encode** it; keep processing the rest.
* No panics. No logging side‑effects from the mapper.

If a mapper needs a different policy (e.g., “skip bad rows”), document it clearly in a comment block and tests must reflect that behavior.

---

## Tests & fixtures

Each mapper directory **must** include:

```
tests/input.json     # NDJSON input
tests/expected.json  # NDJSON expected output (one line per output record)
```

**Rules:**

* Input and expected are both **NDJSON**.
* Line order in expected **matches** encoded order.
* Include at least:

  * A record with all fields set.
  * A record with some fields missing (assert defaults/omissions).
  * A record that should be **dropped** by the probe (absent from expected).
* Keep fixtures small (3–8 lines total).

**Make targets used by CI:**

* `make test` — runs fixture tests.
* `make run` — local development runner against fixtures.
* `make build` — produces the `.wasm` artifact.

> Tests **must** pass without modifying the runtime or relying on external resources.

---

## Performance requirements

* Use a **`sync.Pool`** for buffers; call `buf.Reset()` before reuse.
* Copy `cm.List` to a fresh slice: `items := append([]log.Logview(nil), input.Slice()...)`.
* Avoid reflection and dynamic structures in the hot path.
* Keep per‑record work O(number of fields you read).
* Prefer early returns or `continue` when a record is to be dropped.
* Keep globals minimal and read‑only (except the buffer pool).
* No goroutines, channels, or blocking calls (WASM sandbox).

---

## Style & structure

* Go code must be `gofmt`‑clean; idiomatic names (`out`, `lv`, `buf`).
* JSON tags required for every output field.
* File header comment includes: name, version, target schema, and the **MappingSpec** table.
* Keep functions small; the main loop lives in `ProcessLogs`.
* Comment **why** for non‑obvious decisions (e.g., copying lists, strict failure).

---

## PR checklist (Definition of Done)

**The PR is ready when all items are true:**

* [ ] `Metadata.Name` set to a unique, descriptive name; `Version` uses SemVer.
* [ ] `Probe` is as **narrow** as possible and correctly targets the intended logs.
* [ ] `ProcessLogs` copies `cm.List`, uses `tangenthelpers`, and encodes with Segment JSON.
* [ ] Output struct is typed with JSON tags; no unused fields.
* [ ] NDJSON semantics: exactly one `Encode` per emitted record; no extra whitespace concerns.
* [ ] Buffer pooling implemented and returned to pool.
* [ ] **No** imports of `encoding/json` (stdlib) or network/file packages.
* [ ] `tests/input.json` and `tests/expected.json` are present, correct, and small.
* [ ] `make build`, `make test`, and (optionally) `make run` succeed locally.
* [ ] MappingSpec comment updated and consistent with the implementation.
* [ ] No panics; errors use `res.SetErr(err.Error())` for batch failure when needed.

---

## Anti‑patterns

* ❌ Using `encoding/json` (stdlib) instead of Segment’s encoder.
* ❌ Retaining `cm.List` or other borrowed buffers past the call.
* ❌ Building JSON via string concatenation.
* ❌ Relying on reflection, `interface{}`, or `map[string]any` for output.
* ❌ Logging, networking, filesystem I/O, or goroutines inside the mapper.
* ❌ Broad probes with heavy in‑function filtering.

