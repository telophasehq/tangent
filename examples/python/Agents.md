# Agents (Mappers) — Authoring Guide for LLMs (Python)

> **Audience:** Language models contributing **Python** mapper plugins (a.k.a. “agents”).
> **Goal:** Produce deterministic, fast, and correct WASM mappers that subscribe to specific logs, map them to a target schema, and emit NDJSON (**bytes**).

* [Golden rules](#golden-rules)
* [Mapper contract (what you must implement)](#mapper-contract-what-you-must-implement)
* [Output model (dict) rules](#output-model-dict-rules)
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
* [Reference example](mapper.py)

---

## Golden rules

1. **Deterministic & pure:** No network, filesystem, timers, randomness, threads, or background work. Treat the mapper as a pure transform.
2. **Stable, explicit shape:** Emit a consistent JSON shape (fixed keys/types). Prefer a small, explicit dict to dynamic structures.
3. **Own your lifetimes:** Treat `logs` and each `Logview` as **ephemeral**; do not retain them after `process_logs` returns. Always use `with lv:` when iterating a `Logview`.
4. **Narrow probes:** Subscribe only to records you can transform; avoid doing selection inside `process_logs` if the probe can express it.
5. **NDJSON out:** Encode **one line per emitted record**; return a single `bytes` buffer containing all lines.
6. **WASM‑safe performance:** Use a local `bytearray` buffer, minimize allocations, avoid reflection/`__dict__` walks and large intermediates.
7. **Tests drive correctness:** Provide `tests/input.json` and `tests/expected.json` (JSON, see [Tests & fixtures](#tests--fixtures)). Keep them small and representative.
8. **No surprises:** No global mutable state (beyond module constants). No prints/logging from the mapper.

---

## Mapper contract (what you must implement)

Implement a class that extends the runtime base and the three required methods:

```py
from typing import List
import json
import wit_world
from wit_world.exports import mapper
from wit_world.imports import log

class Mapper(wit_world.WitWorld):
    def metadata(self) -> mapper.Meta: ...
    def probe(self) -> List[mapper.Selector]: ...
    def process_logs(self, logs: List[log.Logview]) -> bytes: ...
```

* **`metadata()`**: return `mapper.Meta(name="<unique-name>", version="<semver>")`.
* **`probe()`**: return a list of `mapper.Selector` with predicates in `any` / `all` / `none`. Use `mapper.Pred_Eq((path, log.Scalar_*))`.
* **`process_logs()`**: accept `List[log.Logview]`, return **`bytes`** (NDJSON for the entire batch).

> The runtime wires these methods via WIT; do **not** rename or change signatures. `raise Err("why")` inside `process_logs` fails the batch (see [Error handling policy](#error-handling-policy)).

**Required imports used in this repo:**

```py
import json
import wit_world
from wit_world.exports import mapper
from wit_world.imports import log
from typing import List
```

---

## Output model (dict) rules

* Emit a **single, stable dict** per output record with fixed keys and types.
* Optional fields: use sensible defaults (`""`, `0`, `0.0`, `None` for nullable arrays) or **omit** the key—be consistent with tests.
* Prefer simple, flat shapes unless your target schema demands nesting.

**Example dict schema:**

```py
out = {
    "message": "",      # str
    "level": "",        # str
    "seen": 0,          # int
    "duration": 0.0,    # float
    "service": "",      # str
    "tags": None,       # List[str] | None
}
```

Add a **MappingSpec** docstring at the top of the file so humans and tooling can verify the mapping:

```py
"""
MappingSpec:
source.name   -> service   (required)
msg           -> message   (optional; default "")
msg.level     -> level     (optional; default "")
seen          -> seen      (optional; default 0)
duration      -> duration  (optional; default 0.0)
tags[]        -> tags[]    (optional; may be null or omitted)

Drop conditions: if source.name != "myservice", record is not encoded (handled by probe).
"""
```

---

## Probe design (subscribe narrowly)

Use `mapper.Pred_Eq` with `log.Scalar_*` constructors to target only the logs you can handle. The other predicates: `Pred_Has(path)`, `Pred_Prefix((path, prefix))`, `Pred_In((path, [log.Scalar_*, ...]))`, `Pred_Gt((path, 1.5))` (a float; the field is a number above it) and `Pred_Regex((path, pattern))`. A selector matches when all of `all`, at least one of `any` (if not empty) and none of `none` hold; a record goes to the plugin when any of its selectors match.

**Pattern:**

```py
def probe(self) -> List[mapper.Selector]:
    return [
        mapper.Selector(
            any=[],
            all=[
                mapper.Pred_Eq(("source.name", log.Scalar_Str("myservice"))),
                # Add more predicates here if needed, e.g.:
                # mapper.Pred_Eq(("event.type", log.Scalar_Str("conn")))
            ],
            none=[],
        )
    ]
```

> **Do not** implement broad probes and then filter heavily in `process_logs`. Selection belongs in the probe.

---

## Processing pattern (batch transform)

* Build a **`bytearray()`** buffer for the whole batch.
* Iterate `logs`; for each `Logview` use `with lv:` to ensure safe access, then:

  * Read fields via `lv.get("path")` or `lv.get_list("path")`.
  * Populate the `out` dict.
  * Append `json.dumps(out).encode("utf-8") + b"\n"` to the buffer.
* Return `bytes(buf)` at the end.

**Pattern:**

```py
def process_logs(self, logs: List[log.Logview]) -> bytes:
    buf = bytearray()
    for lv in logs:
        with lv:
            out = {...}  # initialize with defaults
            # read fields; set keys
            buf.extend(json.dumps(out).encode("utf-8") + b"\n")
    return bytes(buf)
```

> **Do not** hold references to `lv` or `logs` after returning; treat them as borrowed views.

---

## Reading fields safely

Use the `Logview` helpers:

* `lv.has("path")` → `True` when the path is present.
* `lv.get("path")` → a scalar wrapper (`log.Scalar_Str`, `_Int`, `_Float`, `_Boolean`, `_Bytes`) or `None` (missing, or an object/array). Access the Python value via `.value`.
* `lv.len("path")` → the length of the list or string at the path, or `None`.
* `lv.get_list("path")` → `List[Scalar]` or `None`. Each item exposes `.value`; non-scalar items are skipped.
* `lv.get_map("path")` → `List[Tuple[str, Scalar]]` or `None`: the object's scalar members as `(key, scalar)` pairs.
* `lv.keys("path")` → the object's keys as `List[str]`; empty when the path is missing or not an object.
* `lv.log()` → the whole record as a JSON `str`.

**Patterns:**

```py
s = lv.get("msg.level")
if s is not None and hasattr(s, "value"):
    out["level"] = s.value

lst = lv.get_list("tags")
if lst is not None:
    out["tags"] = [item.value for item in lst]
```

> Avoid assuming types. Only read `.value` after you’ve confirmed the wrapper is present.

---

## Encoding & output

* Use the **stdlib `json`** module (`json.dumps`).
* Encode **exactly one JSON object per emitted record** and append `\n`.
* Concatenate into a single `bytearray` and return `bytes` at the end.

**Do NOT:**

* Pretty‑print (`indent=`): a record is one line, and a newline inside it splits it in two.
* Manually craft JSON strings.
* Return `str` (must be `bytes`).

---

## Error handling policy

**Default (strict):** If any unrecoverable transform/encode issue occurs:

* `raise Err("why")`, with `from componentize_py_types import Err`. The runtime logs `guest error ... error="why"` and drops the batch (`drop_on_error`, on by default).
* Any other exception (a `RuntimeError`, a `KeyError` from a bug) **traps** the plugin: the runtime logs the Python traceback and `plugin trapped; reinstantiating it`, drops the batch and starts a fresh instance, which is slow. Catch what you expect and turn it into `Err`.
* Do **not** emit partial output for that batch.

**Dropping records:** If a record is not applicable (should not be emitted), simply **do nothing** for that record (no line added). This should be rare if your probe is narrow.

---

## Tests & fixtures

Each mapper directory **must** include:

```
tests/input.json     # JSON array of input records
tests/expected.json  # JSON of the output: an array of the emitted records (an object if just one)
```

**Rules:**

* `input.json` is a **JSON array** of input records.
* `expected.json` is the plugin's output as JSON: an **array** of the emitted records in emission order, or a single **object** when the plugin emits one record.
* They are compared as parsed JSON: key order and whitespace do not matter, types do (`0` is not `0.0`). What the plugin produced is left in `test_out.ndjson`.
* Include at least:

  * One fully‑populated record.
  * One record with missing optional fields (assert defaults/omissions).
  * One record filtered out by the probe (absent in expected).
* Keep them small (3–8 records).

**Make targets:**

* `make build` — compiles `plugins/<name>.cwasm`.
* `make test` — builds, then runs the fixture comparison.
* `make run` — builds, then runs the pipeline in `tangent.yaml`.

> Tests must pass without external resources or environment tweaks.

---

## Performance requirements

* Build a single **`bytearray`** per batch; avoid creating many small `bytes` objects.
* Avoid per‑field allocations where possible; reuse local variables in the loop.
* Keep per‑record work proportional to fields accessed.
* Do not store large intermediates or caches; treat the mapper as stateless.
* No threads, no async, no I/O—pure CPU work in the hot path.

---

## Style & structure

* Type annotate function signatures (`List[log.Logview]`, `-> bytes`) and locals where it improves clarity.
* Put a **MappingSpec** docstring at the top of the module (see above).
* Keep functions focused; the main loop lives in `process_logs`.
* Comment **why** for non‑obvious implementation choices (e.g., newline framing, defaults).

---

## PR checklist (Definition of Done)

**The PR is ready when all items are true:**

* [ ] `metadata()` returns a unique `name` and SemVer `version`.
* [ ] `probe()` is as **narrow** as possible and correctly targets intended logs.
* [ ] `process_logs()`:

  * [ ] Uses `with lv:` and does not retain `lv` after the block.
  * [ ] Builds a single `bytearray` and returns `bytes`.
  * [ ] Emits exactly one line per output record and newline‑terminates each line.
* [ ] Output dict shape is stable; keys/types match tests.
* [ ] Reads fields via `lv.get` / `lv.get_list` and accesses `.value` safely.
* [ ] No I/O, no network, no randomness, no threads/async.
* [ ] `tests/input.json` and `tests/expected.json` present, small, and pass.
* [ ] `make build`, `make test`, and (optionally) `make run` succeed locally.
* [ ] No prints/logging from the mapper; exceptions only for batch‑fatal errors.

---

## Anti‑patterns

* ❌ Retaining `logs`/`lv` outside `process_logs` or outside the `with lv:` block.
* ❌ Printing/logging, file or network I/O from inside the mapper.
* ❌ Building JSON by hand or emitting partial lines.
* ❌ Huge dynamic dicts or reflection‑like patterns (`__dict__` walks) in the hot path.
* ❌ Broad probes with heavy in‑function filtering.
* ❌ Returning `str` instead of `bytes`.

