# packs

One directory per provider, each holding one plugin that turns that provider's
raw records into finished ones: normalized into `_norm`, stripped of what never
decides a verdict, its address enriched, and given what your company profile
says about it in `_facts`. How to use them is in the main
[README](/README.md#packs).

| directory | what it is |
|---|---|
| [enrich](enrich) | not a provider: the enrichment tables every pack's plugin reads. Its README has the record contract and the profile's keys. |
| [behavior-baseline](behavior-baseline) | not a provider: `baseline_builder`, which learns each actor's history into the state store, and `baseline_renderer`, which stamps it on the actor's records as `_baseline`, and the state store they keep it in. |
| [judge](judge) | not a provider: the judge, with a row for each pack's sources, and the renderer's records grouped an actor at a time for it. It includes enrich and behavior-baseline, so a pipeline with the model includes judge and its packs, routes each pack's plugin to `baseline_builder`, `baseline_renderer` and its own sink, and needs nothing else; one without the model includes enrich, behavior-baseline, or both. |
| [aws](aws) | `cloudtrail` |
| [github](github) | `github_audit`, `gha_ci`, `runner_egress` |
| [okta](okta) | `okta` |
| [google_workspace](google_workspace) | `google_workspace` |
| [app](app) | your product's NDJSON: `app`, `agent`, and names you list |
| [macos](macos) | planned, not built |
| [shared](shared) | not a pack: `normkit`, `enrichkit`, `identitykit` and `factskit`, the Rust libraries every pack's plugin links |

A `source` value belongs to one pack: two packs taking it would store each
record twice.

## The lake

No pack defines a sink: where a pipeline keeps its records is its config's
to say. The examples keep every record in `lake`, and route each pack's
plugin to it:

```yaml
sinks:
  lake:                          # every finished record, kept: Parquet files, a directory an hour; delete old hours to trim it
    type: rolling_file
    path: lake/logs/%Y/%m/%d/%H
    wal_path: lake/wal
    max_file_age_seconds: 3600
    compression: { type: zstd, level: 3 }
    encoding:
      type: parquet
      record_column: record      # each record whole, for the fields the schema leaves out
      schema:
        ts: timestamp
        source: string
        _norm: { org: string, actor: string, kind: string, ip: string }
```

Every record, kept, as Parquet files in
`lake/logs/<year>/<month>/<day>/<hour>/`, the hour of the record's own `ts`
in UTC, whenever it arrives, a replay's too, each record whole in `record` and
`ts`, `source` and `_norm`'s org, actor, kind and ip as columns, for
DataFusion or DuckDB to read. Kind and ip are empty when the log already
holds them ([the record](enrich#the-record)), which is nearly always, so a
query reads them from `record` (CloudTrail's `eventName`, Okta's
`client.ipAddress`). A file is sealed an hour after it opened, and
only sealed files are there; a record that arrives after its hour's file
was sealed adds another file to that hour. Delete old hours to trim it.

## Tests

Each pack's `tests/` holds its fixtures and the tables they were written
against: `profile.yaml`, the company; `addresses.yaml`, GeoLite2's answers for
their addresses; `networks.csv`, those addresses' hosting, Tor and VPN labels.
Its `ingest.yaml` names them for the test, so the fixtures mean the same in
any config and on any day, with no data fetched.

```bash
make -C packs test                # each pack alone: compile, then its fixtures; behavior-baseline's story and tests; and the libraries' tests
make -C packs fixtures-okta       # okta/tests/expected.json from the plugin's output; read the diff
                                  # (a test that fails for any reason but its output leaves it as it was)
make -C packs fixtures-baseline   # behavior-baseline/tests/expected.json, the same way
make -C packs dist                # every plugin as a component in target/packs, what a release ships (dist.sh)
```

A release carries each plugin compiled, with its build's attestation, and
`tangent plugin fetch` takes them from there: a user needs no toolchain
([Prebuilt plugins](/README.md#prebuilt-plugins)).
