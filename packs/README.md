# packs

One directory per provider, each holding one plugin that turns that provider's
raw records into finished ones: normalized into `_norm`, stripped of what never
decides a verdict, its address enriched, and given what your company profile
says about it in `_facts`. How to use them is in the main
[README](/README.md#packs).

| directory | what it is |
|---|---|
| [enrich-ip](enrich-ip) | not a provider: the enrichment tables keyed by address, `geo`, `asn` and `networks`, that every pack's plugin reads. |
| [behavior-baseline](behavior-baseline) | not a provider: `baseline_builder`, which learns each actor's history into the state store, and `baseline_renderer`, which stamps it on the actor's records as `_baseline`, and the state store they keep it in. |
| [judge](judge) | not a provider: the judge, with a row for each pack's sources, and the renderer's records grouped an actor at a time for it. It includes enrich-ip and behavior-baseline, so a pipeline with the model includes judge and its packs, routes each pack's plugin to `baseline_builder`, `baseline_renderer` and its own sink, and needs nothing else; one without the model includes enrich-ip, behavior-baseline, or both. |
| [cloudtrail](cloudtrail) | `cloudtrail` |
| [github](github) | `github_audit`, `gha_ci`, `runner_egress` |
| [okta](okta) | `okta` |
| [google_workspace](google_workspace) | `google_workspace` |
| [macos](macos) | planned, not built |
| [shared](shared) | not a pack: `normkit`, `enrichkit`, `identitykit` and `factskit`, the Rust libraries every pack's plugin links |

A `source` value belongs to one pack: two packs taking it would store each
record twice.

## The record

What a pack's plugin writes, the same for every provider (`factskit::Finished`):

| field | always there | what it is |
|---|---|---|
| `ts` | yes, text | the record's own `ts`, or the time its pack read it when it had none |
| `source` | yes, text | the shape it came in as: `cloudtrail`, `okta`, `gha_ci`, ... |
| `_norm` | yes, an object | who did what, from where, to what (below); each field may be missing |
| `_facts` | yes, an object | what the profile and [enrich-ip](enrich-ip)'s tables say (below) |
| the rest | no | the provider's fields the pack kept, as the pack trimmed them |

A sink can rely on `ts`, `source` and `_norm` and nothing else: the
examples' lake keeps `ts` and `_norm` as columns, and the judge groups by
`_norm`'s org and actor. A raw record's own
`_norm`, `_facts`, `_ip`, `_context` and `_baseline` (and a key naming a field
under one, such as `_facts.notes`) are dropped first, so a log line cannot
speak for the pipeline.

`_norm`, who did what, from where, to what:

| field | what it is |
|---|---|
| `actor` | who acted: a user, role or key; for CI, the repository |
| `person` | the person the actor is, when the profile's `people` names them |
| `org` | whose it is: your org for your own sources (the profile's `org` and `own_sources`), else the record's own tenant |
| `kind` | what happened: the event's name |
| `ip` | the address it came from; this one is enriched |
| `country` | the country the provider says it came from |
| `dest` | the host it went to |
| `target` | what it touched: a bucket, role, repository, application |

`actor` and `org` are always kept, since they route the record, and so is
`person`; the others only when the record does not already hold the value.

`_facts`, what the profile and the tables say:

| key | when |
|---|---|
| `principal` | `member`, `service`, `key` or `unknown` from the profile; `aws` when AWS acted on its own; none for a repository |
| `about` | the profile's line on the actor |
| `watch` | the kind is one you watch |
| `ip` | the address: `country`, `asn`, `org`, `hosting`, and `tor`, `vpn`, `ours`, `private` when true |
| `ips` | other addresses a pack names, by field (no pack does yet) |
| `key_known_network` | a key called from the networks you expect, or not |
| `dest_known` | the destination is a host you talk to, or not |
| `sensitive_target` | the target is one you mark sensitive |
| `notes` | what a model reads all this by, a sentence each |

A pack adds what only its provider's log says: `cloudtrail` adds `root` and
`world_open`, each with its note after the watch note.

## The profile

Every fact about your company that no log carries, as the `profile` table: a
YAML file your config names, a `document` table every pack's plugin reads
beside [enrich-ip](enrich-ip)'s. Who is who is resolved as records come in,
and reaches the judge on the records (`_norm.person`) and in the state store.

```yaml
enrichment_tables:
  profile: { type: document, path: profile/profile.yaml, required: true }
```

| key | what it is | what the record gets |
|---|---|---|
| `org`, `own_sources` | your name; the `source` values that are your own infrastructure | their records are all one org, yours; list your packs' sources here |
| `countries` | where your people are | a note when a person acts from elsewhere |
| `cloud` | where your services run | a note when a service acts from any other hosting |
| `networks` | your own addresses: office, VPN, CI egress | `ip.ours`; never a person's odd location |
| `members` | people: IdP emails, GitHub handles | `principal: member`; anyone else is `unknown` |
| `people` | each person's other names, a list or one: IAM users, GitHub handles (`dave@example.com: [dave]`) | `_norm.person` when the actor is the person or one of their names; `principal` still comes from `members`, so list the names there too; in `_baseline`, their history, sign-ins and networks as one person's |
| `services` | IAM users and roles, bots | `principal: service`, held to `cloud` |
| `keys` | API keys and the networks each calls from | `principal: key`, `key_known_network` |
| `about` | who an actor is, when its name does not say | `about`, in place of the "nobody it knows" note |
| `hosts` | hosts your systems talk to (`*.suffix` allowed) | `dest_known` |
| `sensitive` | buckets, repos, tables | `sensitive_target` |
| `watch` | kinds a person wants seen every time | `watch` |

`required: true` makes a mistyped path an error that stops `tangent run`;
without it the table would answer nothing, with one warning, and every record
would go on as nobody the org knows, in the provider's org rather than yours.

## Writing a pack's plugin

A pack's plugin links [packs/shared](/packs/shared) by path:

- `normkit::Norm` is `_norm`, filled from the raw record. It also carries what
  the pack read off the log for `_facts`: `principal` (`"aws"` or `"none"`;
  unset, the profile decides), `fact(key, note)` for what only that log says,
  and `address(field, text)` for another address to enrich.
- `normkit::record`, `scrubbed` and the trim helpers give the record without
  the reserved keys, its per-event ids or its URL signatures.
- `normkit::Stamp::of(&read)` takes `ts` and `source` from the record as
  read, before the trim, so a trim need not keep them.
- `factskit::kit(lookup)` reads the profile and the tables once per instance,
  and `kit.write(out, stamp, record, norm)` folds the org, names the person,
  enriches the addresses, adds the facts and writes the record as one line.

The pack's `ingest.yaml` defines the plugin and its fixtures, and no edges:
the config that includes the pack routes it to its sink, `baseline_builder`
and `baseline_renderer`. behavior-baseline reads a source it knows from its
own table ([kit/src/kinds](behavior-baseline/kit/src/kinds)); a new pack's
records pass through it unlearned, with no `_baseline`, until the table knows
their `source`.

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
holds them ([the record](#the-record)), which is nearly always, so a
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
