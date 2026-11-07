<p align="center">
  <img src="https://github.com/user-attachments/assets/7da02584-0a4e-4e41-af6d-61cb081029c4" alt="Tangent logo" height=170>
</p>
<h1 align="center">Tangent</h1>
<p align="center"><em>Stream processing without DSLs. Shareable, sandboxed, fast.</em></p>


<p align="center">
  <a href="https://github.com/EthanBlackburn/tangent/actions">
  <img height="20" src="https://img.shields.io/github/actions/workflow/status/EthanBlackburn/tangent/scaffold_test.yml?label=build&logo=github" alt="Build status"></a>
  <a href="https://twitter.com/telophasehq">
  <img src="https://img.shields.io/twitter/follow/telophasehq?style=social" alt="X">
  </a>

</p>

<p align="center">
  <a href="https://docs.telophasehq.com">Documentation</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://discord.gg/ZUHB3BRa8c">Discord</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/EthanBlackburn/tangent/issues/new">Issues</a>
</p>

## What is Tangent?
Tangent is a stream‑processing toolkit that treats **plugins** (user‑defined functions) as first‑class citizens – exceptionally easy to write and share. Perfect for vibe-coding those pesky log transformations.


Plugins run in a lightweight WASM sandbox with near-native speed and full language flexibility — no DSLs, no vendor-locked runtimes. Plugins are designed to be shareable, so common transformations (e.g. GuardDuty findings → OCSF) can be written once and shared with the [community](https://github.com/EthanBlackburn/tangent-plugins).

Tangent ships with everything you need to develop, test, and benchmark your own transforms:
* `tangent plugin scaffold` – generate plugin boilerplate (`--setup` also installs the toolchain)
* `tangent plugin compile` – compile plugins to WASM
* `tangent plugin fetch` – download plugins a release built, their build's attestation checked; no toolchain
* `tangent plugin test` – run plugin tests
* `tangent bench` – measure throughput and latency before deploying
* `tangent run` – start the Tangent runtime
* `tangent config` – print a config as the runtime reads it, its includes merged

## Why use Tangent?
1. **Use real languages, not DSLs** – Real code > DSL. Reviewable, testable, LLM‑friendly.

2. **Catch breakage before prod** — `tangent plugin test` for correctness; `tangent bench` for throughput/latency.

3. **Shareable and secure** – Data transformations are easy to write and _share_. Publish and discover open-source plugins in the [Tangent Plugins library](https://github.com/EthanBlackburn/tangent-plugins). Each plugin runs in its own lightweight sandbox.

## State
Plugins keep state through the `state` interface: `state.Get`, `state.Set`, `state.Incr`, `state.Del`, and `state.Scan` for a key range, each write with an optional TTL. That state lives in the runtime's **state store**, configured under `runtime.state`:

```yaml
runtime:
  state:
    backend: sqlite        # or lru (in-process, bounded); distributed backends slot in here
    path: state.sqlite
    default_ttl_ms: 600000 # 0 = never expire
    max_ttl_ms: 3600000    # cap on a plugin's TTL; 0 = no cap
    max_entries: 100000    # lru only: evict the least recently used past this
```

An entry past its TTL reads as missing at once, and the running pipeline deletes it when it starts and every five minutes after. SQLite reuses the space a deleted entry held, so `state.sqlite` grows only while the live entries do; it does not shrink.

Entries are keyed by namespace and key. Plugins share one namespace, `plugins`, so what one plugin writes the next can read; a read, and a write that depends on it, go in a transaction when several workers run the plugin (below). Give a plugin a `state_namespace` of its own when it must not share, such as two instances of one plugin with different configs:

```yaml
plugins:
  detect_login:  { module_type: rust, path: ./detect, state_namespace: login }
  detect_export: { module_type: rust, path: ./detect, state_namespace: export }
```

A plugin may read files too: `data: <dir>` mounts a directory, relative to the file that names it, read-only at `/data` in the guest, for reference data too big for `config`. [examples/enrich](/examples/enrich) reads MaxMind GeoLite2 and the cloud providers' ranges that way, and its `setup.sh` fetches them. Each plugin instance on each worker reads such files into its own memory; reference data that several plugins read, or that is large, belongs in an [enrichment table](#enrichment-tables), which the process loads once.

A plugin can take its records a group at a time:

```yaml
plugins:
  renderer:
    group_by: [_norm.org, _norm.actor]   # a group per value of these fields
    max_events: 40                       # closes at 40 records,
    expire_after_ms: 30000               # or once nothing has joined it for 30 s (the default),
    max_age_ms: 600000                   # or 10 minutes after it opened, however busy its key (unset by default)
    history: 39                          # the key's latest records, sent again ahead of its next group
```

Records reaching the plugin wait in their key's group, in memory, and are acked once there. A closed group reaches the plugin whole, in one call, on workers of the plugin's own, so a slow plugin, or a model after it, never holds up what feeds it, and one group per key is out at a time. At the end of input (`--once`, a shutdown) every open group closes; a crash loses the open ones. With `history`, a key's latest records are kept in the state store (namespace `groups`) and go again ahead of its next group, marked `"_context": true`, so the plugin sees what came before.

When a plugin's call fails (it traps or returns an error), the records it was given are logged and dropped. `drop_on_error: false` passes them on unchanged instead, as if the plugin were not there: right for a plugin that only adds to records, such as context for a model, wrong for a filter. `tangent plugin test` always drops, so a failed call fails the test.

Each state call stands alone: two workers running a plugin can interleave one's read with the other's write. A plugin that reads state and writes what depends on it makes them one transaction: `state.begin()`, its reads and writes, then `state.commit()`, or `state.rollback()`. They see each other's writes and land together, or not at all, however many workers run the plugin. A plugin that keeps state per tenant puts the tenant in its keys: [examples/counter](/examples/counter) counts events per org under `count:<org>`, and marks "first time this org did X" in a transaction. No other state call runs while one is open, so begin late, after the parsing, and commit early. A second `begin`, or a remote call, inside one fails, and a call that returns with one open has it rolled back, with a warning: only what a plugin commits is kept.

The store is readable while the pipeline runs:

```bash
tangent state --config tangent.yaml ls                          # keys per namespace
tangent state --config tangent.yaml get count:acme              # one entry (--ns defaults to the only namespace)
tangent state --config tangent.yaml scan --prefix user: --limit 50
tangent state --config tangent.yaml sql "select ns, count(*) from state group by 1"   # sqlite backend only
```

Another pipeline can read it too: `read_only: true` under `runtime.state` opens the same SQLite file without the lock and creates nothing, so one pipeline learns while another reads what it learned. The file must exist and hold the writer's table, so the writer starts first. Its writes fail, and an expired entry reads as missing; a pipeline whose runtime writes state (a source that polls an API) is refused.

## Enrichment tables
Reference data every plugin can read, loaded once for the process: an IP database, a list of networks, a company profile. Each table has a name, a `type` and a `path`, relative to the file that names it:

```yaml
enrichment_tables:
  geo:      { type: mmdb, path: data/GeoLite2-Country.mmdb }
  asn:      { type: mmdb, path: data/GeoLite2-ASN.mmdb }
  networks: { type: cidr, path: data/networks.csv }
  profile:  { type: document, path: profile/profile.yaml, required: true }
```

A plugin looks a key up in a table by name through the `enrichment` interface ([assets/wit/processor.wit](/assets/wit/processor.wit)) and gets JSON back, or nothing:

```rust
use tangent::logs::enrichment::lookup;

let geo = lookup("geo", ip);              // Some(r#"{"country":{"iso_code":"FI",...},...}"#)
let nets = lookup("networks", ip);        // Some(r#"["aws","datacenter"]"#), None when in none
let org = lookup("profile", "org");       // Some(r#""acme""#)
```

Every address in a record is a call: a plugin that knows three fields holding addresses looks up all three.

| `type` | file | key | answer |
|---|---|---|---|
| `mmdb` | a MaxMind DB | an address | its record, as JSON |
| `cidr` | `prefix,label` lines, IPv4 and IPv6; `#` comments; a bare address is its own /32 or /128; an IPv6 prefix holds no IPv4 address, however wide (`::/0`), and `::ffff:192.0.2.0/120` is the IPv4 prefix it maps | an address | the label of every prefix that holds it, as a list: each label once, in the order the labels first appear in the file, so a file lists first the labels that should win |
| `document` | YAML, or JSON when the name ends `.json` | a dotted path, `[n]` or `.n` for an item of a list, `""` for all of it | the value there; a key may hold dots |

An IPv4-mapped IPv6 address (`::ffff:203.0.113.7`) is looked up as the IPv4 address. A key that is not an address, a path that is not there, and a null all answer nothing.

`tangent run` reads every table when it builds the DAG, before the first record, into one copy every worker and every plugin shares: two plugins, or two packs, that read GeoLite2 hold it in memory once, and a lookup is a search in that memory, with no file read and no lock: tens of nanoseconds for a `cidr` table, about a microsecond for a GeoLite2 Country record. A file named by two tables, as the same type, is read once. A file that is missing is a warning, once, and its table answers nothing, as a missing file did for a plugin that read its own; a file that is there but cannot be read as its type stops `tangent run`, naming the table and the file. A table with `required: true` must have its file: without it `tangent run`, `tangent plugin test` and `tangent config` stop, naming both. Mark a table so when nothing is right without it, as for a company profile. A table no config defines answers nothing too, with a warning the first time a plugin asks for it. A refreshed file is picked up at the next start. `tangent plugin test` loads the tables its config declares, so a fixture runs against the same data. A test may name tables of its own (`tests: [{input, expected, enrichment_tables: {...}}]`), which take the place of the config's of the same name for that test: the data its expected output was written against, so a pack's fixtures pass in any config that includes the pack.

The runtime knows these three formats and nothing about what is in them. A provider's own format is converted beforehand: [examples/enrich/setup.sh](/examples/enrich/setup.sh) fetches GeoLite2, the AWS, GCP and Cloudflare ranges, X4BNet's datacenter and VPN lists and the Tor exit list, and writes the ranges into `data/networks.csv` labelled `aws`, `gcp`, `cloudflare`, `datacenter`, `vpn` and `tor`, in that order (`./setup.sh networks` writes it again from the files already there).

A plugin built before the interface existed does not import it and runs as before. To call it from an existing plugin, compile the plugin again: `tangent plugin compile` writes this tangent's WIT ([assets/wit](/assets/wit)) into each plugin's `.tangent/wit/` before building it, so a plugin builds against the interface of the tangent that compiles it, and keeps no copy of its own (`--wit <dir>` builds against another). A plugin that imports it needs a runtime that provides it.

## Keeping records and reading them back
A `sqlite` sink keeps every record once, in its `table`, with a column per partition field holding the field's value as text, NULL without it. A partition is an index on its fields and the record's time, `<table>_<partition>`; `pending` and `at` are taken by the table's own indexes. A `sqlite` source, in the same process or another, reads one partition back in batches, each of one partition value; it takes the partition's fields from that index:

```yaml
sinks:
  logs:
    type: sqlite
    path: logs.sqlite
    table: logs
    timestamp_key: ts      # the record's own time (RFC 3339 or epoch); arrival time without it
    partitions:
      org: [_norm.org]
      actor: [_norm.org, _norm.actor]

sources:
  actors:
    type: sqlite
    path: logs.sqlite
    table: logs
    partition: actor
    batch_size: 40         # every batch is a partition value's latest 40 records
    timeout_secs: 3600     # send by then even if fewer than 40 are new
```

A partition value's records not yet sent go oldest first, `batch_size` to a batch, one batch of a value at a time. Fewer wait until the oldest is `timeout_secs` older than the newest record stored, so a replay of old logs goes as live ones do, or until nothing new has been stored for `timeout_secs`; then they go topped up to `batch_size` with the value's latest records already sent, which carry `"_context": true`. A batch's new records get the time it is through the DAG in `emitted`, NULL until then, and the value's next batch goes then; a restart sends again what was not. The table is a queue: once a batch is through the DAG, the same transaction deletes the value's sent records past its latest `batch_size` - 1, the most a top-up uses, so it holds what is not sent yet and what tops up each value's next batch. Keep the full history elsewhere, in a `rolling_file` sink. A record that arrives after its value's later ones were sent is topped up with what is left.

A `file` sink appends every write to one file at `path`, which only NDJSON survives, so it takes no other encoding. A `rolling_file` sink keeps records as whole files for a query engine (DataFusion, DuckDB, Spark) to read, starting a new file each time the WAL an `s3` sink uses seals one: a write is acked once it is in the WAL under `wal_path`, a WAL file is sealed at `object_max_bytes` of records or `max_file_age_seconds` after it was opened, and each sealed file becomes one file in the directory, in the sink's `encoding`, synced and renamed into place (copied under a hidden temporary name first if the WAL is on another filesystem), so a reader listing the directory sees whole files only. `path` may hold strftime specifiers, rendered in UTC for each record's own time, the RFC 3339 string in the field `timestamp_field` names (`ts` unless set; found as `group_by` finds a field, so `a.b` is a key holding all of it or else field `b` of the object at `a`, and `a[0].b` is in the first item of the list at `a`), so `lake/logs/%Y/%m/%d/%H` puts each hour's records in a directory of their own, whenever they arrive, a replay's too, and a query engine can skip an hour by its path. A record whose field is missing or not RFC 3339 (an epoch number, which a `sqlite` sink's `timestamp_key` takes, is not) goes under the time it reaches the sink, and `tangent_sink_records_untimed_total{sink}` counts it; a `path` with no specifier reads no record's time. A record arriving after its hour's file was sealed adds another file there. The WAL keeps a file open for at most 32 directories at once, sealing the one written longest ago, so records far out of time order make small files. A file is named as an `s3` object is, `<ULID>.parquet` (`.ndjson.zst` for zstd NDJSON), and the ULIDs sort by time. Parquet has the columns its `schema` names, in order, each a name and a type: `string`, `int` (64 bits), `float` (a double), `bool`, `timestamp` (milliseconds in UTC, read from RFC 3339 text in any zone), or a map of columns, a struct, for a nested object; every column may be null. `record_column` keeps each record beside the columns, its line without the line break, for the fields the schema leaves out. A record the schema cannot read (a type it does not give the field, a time arrow cannot parse) does not fail its file: it is kept in `record_column` with its typed columns null, or, without a record column or if it is not JSON, dropped, as JSON drops a line that is not JSON and Avro one its schema does not take; `tangent_sink_records_unfit_total{fate="kept"|"dropped"}` counts them once their file is out, not at each try:

```yaml
sinks:
  lake:
    type: rolling_file
    path: lake/logs/%Y/%m/%d/%H      # lake/logs/2026/11/06/13/<ULID>.parquet for a ts in that hour
    timestamp_field: ts              # the default
    wal_path: lake/wal               # apart from path
    max_file_age_seconds: 3600
    object_max_bytes: 67108864       # 64 MiB of records a file
    compression: { type: zstd, level: 3 }
    encoding:
      type: parquet
      record_column: record
      schema:
        ts: timestamp
        source: string
        _norm: { org: string, actor: string }
```

An `s3` sink goes through the same WAL and puts each sealed file in `bucket_name` as one object, named as a `rolling_file` sink's files are. Its `key_prefix` is what each object's key starts with, before a `/` and the name: a strftime template rendered in UTC for each record's own time, read from `timestamp_field` as a `rolling_file` sink's `path` is, so `logs/cloudtrail/%Y/%m/%d` puts each day's records under a prefix of their own, whenever they arrive, a replay's too (`%%` is a `%`). It is a path in the bucket, and strftime is all that is rendered: `tangent run` and `tangent config` refuse one that is not a strftime template, holds a `{{ field }}`, starts with `/`, or has an empty (`//`), `.` or `..` segment. Without it, objects go to the bucket's root. As in a `rolling_file` sink, a record without a time goes under the time it reaches the sink and is counted, a record arriving after its prefix's objects went out adds another object under it, and records far out of time order make small objects:

```yaml
sinks:
  archive:
    type: s3
    bucket_name: acme-logs
    key_prefix: logs/cloudtrail/%Y/%m/%d   # logs/cloudtrail/2026/11/06/<ULID>.ndjson.zst
    timestamp_field: ts                    # the default
    wal_path: /var/lib/tangent/wal
```

A write is acked once it is in the open WAL file; that file is synced only when it is sealed. What was acked survives tangent stopping without its flush (killed, crashed, out of time at shutdown): the next start seals and sends the files left open, cut back to their last whole line. It does not survive the machine going down before the file is sealed.

`tangent_sink_bytes_total` counts a file's bytes when it is in place, and `tangent_sink_bytes_uncompressed_total` the records' NDJSON bytes it holds; a write acked into the WAL counts only once its file is sealed and written.

## Inference
An `inference` node asks a model about each record. Nodes are configured under `inference:`, each with its own model and prompt, and sit in the DAG by name, as every node does:

```yaml
inference:
  judge:
    url: http://localhost:8080/v1          # any OpenAI-compatible chat endpoint
    model: mlx-community/GLM-4.7-Flash-4bit
    prompt_file: judge.md                  # the system message
    extra: { chat_template_kwargs: { enable_thinking: false } }
```

The model is reached through [rig](https://github.com/0xPlaygrounds/rig): `backend: openai` (the default) for any OpenAI-compatible chat endpoint such as OpenAI, mlx_lm.server, llama-server, vLLM or Bedrock's OpenAI endpoint, and `backend: bedrock` for Amazon Bedrock's Converse API with the AWS credential chain (`aws: { region, profile }`).

The records the node is handed are the user message, one JSON object per line (the record, or its `fields`) with its index `i`, or with `encoding: {type: logfmt}` a logfmt line each, and the reply is `{"answers": [...]}`: each answer, a JSON object, lands on its record under the node's name (`judge` above); a failure lands there as `{"error": ...}`. The node adds and forwards, never drops. What goes on from there is a plugin's decision, and a plugin's `probe()` is already that filter: [examples/inference-local/flagged](/examples/inference-local/flagged/src/lib.rs) keeps `judge.flag == true` (and anything the judge could not read) with a pass-through body.

The reply is read as JSON and nothing else, and the backend is asked to make it so: with `schema_file` (a JSON Schema next to the prompt) through `response_format` on an OpenAI-compatible server or structured output on Bedrock; without one, an `openai` server is put in JSON mode. A server that ignores both (mlx_lm.server does) is held to the prompt, and a reply that is not a JSON object is the error.

A request holds the records the node is handed, or with `group_by: [_norm.actor]` those of one value of these fields, and is split past `batch_bytes` (32 KiB of JSON) and, with logfmt, past `budget_tokens` (90k). A `sqlite` source hands over a partition value's records `batch_size` at a time, so with `group_by` on the partition's fields a request is one of its batches. A schema is wrapped around each answer, `max_tokens` is per record, and a reply that cannot be read is asked again in halves.

`encoding: {type: logfmt, keep: [...], sources: {<source>: {principal, time, keep, ids}}}` writes the records as logfmt lines in about half the tokens of JSON, and the text decodes back to the records: a principal's records are grouped, the fields they share written once above their rows, the `keep` fields on every row, a day's timestamps as times of day; `sidecar: true` swaps a source's `ids` for references the model never sees. The judge pack has a row for each pack's sources ([packs/judge/ingest.yaml](/packs/judge/ingest.yaml)).

Inference nodes run inside the workers, in DAG order with the plugins: a worker hands a model the records that reached the node, waits for the replies, and carries them on, so a batch's records stay in order and nothing else moves while the model thinks. `concurrency` bounds requests in flight to the node, every worker's together (1 for a laptop GPU, 8 or more for a hosted model); `timeout_ms` (30 s) frees a slot a hung request would otherwise hold; `retries` (2) tries again, backing off, when a request got no reply at all. `temperature` is sent only when set. A signal (SIGINT or SIGTERM) ends the calls under way rather than wait out `timeout_ms` and `retries`: records waiting on one go to no sink and are not acked, so a `sqlite` source sends them again at its next start. Another signal, a second or more after the first, exits at once. See [examples/inference-local](/examples/inference-local) and [examples/inference-bedrock](/examples/inference-bedrock).

## Including files
A config can span several files. `include:` lists files merged into the one that names it, each path relative to that file:

```yaml
include:
  - packs/judge/ingest.yaml
  - packs/aws/ingest.yaml
plugins:
  baseline_renderer: { expire_after_ms: 10000 }   # one setting of a plugin an included file defines
```

- Maps merge key by key at any depth, so a file can add a source, a plugin, or one setting of another file's plugin.
- A file wins over the files it includes, and its `key: null` (or `key:` alone, or an unset `${VAR}` alone) removes what they set there; the field's default applies. A null where no file set anything is no value either, so `model:` alone is a missing `model`, not an empty one. But in a plugin's `config` and an inference node's `extra`, which tangent hands on as written, a null is a value like any other: `extra: { response_format: ~ }` sends the null.
- Two files that don't include each other must agree on any value that is not a map, and on whether a key holds a map at all. When they don't, a file that includes both settles it; without one, loading fails naming the key and both files.
- A value reads as its file wrote it: `model: 1.10` is `"1.10"` to a string field, with `include:` or without, and two files agree only when they wrote the same.
- A plugin two files define the same way is one plugin, loaded once per worker however many files define it; an enrichment table two files define the same way is one table, loaded once for the process.
- Lists are values, except `dag`: every file's edges add up, and an edge given twice counts once.
- A plugin's `path`, `data` and `tests`, an inference node's `prompt_file` and `schema_file`, and an enrichment table's `path` are relative to the file that names them. `runtime` paths stay relative to the config you run, and source and sink paths to the working directory, as before.
- A file included twice (by two packs, say) is read once, and what each includer changes in it holds; a cycle is an error. `${VAR}` works in every file, in `include` paths too, and `${VAR:-default}` is `default` when the variable is unset or empty.
- `runtime` may be left out, for its defaults. An unknown key is an error: `includes:` at the top, `bind_adress` in a source. A top-level key that starts `x-` is the file's own, a place for YAML anchors (`x-prompt: &prompt judge.md`), and is neither read nor merged.

Unknown keys aside, a config without `include:` reads as it always has.

`tangent config --config FILE` prints the merged config, every default filled in, after the files it came from. Secret fields (tokens, `api_key`, `webhook_url`, passwords) are left out, so it is for reading, not for running.

An edge names its nodes and nothing else, `from: cloudtrail` and `to: [aws, lake]`: a name is one node, so no source, plugin, inference node and sink share one, and a config where two do fails to load, naming both. `tangent run` checks the DAG before it starts: an edge to a node no file defines is an error, and so is one out of a sink or into a source. It warns about a source with no edge out, and names once a source whose records none of its targets take, as when no included plugin's probe claims a record's `source`; `tangent_unrouted_records_total` counts every record dropped that way, per node.

## Packs
A pack is what tangent knows about one provider's logs, and it is one plugin, named after the provider. Tangent's own sources read the logs (`cloudtrail`, `github_audit`, `okta` and `google_workspace` read the providers' APIs; `tcp` takes anything else as NDJSON with a `source` field), and the pack's plugin turns each raw record into a finished one:

1. drops the keys only the pipeline writes (`_norm`, `_facts`, `_ip`, `_context`, `_baseline`), so a log line cannot speak for it;
2. normalizes it: `_norm` says who did what, from where, to what (`actor`, `person`, `org`, `kind`, `ip`, `country`, `dest`, `target`), the same way for every provider;
3. strips what is redundant or never decides a verdict: per-event ids, hashes, pagination, issued credentials, URL signatures;
4. enriches its address from the [enrichment tables](#enrichment-tables): country, network and owner, hosting provider, Tor exit, VPN, one of yours;
5. adds under `_facts` what your company profile says about it (a member, a service, a key or nobody known; a usual place, destination and target or not; a kind you watch) and what only that provider's log can say.

| pack | takes `source` | from | of its own |
|---|---|---|---|
| [aws](/packs/aws) | `cloudtrail` | the `cloudtrail` source: a trail's files, every account of an organization trail | the role or SSO user as actor, `aws:<service>` when AWS acts on its own; `_facts.root`, `_facts.world_open` |
| [github](/packs/github) | `github_audit`, `gha_ci`, `runner_egress` | the `github_audit` source; Actions job logs and runner egress as NDJSON | a CI run told per repository, which is not a principal |
| [okta](/packs/okta) | `okta` | the `okta` source: the System Log | Okta's own risk signals kept |
| [google_workspace](/packs/google_workspace) | `google_workspace` | the `google_workspace` source: login, token, drive and admin activity | each event's parameters by name |
| [macos](/packs/macos) | | planned: Endpoint Security events from laptops | |

A pack is a directory: the plugin (`Cargo.toml`, `src/`, Rust compiled to WASM), its fixtures (`tests/`), `ingest.yaml` (the plugin and its fixtures; your config says where its records go), and a README saying what it reads, writes, drops and enriches, with the lines to paste.

Three packs are not providers: [packs/enrich](/packs/enrich), the enrichment tables every pack's plugin reads; [packs/behavior-baseline](/packs/behavior-baseline), `baseline_builder` and `baseline_renderer`, each actor's history in the state store; and [packs/judge](/packs/judge), which groups each actor's records for the judge, with a row for each pack's sources, and includes the other two. No pack defines a sink: your config says where records are kept ([the lake](/packs/README.md#the-lake) in the examples). The packs' shared code is in [packs/shared](/packs/shared): `normkit` (`_norm` and the scrubs), `enrichkit` (an address from the tables), `identitykit` (which names are one person) and `factskit` (the profile's facts), Rust libraries each plugin links.

No pack loads IP data itself. enrich's `ingest.yaml` names it as enrichment tables (GeoLite2 Country and ASN, and `networks.csv`, the cloud, datacenter, VPN and Tor ranges in one file), the runtime loads each once when the pipeline starts, and every pack's plugin, on every worker, reads that one copy: four packs load GeoLite2 once. [examples/enrich/setup.sh](/examples/enrich/setup.sh) fetches the files. Your profile is a table too, `profile`, which your config names.

The judge also sees each actor's history, which one window of its records cannot show. behavior-baseline folds every finished record into the state store (`baseline_builder`), and before the judge puts the same `_baseline` on each of an actor's records (`baseline_renderer`): who it is and, for a role, who assumed it; how long it has been seen; what it does for the first time (an API, a resource, a client, a network, an OAuth app); its volume and hours against its own 28 days; its person's sign-ins and credential changes in every source over three days (the profile's `people` says which names are one person); and addresses it shares with other actors. Only what happened before the actor's first record counts, and it is facts, not verdicts ([packs/behavior-baseline](/packs/behavior-baseline)). The judge writes records as logfmt, `_baseline` once per request, about 5% more tokens than without it; JSON lines (`encoding: {type: ndjson}`) repeat it on every record, at three times logfmt's tokens ([packs/judge](/packs/judge#the-model) has the numbers). Baseline's folds are one state transaction per batch, so ingest runs as many workers as it likes.

### Using packs
Packs need a tangent newer than 0.1.9, the latest release as this is written: `include:` and `enrichment_tables` are newer, and that release stops at ``missing field `runtime` ``. Until the next release, build one from the checkout (`cargo install --path ../tangent/crates/cli --locked`) and `tangent plugin compile` the packs where the lines below fetch them: their prebuilt plugins are released with tangent, from the next release on.

A pipeline is one config: judge's `ingest.yaml`, which includes enrich's and behavior-baseline's, and the `ingest.yaml` of each pack you want. It names your sources and the sink that keeps their records, sends each source to its pack and each pack's records on to the lake, `baseline_builder` and the judge (`baseline_renderer`), and points the `profile` table at your company. Without the model, include enrich's `ingest.yaml` in place of judge's, and behavior-baseline's too for each actor's `_baseline`, and draw only the edges to what you included: finished records in the lake, and no model. Each actor's records wait in a group for the judge (`baseline_renderer`'s `group_by`), and the judge runs on workers of its own, so ingest never waits on the model. To watch AWS and Okta from a directory beside your tangent checkout:

```yaml
# tangent.yaml
include:
  - ../tangent/packs/judge/ingest.yaml
  - ../tangent/packs/aws/ingest.yaml
  - ../tangent/packs/okta/ingest.yaml

enrichment_tables:
  profile: { type: document, path: profile/profile.yaml, required: true }   # your company

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

sources:
  cloudtrail: { type: cloudtrail, region: "${AWS_REGION}", queue_url: "${CLOUDTRAIL_QUEUE_URL}" }
  okta_audit: { type: okta, org_url: "${OKTA_ORG_URL}", token: "${OKTA_API_TOKEN}" }

dag:
  - from: cloudtrail
    to: [aws]
  - from: okta_audit
    to: [okta]
  - from: aws     # kept, learned, judged
    to: [lake, baseline_builder, baseline_renderer]
  - from: okta
    to: [lake, baseline_builder, baseline_renderer]
```

```yaml
# profile/profile.yaml: what no log says about you; packs/enrich/README.md lists every key
org: example
own_sources: [cloudtrail, okta]      # their records are all this org's
countries: [US]
members: [alice@example.com, bob@example.com]
people: { alice@example.com: [alice] }   # one person under each name: the IAM user alice is alice@example.com
networks: [203.0.113.0/24]           # your office and VPN egress
watch: [StopLogging, user.mfa.factor.deactivate]
```

```bash
brew install gh                                   # once: it checks each prebuilt plugin's attestation; no login
../tangent/examples/enrich/setup.sh data          # once: GeoLite2, cloud ranges, Tor exits
tangent plugin fetch --config tangent.yaml        # the packs' plugins, prebuilt and attested
tangent plugin test --config tangent.yaml         # the aws and okta fixtures
tangent run --config tangent.yaml
```

`tangent plugin compile` in place of `fetch` builds the packs here, from source, for a pack you are changing: it needs the Rust WASM toolchain, which `../tangent/examples/enrich/setup.sh` (without `data`) installs.

The judge is any OpenAI-compatible model: `JUDGE_URL` (a local model on `:8080` by default), `JUDGE_MODEL` (`glm-4.7-flash`), `JUDGE_API_KEY`. Each pack's README has its source's block and credentials, [packs/enrich](/packs/enrich#the-profile) the profile's keys, and [packs/judge](/packs/judge#the-model) the judge's settings.

A `tcp` source can feed several packs: route it to each, and each takes only its own `source` values. A record whose `source` no included pack takes goes no further, and `tangent run` says so. Your product's own logs have no pack: no two products' are alike, so they want a plugin of your own that fills `_norm` ([Writing a pack](#writing-a-pack) starts from packs/okta; `tangent plugin scaffold` from nothing). The judge's logfmt wants a row for each of its sources in your config, under `inference.judge.encoding.sources`, as [judge's ingest.yaml](/packs/judge/ingest.yaml) has for each pack's, for their times and the fields on every line; behavior-baseline passes their records through unlearned, with no `_baseline`.

[examples/cloud-infra](/examples/cloud-infra) is every pack for a fictional company, whose cases in [tangent-eval's cases](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases) it is measured against.

### Prebuilt plugins
A plugin is a WASM component, compiled once and run on every machine, so only a pack's author needs the toolchain. Each release of tangent carries every pack's plugin as a release asset, `<plugin>.component.wasm`, built by [packs/dist.sh](/packs/dist.sh) in the release workflow, and a pack's `ingest.yaml` says so:

```yaml
plugins:
  aws:
    module_type: rust
    path: .                                          # the source, for `tangent plugin compile`
    prebuilt: { repo: EthanBlackburn/tangent, workflow: .github/workflows/attest-packs.yml }
```

`tangent plugin fetch --config tangent.yaml` downloads each `prebuilt` plugin from `repo`'s release of this tangent's version (`release:` names another; `asset:` another file name; `host:` a GitHub Enterprise host) and precompiles it into `runtime.plugins_path`, where `tangent plugin compile` puts it and `tangent run` reads it. A plugin with `prebuilt` and no `path` is fetched only.

A prebuilt plugin is code off the network that reads every record, so before one is kept its build is checked: the release workflow attests each component, on its own, with [GitHub artifact attestations](https://docs.github.com/en/actions/concepts/security/artifact-attestations) ([.github/workflows/attest-packs.yml](/.github/workflows/attest-packs.yml)), a Sigstore signature naming the component's file name and digest, the repository, the workflow and the commit that built it, with the attestation bundle uploaded beside the component as `<asset>.sigstore.json`. `fetch` has `gh attestation verify`, GitHub's own verifier, check it: signed for `repo` from the release's tag (`--source-ref refs/tags/<release>`), by `workflow` when named; and then that the statement names the file as the asset asked for, not only by its digest, so one attested component of a release cannot be served under another's name. With the bundle on the release nothing asks GitHub's API, so `gh` need not be logged in; a release without the bundle falls back to the API, which needs `gh auth login`. A plugin that does not verify is not kept, and there is no flag to skip the check. `sha256:` pins the component's digest as well, for a lockfile's certainty. The command `fetch` ran is printed, to run again by hand:

```bash
gh attestation verify plugins/aws.component.wasm --repo EthanBlackburn/tangent --source-ref refs/tags/v0.1.10 \
  --bundle plugins/aws.component.wasm.sigstore.json --signer-workflow EthanBlackburn/tangent/.github/workflows/attest-packs.yml
```

A pack of your own is published the same way: a workflow that builds the component with `cargo component build --release --target wasm32-wasip2`, attests it with `actions/attest-build-provenance` under the file name it will have on the release (the step's `bundle-path` output is the bundle), and uploads both to a release; its users write `prebuilt: { repo: you/packs, release: <tag>, workflow: .github/workflows/release.yml }`.

### Volume and cost
What a pack costs to run is what its judge's model charges: tangent moves hundreds of MB a second ([Benchmarks](#benchmarks)), and a model is paid by the token. With Claude Haiku 5.5 as the judge ($0.10 a million tokens in, $0.50 out), a day of logs costs about

```
$15 for each million logs  +  $12 for each GB of them
```

| logs a day | 250 B each | 500 B | 1 KB | 2 KB |
|---|---|---|---|---|
| 100k | $1.80 | $2.10 | $2.70 | $3.90 |
| 1M | $18 | $21 | $27 | $39 |
| 10M | $180 | $210 | $270 | $390 |

A month is thirty times as much. The count matters more than the size. A log reaches the model as a trimmed logfmt line, about a token for every 9 bytes it had, but each one also carries its `_norm` and `_facts`, a share of the prompt and of `_baseline`, and gets an answer (about 11 tokens to pass, 150 to flag): below 1 KB, that is most of what a log costs. The rule fits real CloudTrail, GitHub and Google Workspace logs, and overstates Okta's, which trim further. The corpus the packs are evaluated on, synthetic logs of a few hundred bytes that repeat less than real ones, came to $27 a million (921 requests, $0.98).

Two things the count must include:

- People. A request is one actor's logs: up to thirty new ones, and its latest ten already judged again (`_context`), which the model answers too. A busy actor, a service role or an app server, fills one with new logs. A quiet one, a person, goes an hour after its first new log. Count a person as their logs plus ten for each hour they are active.
- Throughput. At 2–10 s a Haiku 5.5 request, the judge's `concurrency: 4` keeps up with 35k–170k requests a day, 1.4M–6.8M logs; raise `inference.judge.concurrency` past that.

#### Per provider
Rough ranges for a company of 50 people, each active about eight hours a day, priced by the rule above:

| pack | grows with | a log | a day | judged as | Haiku 5.5, a day |
|---|---|---|---|---|---|
| [aws](/packs/aws) | services, scheduled jobs and deploys, more than people | ~2 KB | 50k–500k events, 0.1–1 GB | the same | $2–20 |
| [google_workspace](/packs/google_workspace) | people: 100–500 events each, most of them Drive | ~0.5 KB | 5k–25k events, 3–13 MB | 16k–32k logs | $0.35–0.70 |
| [okta](/packs/okta) | people: 20–100 events each, sign-ins, MFA and app launches | ~2 KB | 1k–5k events, 2–10 MB | 16k logs | $0.60 |
| [github](/packs/github) | engineers: 20–200 events each, pushes, reviews and Actions runs | ~0.3 KB | 1k–10k events, 0.3–3 MB | 16k logs | $0.30 |

A person costs about a cent a day or less in each source that sees them, however little they do. A trail that also logs data events (S3 objects, Lambda invocations) can have ten to a hundred times aws's events. AWS VPC Flow Logs have no pack: a startup writes 5M–30M flow records a day of about 0.1 KB, $80–500 a day by the rule and more than one judge keeps up with, so a pack for them would roll flows up before the judge.

The examples keep every finished log once in their lake, Parquet files in a directory an hour, about 20 bytes a log on a day of CloudTrail, which deleting old hours trims; the judge's groups wait in memory; `state.sqlite` grows with actors, their active hours, role sessions and sign-ins, and each actor's ten latest records the judge saw, not with logs. A local model, the default, costs only the machine it runs on.

These are estimates. Log sizes are measured on samples (CloudTrail on 20k events, the others on tens to hundreds), tokens with Claude Haiku 4.5's count scaled to 5.5's, and the volumes are typical of startups, not yours: a day of your own logs in the lake says what yours are.

### Writing a pack
1. Copy [packs/okta](/packs/okta), the smallest, to a directory beside it under `packs/`, and rename its package and plugin; a name may not contain `-`. Its `Cargo.toml` finds `normkit` and `factskit` at `../shared/`; a copy kept elsewhere points them at `<checkout>/packs/shared/...`. Its `probe()` takes your `source` values and nothing else: a value belongs to one pack. `process_logs` fills a `normkit::Norm`, trims the record, and hands both to `factskit`, which enriches the address, adds the profile's facts and writes the record ([packs/enrich](/packs/enrich#the-record) has the contract).
2. `ingest.yaml`: the plugin, named after the pack, with its fixtures under `tests:` and the tables they were written against (`tests/profile.yaml`, and `tests/addresses.yaml` and `tests/networks.csv` for the addresses). No `dag`: the config that includes the pack routes it. behavior-baseline learns a `source` only once [its table](/packs/behavior-baseline/kit/src/kinds/mod.rs) knows it.
3. A row for each of its sources in [judge's ingest.yaml](/packs/judge/ingest.yaml), under `inference.judge.encoding.sources.<source>`: the fields naming the principal and the time, and those the model should see on every line. Your pack's own `_facts` keys go in its sources' `keep` too: the judge's top-level `keep` is every pack's.
4. `tests/input.json` from synthetic records, two or more, with the tables they read: the company they describe, and what you want their addresses to be (`203.0.113.9` a Tor exit, say). `make -C packs fixtures-<name>` writes `tests/expected.json` from the plugin's output; read it before committing. `make -C packs test` runs every pack's fixtures on its own.
5. So the release ships it prebuilt: `prebuilt:` on the plugin in `ingest.yaml`, as the other packs have it, and the plugin in [packs/dist.sh](/packs/dist.sh)'s list and [dist-workspace.toml](/dist-workspace.toml)'s `extra-artifacts`. `packs/dist.sh check` (CI runs it) says when the list and the configs disagree; `make -C packs dist` builds what the release would.

## How well does it work?

The security packs are measured against [tangent-eval](https://github.com/EthanBlackburn/tangent-eval): 66 cases of attacks and look-alikes across cloud, identity, GitHub, supply chain, SaaS and AI-agent logs, Datadog's 35 recorded AWS attack techniques, and, with the macOS pack, a working week of a developer's Mac, each inside months of ordinary traffic. Its README says what the cases are, what has been measured, and how to run it yourself (`make` there, about $1 on Bedrock or free with a local model).

## Benchmarks
Benchmarks show end-to-end throughput with simple log transformation plugins implemented in [Rust](/examples/rust/src/lib.rs), [Go](/examples/golang/main.go), and [Python](/examples/python/mapper.py). Each plugin is compiled to WASM and run inside Tangent.

| Source → Sink | Rust | Go | Python |
|---|---|---|---|
| tcp → blackhole | `483.35 MB/s` | `399.54 MB/s` | `76.91 MB/s` |

_Measured in November 2025 on a dedicated 16-core AMD machine, with `tangent run` and `tangent bench` pinned to separate cores. To run one yourself (`examples/golang` and `examples/python` the same way):_

```sh
tangent plugin compile --config examples/rust/tangent.yaml
tangent run --config examples/rust/tangent.yaml &
tangent bench --config examples/rust/tangent.yaml --seconds 20 --connections 16 --payload examples/rust/tests/bench.json --synthesize
```

## Install
```bash
# Homebrew
brew tap telophasehq/telophase
brew install tangent-cli
tangent --version

# with install script
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/EthanBlackburn/tangent/releases/latest/download/tangent-cli-installer.sh | sh
tangent --version

# cargo
cargo install --git https://github.com/EthanBlackburn/tangent tangent-cli

# docker
docker pull ghcr.io/telophasehq/tangent-toolchain
docker run --rm --init ghcr.io/telophasehq/tangent-toolchain <command>

```

## Quick links
* Intro
  * [What is Telophase?](https://docs.telophasehq.com/index)
  * [Installation](https://docs.telophasehq.com/installation)
  * [Quickstart](https://docs.telophasehq.com/quickstart)
  * [Runtime Deepdive](https://docs.telophasehq.com/runtime)
* WASM Plugins
  * [Authoring](https://docs.telophasehq.com/plugins/authoring)
  * [Community](https://docs.telophasehq.com/plugins/community)
* CLI
  * [Overview](https://docs.telophasehq.com/cli/overview)
  * [`tangent run`](https://docs.telophasehq.com/cli/run)
  * [`tangent plugin scaffold`](https://docs.telophasehq.com/cli/plugin/scaffold)
  * [`tangent plugin compile`](https://docs.telophasehq.com/cli/plugin/compile)
  * [`tangent plugin test`](https://docs.telophasehq.com/cli/plugin/test)
  * [`tangent bench`](https://docs.telophasehq.com/cli/bench)
* Configuration
  * [`tangent.yaml`](https://docs.telophasehq.com/configuration/tangent-yaml)
  * [Runtime](https://docs.telophasehq.com/configuration/runtime)
  * [Plugins](https://docs.telophasehq.com/configuration/plugins)
  * [DAG](https://docs.telophasehq.com/configuration/dag)
  * Sources
    * [Overview](https://docs.telophasehq.com/configuration/sources/overview)
    * [SQS](https://docs.telophasehq.com/configuration/sources/sqs)
    * [MSK](https://docs.telophasehq.com/configuration/sources/msk)
    * [Socket](https://docs.telophasehq.com/configuration/sources/socket)
    * [File](https://docs.telophasehq.com/configuration/sources/file)
  * Sinks
    * [Overview](https://docs.telophasehq.com/configuration/sinks/overview)
    * [S3](https://docs.telophasehq.com/configuration/sinks/s3)
    * [File](https://docs.telophasehq.com/configuration/sinks/file)
    * [Blackhole](https://docs.telophasehq.com/configuration/sinks/blackhole)


## License

Apache-2.0. See `LICENSE`.
