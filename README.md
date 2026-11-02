<p align="center">
  <img src="https://github.com/user-attachments/assets/7da02584-0a4e-4e41-af6d-61cb081029c4" alt="Tangent logo" height=170>
</p>
<h1 align="center">Tangent</h1>
<p align="center"><em>Stream processing without DSLs. Shareable, sandboxed, fast.</em></p>


<p align="center">
  <a href="https://github.com/telophasehq/tangent/actions">
  <img height="20" src="https://img.shields.io/github/actions/workflow/status/telophasehq/tangent/scaffold_test.yml?label=build&logo=github" alt="Build status"></a>
  <a href="https://twitter.com/telophasehq">
  <img src="https://img.shields.io/twitter/follow/telophasehq?style=social" alt="X">
  </a>

</p>

<p align="center">
  <a href="https://docs.telophasehq.com">Documentation</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://discord.gg/ZUHB3BRa8c">Discord</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/telophasehq/tangent/issues/new">Issues</a>
</p>

## What is Tangent?
Tangent is a stream‑processing toolkit that treats **plugins** (user‑defined functions) as first‑class citizens – exceptionally easy to write and share. Perfect for vibe-coding those pesky log transformations.


Plugins run in a lightweight WASM sandbox with near-native speed and full language flexibility — no DSLs, no vendor-locked runtimes. Plugins are designed to be shareable, so common transformations (e.g. GuardDuty findings → OCSF) can be written once and shared with the [community](https://github.com/telophasehq/tangent-plugins).

Tangent ships with everything you need to develop, test, and benchmark your own transforms:
* `tangent plugin scaffold` – generate plugin boilerplate (`--setup` also installs the toolchain)
* `tangent plugin compile` – compile plugins to WASM
* `tangent plugin test` – run plugin tests
* `tangent bench` – measure throughput and latency before deploying
* `tangent run` – start the Tangent runtime
* `tangent config` – print a config as the runtime reads it, its includes merged

## Why use Tangent?
1. **Use real languages, not DSLs** – Real code > DSL. Reviewable, testable, LLM‑friendly.

2. **Catch breakage before prod** — `tangent plugin test` for correctness; `tangent bench` for throughput/latency.

3. **Shareable and secure** – Data transformations are easy to write and _share_. Publish and discover open-source plugins in the [Tangent Plugins library](https://github.com/telophasehq/tangent-plugins). Each plugin runs in its own lightweight sandbox.

## State and partitioning
Plugins keep state through the `state` interface: `state.Get`, `state.Set`, `state.Incr`, `state.Del`, and `state.Scan` for a key range, each write with an optional TTL. That state lives in the runtime's **state store**, configured under `runtime.state`:

```yaml
runtime:
  state:
    backend: sqlite        # or lru (in-process, bounded); distributed backends slot in here
    path: state.sqlite
    default_ttl_ms: 600000 # 0 = never expire
    max_ttl_ms: 3600000    # cap on any TTL; 0 = no cap
    max_entries: 100000    # lru only: evict the least recently used past this
  partition:
    key: [org_id]          # fields that identify a record's partition
    timestamp_key: ts      # the record's own time (RFC 3339 or epoch); arrival time without it
    retain_ms: 432000000   # 5d: keep every record under its partition, keyed by its time
```

Entries are keyed by `(namespace, partition, key)`. Plugins share one namespace, `plugins`, so what one plugin writes the next can read; within a partition all of it runs on one worker, so there is nothing to race against (below). Give a plugin a `state_namespace` of its own when it must not share, such as two instances of one plugin with different configs:

```yaml
plugins:
  detect_login:  { module_type: rust, path: ./detect, state_namespace: login }
  detect_export: { module_type: rust, path: ./detect, state_namespace: export }
```

A plugin may read files too: `data: <dir>` mounts a directory, relative to the file that names it, read-only at `/data` in the guest, for reference data too big for `config`. [examples/enrich](/examples/enrich) reads MaxMind GeoLite2 and the cloud providers' ranges that way, and its `setup.sh` fetches them. Each plugin instance on each worker reads such files into its own memory; reference data that several plugins read, or that is large, belongs in an [enrichment table](#enrichment-tables), which the process loads once.

When a plugin's call fails (it traps or returns an error), the records it was given are logged and dropped. `drop_on_error: false` passes them on unchanged instead, as if the plugin were not there: right for a plugin that only adds to records, such as context for a model, wrong for a filter. `tangent plugin test` always drops, so a failed call fails the test.

With `runtime.partition` set, a partition's records all go to one worker, which hands them to a plugin one partition at a time with its state calls scoped to that partition, so "first time this tenant did X" is a `state.Get` away with no change to the plugin and no race between workers. Without it, everything is in the empty partition. [examples/partition](/examples/partition) counts events per org with a single `count` key.

With `retain_ms` as well, the runtime keeps every partitioned record in the store, in the plugins' namespace, as `ev:<epoch ms, 13 digits>:<seq>` with that TTL, so a partition's history is a key range: `state.Scan("ev:<from>", "ev:<to>", limit)` returns the last hour in order, however it arrived, and nothing is ever read back to be rewritten. The store is the partition's log; the TTL is its retention. A record is stored once, where it enters its partition, whether the key was on it at the source or a plugin added it. What a record means in the light of that history is a plugin's decision, and the hour of context it attaches for a model is one `Scan`. Each backend answers a range its own way: SQLite walks the primary-key index, a DynamoDB backend would query the sort key.

The store is readable while the pipeline runs:

```bash
tangent state --config tangent.yaml ls                          # keys per namespace and partition
tangent state --config tangent.yaml get seen --partition acme   # one entry (--ns defaults to the only namespace)
tangent state --config tangent.yaml scan --prefix user: --limit 50
tangent state --config tangent.yaml sql "select partition, count(*) from state group by 1"   # sqlite backend only
```

Another pipeline can read it too: `read_only: true` under `runtime.state` opens the same SQLite file without the lock and creates nothing, so one pipeline learns while another reads what it learned. The file must exist and hold the writer's table, so the writer starts first. Its writes fail, and an expired entry reads as missing; a pipeline whose runtime writes state (`partition.retain_ms`, a source that polls an API) is refused.

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

A plugin built before the interface existed does not import it and runs as before. To call it from an existing plugin, copy [assets/wit/processor.wit](/assets/wit/processor.wit) into its `.tangent/wit/`; a plugin that imports it needs a runtime that provides it.

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

A partition value's records not yet sent go oldest first, `batch_size` to a batch, one batch of a value at a time. Fewer wait until the oldest is `timeout_secs` older than the newest record stored, so a replay of old logs goes as live ones do, or until nothing new has been stored for `timeout_secs`; then they go topped up to `batch_size` with the value's latest records already sent, which carry `"_context": true`. A batch's new records get the time it is through the DAG in `emitted`, NULL until then, and the value's next batch goes then; a restart sends again what was not. Nothing is deleted: the table grows, and trimming it is the operator's job.

## Inference
An `inference` node asks a model about each record. Nodes are configured under `inference:`, each with its own model and prompt, and sit in the DAG as `kind: inference`:

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

`encoding: {type: logfmt, keep: [...], sources: {<source>: {principal, time, keep, ids}}}` writes the records as logfmt lines in about half the tokens of JSON, and the text decodes back to the records: a principal's records are grouped, the fields they share written once above their rows, the `keep` fields on every row, a day's timestamps as times of day; `sidecar: true` swaps a source's `ids` for references the model never sees. Each pack's `judge.yaml` has its sources' rows, e.g. [packs/aws/judge.yaml](/packs/aws/judge.yaml).

Inference nodes run inside the workers, in DAG order with the plugins: a worker hands a model the records that reached the node, waits for the replies, and carries them on, so a partition's records stay in order and nothing else moves while the model thinks. `concurrency` bounds requests in flight per worker (1 for a laptop GPU, 8 or more for a hosted model); `timeout_ms` (30 s) frees a slot a hung request would otherwise hold; `retries` (2) tries again, backing off, when a request got no reply at all. `temperature` is sent only when set. A signal (SIGINT or SIGTERM) ends the calls under way rather than wait out `timeout_ms` and `retries`: records waiting on one go to no sink and are not acked, so a `sqlite` source sends them again at its next start. Another signal, a second or more after the first, exits at once. See [examples/inference-local](/examples/inference-local) and [examples/inference-bedrock](/examples/inference-bedrock).

## Including files
A config can span several files. `include:` lists files merged into the one that names it, each path relative to that file:

```yaml
include:
  - packs/core/ingest.yaml
  - packs/app/ingest.yaml
plugins:
  app: { config: { extra_sources: [billing] } }   # one setting of a plugin an included file defines
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

`tangent run` checks the DAG before it starts: an edge to a node no file defines is an error. It warns about a source with no edge out, and names once a source whose records none of its targets take, as when no included plugin's probe claims a record's `source`; `tangent_unrouted_records_total` counts every record dropped that way, per node.

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
| [app](/packs/app) | `app`, `agent`, and names you list | NDJSON from your product: its audit log, an AI agent's run log | the customer's tenant as the org |
| [macos](/packs/macos) | | planned: Endpoint Security events from laptops | |

A pack is a directory: the plugin (`Cargo.toml`, `src/`, Rust compiled to WASM), its fixtures (`tests/`), `ingest.yaml` (the plugin, its fixtures, and its edge to `logs` and `baseline_builder`), `judge.yaml` (how the judge writes the provider's records as logfmt), and a README saying what it reads, writes, drops and enriches, with the lines to paste.

[packs/core](/packs/core) is not a provider but what every pack needs: the enrichment tables, the `logs` sink and `baseline_builder` (`ingest.yaml`), and `baseline_renderer` and the judge (`judge.yaml`). The packs' shared code is in [packs/shared](/packs/shared): `normkit` (`_norm` and the scrubs), `enrichkit` (an address from the tables), `identitykit` (which names are one person) and `factskit` (the profile's facts), Rust libraries each plugin links.

No pack loads IP data itself. Core's `ingest.yaml` names it as enrichment tables (GeoLite2 Country and ASN, and `networks.csv`, the cloud, datacenter, VPN and Tor ranges in one file), the runtime loads each once when the pipeline starts, and every pack's plugin, on every worker, reads that one copy: five packs load GeoLite2 once. [examples/enrich/setup.sh](/examples/enrich/setup.sh) fetches the files. Your profile is a table too, `profile`, which your config names.

The judge also sees each actor's history, which one window of its records cannot show. Core's baseline folds every finished record into ingest's state store (`baseline_builder`), and in the judge puts the same `_baseline` on each of an actor's records (`baseline_renderer`): who it is and, for a role, who assumed it; how long it has been seen; what it does for the first time (an API, a resource, a client, a network, an OAuth app); its volume and hours against its own 28 days; its person's sign-ins and credential changes in every source over three days (the profile's `people` says which names are one person); and addresses it shares with other actors. Only what happened before the actor's first record counts, and it is facts, not verdicts ([packs/core](/packs/core#baseline)). The judge writes records as logfmt, `_baseline` once per request, about 5% more tokens than without it; JSON lines (`encoding: {type: ndjson}`) repeat it on every record, at three times logfmt's tokens ([packs/core](/packs/core#judgeyaml) has the numbers). Baseline keeps one writer: ingest runs one worker and no `runtime.partition`, since more workers lose its counts and a partition files its keys where the judge does not read them.

### Using packs
Packs need a tangent built from the same checkout (the first command below): `include:` and `enrichment_tables` are newer than the latest release, which stops at ``missing field `runtime` ``.

Ingest and the judge are two processes over one `logs.sqlite`, so there are two configs, each including core and the packs you want. Your ingest config also names your sources, sends each to its pack, and points the `profile` table at your company. To watch AWS and Okta from a directory beside your tangent checkout:

```yaml
# ingest.yaml
include:
  - ../tangent/packs/core/ingest.yaml
  - ../tangent/packs/aws/ingest.yaml
  - ../tangent/packs/okta/ingest.yaml

enrichment_tables:
  profile: { type: document, path: profile/profile.yaml, required: true }   # your company

sources:
  cloudtrail: { type: cloudtrail, region: "${AWS_REGION}", queue_url: "${CLOUDTRAIL_QUEUE_URL}" }
  okta: { type: okta, org_url: "${OKTA_ORG_URL}", token: "${OKTA_API_TOKEN}" }

dag:
  - from: { kind: source, name: cloudtrail }
    to: [{ kind: plugin, name: aws }]
  - from: { kind: source, name: okta }
    to: [{ kind: plugin, name: okta }]
```

```yaml
# judge.yaml
include:
  - ../tangent/packs/core/judge.yaml
  - ../tangent/packs/aws/judge.yaml
  - ../tangent/packs/okta/judge.yaml
```

```yaml
# profile/profile.yaml: what no log says about you; packs/core/README.md lists every key
org: example
own_sources: [cloudtrail, okta]      # their records are all this org's
countries: [US]
members: [alice@example.com, bob@example.com]
people: { alice@example.com: [alice] }   # one person under each name: the IAM user alice is alice@example.com
networks: [203.0.113.0/24]           # your office and VPN egress
watch: [StopLogging, user.mfa.factor.deactivate]
```

```bash
cargo install --path ../tangent/crates/cli --locked   # a tangent that knows include: and enrichment tables
../tangent/examples/enrich/setup.sh               # once: Rust WASM toolchain; GeoLite2, cloud ranges, Tor exits
tangent plugin compile --config ingest.yaml && tangent plugin compile --config judge.yaml
tangent plugin test --config ingest.yaml          # the aws and okta fixtures
tangent run --config ingest.yaml & i=$!            # first: it writes state.sqlite
(while kill -0 $i && [ ! -s state.sqlite ]; do sleep 1; done; sleep 1; kill -0 $i) && tangent run --config judge.yaml
```

Ingest starts first: the judge reads its `state.sqlite`, each actor's history, read-only, and refuses to start before the file is there. The second line waits for the file, and starts the judge only while ingest is still running.

The judge is any OpenAI-compatible model: `JUDGE_URL` (a local model on `:8080` by default), `JUDGE_MODEL` (`glm-4.7-flash`), `JUDGE_API_KEY`. Each pack's README has its source's block and credentials, and [packs/core](/packs/core) the profile's keys and the judge's settings.

A `tcp` source can feed several packs: route it to each, and each takes only its own `source` values. A record whose `source` no included pack takes goes no further, and `tangent run` says so. To keep records in your own shape, send them as `source: app`, or list their name under `plugins.app.config.extra_sources`: baseline learns them as it does `app`'s, and the judge's logfmt wants a row for the name, as [packs/app/judge.yaml](/packs/app/judge.yaml) has for `app`, for its times and the fields on every line.

[examples/cloud-infra](/examples/cloud-infra) is every pack for a fictional company, with the cases it is measured against; [examples/app-log](/examples/app-log) is the app pack, one org per customer.

### Writing a pack
1. Copy [packs/okta](/packs/okta), the smallest, to a directory beside it under `packs/`, and rename its package and plugin; a name may not contain `-`. Its `Cargo.toml` finds `normkit` and `factskit` at `../shared/`; a copy kept elsewhere points them at `<checkout>/packs/shared/...`. Its `probe()` takes your `source` values and nothing else: a value belongs to one pack. `process_logs` fills a `normkit::Norm`, trims the record, and hands both to `factskit`, which enriches the address, adds the profile's facts and writes the record ([packs/core](/packs/core) has the contract).
2. `ingest.yaml`: the plugin, named after the pack, with its fixtures under `tests:` and the tables they were written against (`tests/profile.yaml`, and `tests/addresses.yaml` and `tests/networks.csv` for the addresses), and an edge from it to `logs` and `baseline_builder`. Baseline learns a `source` only once [its table](/packs/core/baseline/kit/src/kinds/mod.rs) knows it.
3. `judge.yaml`: under `inference.judge.encoding.sources.<source>`, the fields naming the principal and the time, and those the model should see on every line. Your pack's own `_facts` keys go in its sources' `keep` too: core's `keep` is every pack's, and a list set in two files is a conflict, not a merge.
4. `tests/input.json` from synthetic records, two or more, with the tables they read: the company they describe, and what you want their addresses to be (`203.0.113.9` a Tor exit, say). `make -C packs fixtures-<name>` writes `tests/expected.json` from the plugin's output; read it before committing. `make -C packs test` runs every pack's fixtures on its own.

## Benchmarks
Benchmarks show end-to-end throughput with simple log transformation plugins implemented in [Rust](/examples/rust/src/lib.rs), [Go](/examples/golang/main.go), and [Python](/examples/python/mapper.py). Each plugin is compiled to WASM and run inside Tangent.

<!-- BENCH_START -->
| Source → Sink | Rust | Go | Python |
|---|---|---|---|
| tcp → blackhole | `483.35 MB/s` | `399.54 MB/s` | `76.91 MB/s` |
<!-- BENCH_END -->

_Benchmarks are run nightly on a dedicated 16-core AMD runner. See [nightly benchmarks](https://github.com/telophasehq/tangent/actions/workflows/bench.yml)._

## Install
```bash
# Homebrew
brew tap telophasehq/telophase
brew install tangent-cli
tangent --version

# with install script
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/telophasehq/tangent/releases/latest/download/tangent-cli-installer.sh | sh
tangent --version

# cargo
cargo install --git https://github.com/telophasehq/tangent tangent-cli

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
