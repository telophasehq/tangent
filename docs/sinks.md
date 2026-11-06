# Sinks

A sink is where records go. Each has a name under `sinks:` and a `type`, and
an edge into it is its name ([configuration](configuration.md#edges)):

```yaml
sinks:
  alerts: { type: file, path: alerts.ndjson, compression: { type: none } }
dag:
  - from: flagged
    to: [alerts]
```

| `type` | writes | page |
|---|---|---|
| `file` | NDJSON appended to one file | [below](#file) |
| `rolling_file` | whole files a query engine reads (Parquet, NDJSON), a directory an hour if you like | [below](#rolling_file) |
| `s3` | objects in a bucket | [below](#s3) |
| `sqlite` | every record once, in a table a `sqlite` source reads back | [below](#sqlite) |
| `slack` | a message per record to an incoming webhook | [below](#slack) |
| `blackhole` | nothing | [below](#blackhole) |

Every sink takes these too:

| key | default | |
|---|---|---|
| `compression` | `{ type: zstd, level: 3 }` | `none`, `gzip` (`level` 6), `zstd` (`level` 3); Avro also takes `snappy` and `deflate` |
| `encoding` | `{ type: ndjson }` | `ndjson`, `json`, `avro` (`schema`), `parquet` (`schema`, `record_column`, [below](#rolling_file)) |
| `object_max_bytes` | `134217728` (128 MiB) | a WAL file is sealed at this many bytes of records (`s3`, `rolling_file`) |
| `in_flight_limit` | `16` | writes in flight at once: sealed files uploaded or moved into place (`s3`, `rolling_file`); every sink's together bound the batches waiting on the sinks |

A `default` key (`false`) is accepted and does nothing in this version.

## file

A `file` sink appends every write to one file at `path`, which only NDJSON
survives, so it takes no other encoding. Give it `compression: { type: none }`
for a file you will read with `cat`.

## rolling_file

A `rolling_file` sink keeps records as whole files for a query engine
(DataFusion, DuckDB, Spark) to read, starting a new file each time the WAL an
`s3` sink uses seals one: a write is acked once it is in the WAL under
`wal_path`, a WAL file is sealed at `object_max_bytes` of records or
`max_file_age_seconds` after it was opened, and each sealed file becomes one
file in the directory, in the sink's `encoding`, synced and renamed into
place (copied under a hidden temporary name first if the WAL is on another
filesystem), so a reader listing the directory sees whole files only.

- `path` may hold strftime specifiers, rendered in UTC when a write reaches
  the sink, so `lake/logs/%Y/%m/%d/%H` puts each hour's files in a directory
  of their own, which a query engine can skip by path; a file holds records
  written in that hour, whatever times they carry.
- A file is named as an `s3` object is, `<ULID>.parquet` (`.ndjson.zst` for
  zstd NDJSON), and the ULIDs sort by time.
- `wal_path` is the WAL's own directory: no other sink's, and not inside
  `path`, where a reader would find the files it stages; tangent will not
  start otherwise. `max_file_age_seconds` is 60 unless set.
- Parquet has the columns its `schema` names, in order, each a name and a
  type: `string`, `int` (64 bits), `float` (a double), `bool`, `timestamp`
  (milliseconds in UTC, read from RFC 3339 text in any zone), or a map of
  columns, a struct, for a nested object; every column may be null.
- `record_column` keeps each record beside the columns, its line without the
  line break, for the fields the schema leaves out.
- A record the schema cannot read (a type it does not give the field, a time
  arrow cannot parse) does not fail its file: it is kept in `record_column`
  with its typed columns null, or, without a record column or if it is not
  JSON, dropped; `tangent_sink_records_unfit_total{fate="kept"|"dropped"}`
  counts them.

```yaml
sinks:
  lake:
    type: rolling_file
    path: lake/logs/%Y/%m/%d/%H
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

A write is acked once it is in the open WAL file; that file is synced only
when it is sealed. What was acked survives tangent stopping without its flush
(killed, crashed, out of time at shutdown): the next start seals and sends
the files left open, cut back to their last whole line. It does not survive
the machine going down before the file is sealed.

## s3

The same WAL as `rolling_file`, each sealed file uploaded as one object,
named `<ULID>.<encoding>` (`.ndjson.zst` for zstd NDJSON) at the bucket's
root. Credentials come from the AWS SDK's default chain.

| key | default |
|---|---|
| `bucket_name` | |
| `region` | the SDK's |
| `wal_path` | `/tmp/wal` |
| `max_file_age_seconds` | `60` |

## sqlite

A `sqlite` sink keeps every record once, in its `table`, with a column per
partition field holding the field's value as text, NULL without it. A
partition is an index on its fields and the record's time,
`<table>_<partition>`; `pending` and `at` are taken by the table's own
indexes. A `sqlite` source, in the same process or another, reads one
partition back in batches, each of one partition value; it takes the
partition's fields from that index:

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

A partition value's records not yet sent go oldest first, `batch_size` to a
batch, one batch of a value at a time. Fewer wait until the oldest is
`timeout_secs` older than the newest record stored, so a replay of old logs
goes as live ones do, or until nothing new has been stored for
`timeout_secs`; then they go topped up to `batch_size` with the value's
latest records already sent, which carry `"_context": true`. A batch's new
records get the time it is through the DAG in `emitted`, NULL until then, and
the value's next batch goes then; a restart sends again what was not. The
table is a queue: once a batch is through the DAG, the same transaction
deletes the value's sent records past its latest `batch_size` - 1, the most a
top-up uses, so it holds what is not sent yet and what tops up each value's
next batch. Keep the full history elsewhere, in a `rolling_file` sink. A
record that arrives after its value's later ones were sent is topped up with
what is left.

A plugin's `group_by` ([plugins](plugins.md#group_by)) does the same in
memory, with no table: it is what core's judge uses. The `sqlite` pair is for
a queue that must outlive the process.

## slack

A message to a Slack incoming webhook for each record, from a template:

| key | default |
|---|---|
| `webhook_url` | (an `https://` URL; left out of `tangent config`) |
| `text` | `{_norm.actor} {_norm.kind}: {judge.why\|not judged: judge.error}` |

In `text`, `{a.b}` is the record's field (`-` when it has none) and
`{x|words y}` the first of them it has. A field's value is escaped: a log
line cannot mention anyone, link or start a line in the message. A post that
fails is logged and dropped. [packs/core](../packs/core#slack) has the
template for every pack's records.

## blackhole

Takes every record and keeps none: for benchmarks ([benchmarks](benchmarks.md)).

## Metrics

`tangent_sink_bytes_total` counts a file's bytes when it is in place, and
`tangent_sink_bytes_uncompressed_total` the records' NDJSON bytes it holds; a
write acked into the WAL counts only once its file is sealed and written.
[configuration](configuration.md#metrics) lists the rest.
