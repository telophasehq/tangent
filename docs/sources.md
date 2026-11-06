# Sources

A source is where records come in. Each has a name under `sources:` and a
`type`, and an edge sends what it reads on ([configuration](configuration.md#edges)):

```yaml
sources:
  events: { type: tcp, bind_address: 127.0.0.1:9400 }
dag:
  - from: events
    to: [app]
```

| `type` | reads | page |
|---|---|---|
| `tcp` | NDJSON over TCP | [below](#tcp) |
| `socket` | NDJSON over a Unix socket | [below](#socket) |
| `file` | one file, once | [below](#file) |
| `sqs` | an SQS queue's messages, or the S3 objects they announce | [below](#sqs) |
| `msk` | a Kafka topic (Amazon MSK) | [below](#msk) |
| `sqlite` | a `sqlite` sink's records, a partition value at a time | [sinks](sinks.md#sqlite) |
| `cloudtrail` | a CloudTrail trail's files, through SQS | [packs/aws](../packs/aws#use-it) |
| `github_audit` | a GitHub organization's audit log | [packs/github](../packs/github#use-it) |
| `okta` | Okta's System Log | [packs/okta](../packs/okta#use-it) |
| `google_workspace` | Google Workspace's Reports API | [packs/google_workspace](../packs/google_workspace#use-it) |
| `github_webhook` | GitHub webhooks, and the logs of finished workflow runs | [below](#github_webhook) |
| `npm_registry` | new versions of npm packages | [below](#npm_registry) |

A record is one JSON object. Sources that take bytes (`file`, `sqs`, `msk`)
say how to read them under `decoding`:

```yaml
decoding:
  format: { type: ndjson }      # ndjson, json, json-array, msgpack or text
  compression: { type: auto }   # auto (the default), none, gzip or zstd
```

`auto` reads the compression off the object's content encoding, then its
name (`.gz`, `.zst`), then its first bytes.

## tcp

NDJSON, one record a line, from any number of connections. Anyone who can
reach the address can send records, so bind it to `127.0.0.1` or keep it
behind a firewall.

| key | default | |
|---|---|---|
| `bind_address` | `0.0.0.0:9000` | |
| `read_buffer_size` | `524288` | bytes read at a time from a connection (8 KiB at least) |

## socket

NDJSON over a Unix domain socket.

| key | default |
|---|---|
| `socket_path` | `/tmp/sidecar.sock` |

## file

One file, read whole when the pipeline starts, then nothing more: a replay
or a test, with `tangent run --once`.

| key | default | |
|---|---|---|
| `path` | | relative to the working directory |
| `decoding` | | [above](#sources) |

## sqs

Long-polls an SQS queue. A message that is an S3 event notification
(`Records[].s3`) has each object it names read from S3; any other message's
body is the records.

| key | default | |
|---|---|---|
| `queue_url` | | |
| `wait_time_seconds` | `20` | the long poll |
| `visibility_timeout` | `60` | seconds |
| `decoding` | | [above](#sources) |

Credentials come from the AWS SDK's default chain.

## msk

A Kafka topic, as Amazon MSK serves it.

| key | default |
|---|---|
| `bootstrap_servers` | |
| `topic` | |
| `group_id` | `tangent-node` |
| `security_protocol` | `PLAINTEXT` |
| `ssl_ca_location`, `ssl_certificate_location`, `ssl_key_location` | |
| `auth` | `{ mode: scram, sasl_mechanism: SCRAM-SHA-512, username, password }` |
| `decoding` | [above](#sources) |

## The API sources

`cloudtrail`, `github_audit`, `okta` and `google_workspace` read the
providers' audit logs with nothing in between, and each wakes up when its
credential is set: left empty, it says so once and stays idle. Each pack's
README has the block to paste and what the credential needs.

| `type` | keys (default) |
|---|---|
| `cloudtrail` | `queue_url`, `region`, `profile`, `bucket_role` (a role to assume to read another account's bucket, as an organization trail's log archive) |
| `github_audit` | `org`, `token` (`read:audit_log`), `interval_secs` (60), `since` (`1h`, how far back on first start), `include` (`all`; or `web`, `git`), `api` (`https://api.github.com`) |
| `okta` | `org_url`, `token`, `interval_secs` (30), `since` (`1h`) |
| `google_workspace` | `credentials_file` (a service account's key, with domain-wide delegation), `subject` (the admin it acts as), `applications` (`login`, `token`, `drive`, `admin`), `interval_secs` (60), `since` (`1h`), `api` (`https://admin.googleapis.com`) |

`github_audit`, `okta` and `google_workspace` keep their place in the state
store ([state](state.md)), so a restart goes on from where they were.

## github_webhook

An HTTP endpoint for GitHub's webhooks. A finished workflow run's logs are
fetched with `token` and sent on as records, a line each. With `secret`,
every request's `X-Hub-Signature-256` is checked. See
[examples/githubwebhooks](../examples/githubwebhooks).

| key | default |
|---|---|
| `bind_address` | `0.0.0.0:9000` |
| `path` | |
| `secret` | |
| `token` | |

## npm_registry

Polls `registry.npmjs.org` and sends each new version of a package on as a
record. See [examples/npmpackageupdates](../examples/npmpackageupdates).

| key | default | |
|---|---|---|
| `packages` | | package names; this or `orgs` |
| `orgs` | | every package of these orgs |
| `interval_secs` | `5` | |
| `token` | | |
