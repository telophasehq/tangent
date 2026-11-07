# google_workspace

Google Workspace's activity into finished records, by one plugin,
`google_workspace`. It takes the records whose `source` is `google_workspace`:
login, token, drive and admin activity as tangent's `google_workspace` source
reads it from the Reports API, or the same records sent as NDJSON.

## What it writes

`_norm` ([enrich-ip](../enrich-ip#the-record) has the contract):

| field | from |
|---|---|
| `actor` | `actor.email` |
| `org` | none of its own: your org when `google_workspace` is in the profile's `own_sources` |
| `kind` | the first event's `name` |
| `ip` | `ipAddress` |

`_facts` is what the profile and the tables say; Workspace adds no facts of
its own.

Your config routes each record to its sink and to
[behavior-baseline](../behavior-baseline), which folds it into its actor's
baseline: events, documents, OAuth apps, addresses, sign-ins, 2-step and
password changes, and an admin's resets of someone else's. That is what the
judge's `_baseline` says of the actor.

## What it drops

Kept: `ts`, `source`, `ipAddress`, `ownerDomain`; `id`'s `time` and
`applicationName`; the actor's `email` and `callerType`; each event's `type`
and `name`, with its parameters as `name: value` (whichever of `value`,
`boolValue`, `intValue`, `multiValue`, `multiIntValue` or `messageValue` it
has), less `billable`.

Dropped: `kind`, `etag`, `id.uniqueQualifier`, `id.customerId`, the actor's
`profileId`, and each parameter's own shape.

## What it enriches

`ipAddress`, as `_norm.ip`: its country, network and owner, hosting, Tor and
VPN, and whether it is one of yours (`_facts.ip`). Workspace's records hold
no other address in the shapes the cases have; an event parameter that does
would go through `Norm::address`.

## Use it

In your config, beside [judge](../judge), which brings enrich-ip and
behavior-baseline:

```yaml
include:
  - ../tangent/packs/judge/ingest.yaml
  - ../tangent/packs/google_workspace/ingest.yaml

enrichment_tables:                  # your company: packs/enrich-ip/README.md#the-profile
  profile: { type: document, path: profile/profile.yaml, required: true }

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
  workspace_audit:
    type: google_workspace
    credentials_file: ${GOOGLE_APPLICATION_CREDENTIALS}
    subject: ${GOOGLE_ADMIN_EMAIL}

dag:
  - from: workspace_audit
    to: [google_workspace]
  - from: google_workspace   # kept, learned, judged
    to: [lake, baseline_builder, baseline_renderer]
```

and `google_workspace` in your profile's `own_sources`.

`GOOGLE_APPLICATION_CREDENTIALS` is a service account key file with
domain-wide delegation for the scope
`https://www.googleapis.com/auth/admin.reports.audit.readonly`;
`GOOGLE_ADMIN_EMAIL` is the administrator it acts as (the Admin SDK API
enabled on the project). The source keeps its place in the runtime's state
store; on first start it reads the last hour (`since`). Left without
credentials, it says so once and stays idle.

Kinds worth the profile's `watch`: `EMAIL_FORWARDING_OUT_OF_DOMAIN`.

## Test it

[tests/input.json](tests/input.json): Workspace records from the cases, one carrying the
keys only the pipeline writes. [tests/expected.json](tests/expected.json) is
what the plugin makes of them against the tables in `tests/` (the company,
GeoLite2's answers for their addresses, their labels), which `ingest.yaml`
names for the test in any config.

```bash
make -C packs test-google_workspace
make -C packs fixtures-google_workspace   # after a change; read the diff
```

Cases in [tangent-eval's cases](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases) with Workspace
records: `data-exfil/gmail-forwarding-rule` and
`vendor/ai-app-oauth-token-stolen-sso-pivot`.
