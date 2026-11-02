# google_workspace

Google Workspace's activity into finished records, by one plugin,
`google_workspace`. It takes the records whose `source` is `google_workspace`:
login, token, drive and admin activity as tangent's `google_workspace` source
reads it from the Reports API, or the same records sent as NDJSON.

## What it writes

`_norm` ([packs/core](../core#the-record) has the contract):

| field | from |
|---|---|
| `actor` | `actor.email` |
| `org` | none of its own: your org when `google_workspace` is in the profile's `own_sources` |
| `kind` | the first event's `name` |
| `ip` | `ipAddress` |

`_facts` is what the profile and the tables say; Workspace adds no facts of
its own.

Each record goes to `logs` and to core's [baseline](../core#baseline) too,
which folds it into its actor's baseline: events, documents, OAuth apps,
addresses, sign-ins, 2-step and password changes, and an admin's resets of
someone else's. That is what the judge's `_baseline` says of the actor.

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

In your ingest config, beside [core](../core):

```yaml
include:
  - ../tangent/packs/core/ingest.yaml
  - ../tangent/packs/google_workspace/ingest.yaml

enrichment_tables:                  # your company: packs/core/README.md#the-profile
  profile: { type: document, path: profile/profile.yaml, required: true }

sources:
  google_workspace:
    type: google_workspace
    credentials_file: ${GOOGLE_APPLICATION_CREDENTIALS}
    subject: ${GOOGLE_ADMIN_EMAIL}

dag:
  - from: { kind: source, name: google_workspace }
    to: [{ kind: plugin, name: google_workspace }]
```

and `google_workspace` in your profile's `own_sources`. In your judge config,
`../tangent/packs/google_workspace/judge.yaml`.

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

Cases in [examples/cloud-infra](/examples/cloud-infra/cases) with Workspace
records: `data/gmail-forwarding-rule` and
`vendor/ai-app-oauth-token-stolen-sso-pivot`.
