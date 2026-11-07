# behavior-baseline

Not a provider: what each actor did before, from the state store, on its
records. Two plugins, both in [ingest.yaml](ingest.yaml): `baseline_builder`
learns what each actor does, and `baseline_renderer` tells whoever reads the
records, the [judge](../judge) or a sink of yours. They answer the question
one window of records cannot, "is this normal for this identity?", from the
state store alone, and only from what happened before the actor's first
record in the call: nothing at or after it counts, so a window never
explains itself.

## ingest.yaml

- **`baseline_builder`**, which your config routes each pack's plugin to: it
  folds each record into the state store, under the namespace `baseline`,
  and passes nothing on ([below](#what-it-learns)).
- **`baseline_renderer`**, which your config routes each pack's plugin to
  as well: it reads the state store and puts the same `_baseline` on each of
  an actor's records in a call ([below](#what-it-stamps)), however many
  actors the call holds. `drop_on_error: false`: when it fails, the records
  go on without `_baseline` rather than not at all. The judge groups its
  records an actor at a time for the model
  ([judge](../judge#ingestyaml)).
- **The state store** at `state.sqlite` (`runtime.state`), where the API
  sources keep their place and baseline its keys, which never expire.

The judge includes this file. Without the model, include it and
[enrich-ip](../enrich-ip)'s beside the packs, and send the renderer's records
to a sink of yours. enrich-ip's tables give each address its ASN and country;
without them the builder learns neither ([below](#what-it-learns)).

```yaml
include:
  - ../tangent/packs/enrich-ip/ingest.yaml
  - ../tangent/packs/behavior-baseline/ingest.yaml
  - ../tangent/packs/okta/ingest.yaml
enrichment_tables:
  profile: { type: document, path: profile/profile.yaml, required: true }
sources:
  okta_audit: { type: okta, org_url: "${OKTA_ORG_URL}", token: "${OKTA_API_TOKEN}" }
sinks:
  baselined: { type: file, path: baselined.ndjson }   # each record with its `_baseline`
dag:
  - from: okta_audit
    to: [okta]
  - from: okta
    to: [baseline_builder, baseline_renderer]
  - from: baseline_renderer
    to: [baselined]
```

## What it learns

`baseline_builder` learns from every record with a `_norm.actor` whose
`source` it knows: `cloudtrail`, `github_audit`, `okta`, `google_workspace`,
`gha_ci` and `runner_egress`. Others it passes over.

- when the actor was first seen, and its events per day and per five minutes,
  by category: read, write, IAM or policy on someone else, its own
  credentials, delete, sign-in;
- when it first called each API, touched each resource (bucket, role, user,
  secret, repository, document, path, ...), used each client, OAuth app, Okta
  device and GitHub token, from each source; only what succeeded counts as
  seen, since anyone can fail in someone's name;
- the networks (ASN, country, /24 or /48) each person and the org have used,
  and which actors have used each address;
- the actors each person the profile names has been seen as;
- each person's sign-ins, failed sign-ins and credential changes (MFA,
  passwords, keys, grants, OAuth grants), filed under the person they were
  done to as well;
- who assumed each role session, and when.

Each is a minimum, a sum or a row keyed by the event's time, so records that
arrive out of order leave the same state. Keys begin with `_norm.org`, so
orgs stay apart in one namespace, `baseline`, and never expire.

## What it stamps

`baseline_renderer` writes `_baseline`, one object, the same on each of an
actor's records in the call, `_context` ones included; a key is left out
when it has nothing to say.

| key | what it says |
|---|---|
| `actor` | who it is to the profile, its person and the actors that person was seen as before (when they name anyone but the actor), the identities seen here (IAM user, assumed role, okta user, ...); for a role, who assumed it, and who assumed them, up to four hops and twelve hours back, or its SSO permission set; the privilege it used before (admin, write, read-only, none); a session's MFA against its earlier sessions' |
| `seen` | how long it has been seen, its events and active days, from which sources; or `none: first seen in this window` |
| `new` | what it does here for the first time, or first did within the hour: `api`, `resource`, `client`, `network` (with `(new to org)`), `source`, `oauth`, `device`, `token`; or `nothing: ...` |
| `volume` | the last hour against the p90 of its active hours in 28 days, in total and per category, as `normal`, `2-5x`, `5-10x`, `>10x` or `none before` |
| `time` | the window's hours against its usual hours in 28 days, and how long since it was last active; or dormant |
| `auth` | the last 72 hours of its person, and of whoever is behind a role: logins per source, failed logins, MFA, password, key and grant changes, and the last login's address, network and MFA |
| `links` | when an OAuth app in the window was first seen; a login from the window's address; a login from a network new for the person in the hour before; an address new for this actor, and who used it before |

The text is printable ASCII with no `"`, `\` or `=`, so the judge's logfmt
writes each key as a bare line, once per request with the fields an actor's
records share; JSON lines repeat it on every record
([the cost](../judge#the-model)).

- **People**: `people` in the profile
  ([packs](../README.md#the-profile)) says which names are one person, as an
  IAM user `dave` is `dave@example.com`; a name not there is its own person,
  and one other name may be written bare (`dave@example.com: dave`). Who is
  who is resolved once, in ingest: a pack's plugin writes the actor's person
  on the record as `_norm.person`, and the builder files each person with
  what they did: the actors they were seen as, their sign-ins and credential
  changes, the role sessions they assumed. The renderer reads persons off
  the records and out of state and resolves no one.
- **Any number of workers**: baseline's keys carry the org, and the builder
  reads an hour's counts, adds to them and writes them back, as it does
  first-seen times and role sessions, all in one state transaction per batch,
  so no worker's update lands inside another's.

## The plugins

| directory | plugin | where | what it does |
|---|---|---|---|
| [builder](builder) | `baseline_builder` | ingest | folds every record into state, each person with what they did; emits nothing |
| [renderer](renderer) | `baseline_renderer` | before the judge, or your sink | puts `_baseline` on each of an actor's records; writes nothing |
| [kit](kit) | | both link it | the keys, the event read off each source's record, the time helpers, the store as `Kv` |

They never call each other and share only the keys, so the kit holds
everything both must agree on. The builder is a file per kind of key it
writes; the renderer is a file per `_baseline` key, and `before.rs`, which
every one of them reads state through.

Who is who is resolved in ingest, where the profile is. An actor's person is
its record's `_norm.person`, which the pack's plugin wrote from the profile's
`people`, else the actor lowercased; both plugins read it so. The builder asks
`people` itself ([identitykit](../shared/identitykit)) only for the other
names a record holds: the persons a credential change was done to, and a
federated user's issuer. It files a person with each row that names someone,
and the actors each person was seen as. The renderer has no profile and
resolves no one: a name that neither a record nor state gives a person is its
own, lowercased.

Each probe takes every record: one it cannot read, with no `_norm.actor` or
from a source it does not know, the builder passes over and the renderer
passes on unchanged. A state call that fails fails the call;
`drop_on_error: false` then sends the records on without `_baseline`.

## Keys

In the namespace `baseline`: each key starts with the org.
`|` separates the parts, `%` and `|` in a value are %-escaped, `ms13` is epoch
milliseconds in 13 digits. Nothing expires.

| key | value |
|---|---|
| `<org>\|a\|<actor>\|first` | when first seen (a minimum) |
| `<org>\|a\|<actor>\|d\|<day>` | events that day |
| `<org>\|a\|<actor>\|h\|<hour>` | events per five minutes of the hour, by category, as JSON |
| `<org>\|a\|<actor>\|n\|<dim>\|<value>` | when first seen, for `api`, `src`, `res`, `client`, `oauth`, `device`, `token`, `cat`, `mfa`, `by` (who assumed this role), and `person` (its person, when who is who names one; for a federated user's issuer, filed from the federated user's records too, so the issuer is named once it or a federated user it issued was seen) |
| `<org>\|p\|<person>\|n\|<asn, cc or net>\|<value>` | when the person first used that network |
| `<org>\|p\|<person>\|n\|actor\|<actor, lowercased>` | when the person was first seen as that actor; only a person who is who names |
| `<org>\|o\|n\|<asn, cc or net>\|<value>` | when the org did |
| `<org>\|o\|ip\|<ip>\|<actor>` | when an actor first used an address |
| `<org>\|p\|<person>\|e\|<ms13>\|<hash>` | a sign-in or credential change, as JSON: who did it and their person, the persons it was done to; a redelivered record writes the same row |
| `<org>\|p\|<person>\|f\|<minute, ms13>` | failed sign-ins that minute |
| `<org>\|s\|<account>\|<role>\|<session>\|<hour>` | who assumed the session that hour and their person, as JSON |

Every read in the renderer is cut at the window's start: first-seen times
only when earlier, rows only from before it, whole days and whole five-minute
slots only when they ended before it.

## Sources

[kit/src/kinds](kit/src/kinds) reads each source's own fields: its verb,
category, resources, client, identity and sign-in or credential event, for
`cloudtrail`, `github_audit`, `okta`, `google_workspace`, `gha_ci` and
`runner_egress`. A record of any other source passes through unlearned,
with no `_baseline`. The packs could later write these as a normalized field
instead, and this table would go.

## Tests

```bash
make -C packs test-baseline      # story.sh, then cargo test in kit, builder and renderer
make -C packs fixtures-baseline  # story.sh write: tests/expected.json from the story
```

`tests/input.json` is a sign-in story, records as the packs write them: office
logins, a failed one from another country, an MFA reset by an admin, the
sign-in from the new country, an aliased IAM user's bucket policy twenty
minutes later, an AssumeRole and the role's call, and an SSO session.
[test.yaml](test.yaml) sends each record to the renderer and then the
builder, a call each, over one state, so its `_baseline` is what the records
before it taught. `tangent plugin test` runs one plugin, so
[story.sh](story.sh) runs the pipeline and compares.
