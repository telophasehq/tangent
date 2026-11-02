# baseline

Core's two plugins for `_baseline`: what the state store knew about an actor
before the first of its records in a call. [packs/core](../README.md#baseline)
says what they learn and what `_baseline` says; this is how.

| directory | plugin | where | what it does |
|---|---|---|---|
| [builder](builder) | `baseline_builder` | ingest | folds every record into state, each person with what they did; emits nothing |
| [renderer](renderer) | `baseline_renderer` | the judge | puts `_baseline` on each of an actor's records; writes nothing |
| [kit](kit) | | both link it | the keys, the event read off each source's record, the time helpers, the store as `Kv` |

They run in different processes and share only the keys, so the kit holds
everything both must agree on. The builder is a file per kind of key it
writes; the renderer is a file per `_baseline` key, and `before.rs`, which
every one of them reads state through.

Who is who is resolved in ingest, where the profile is. An actor's person is
its record's `_norm.person`, which the pack's plugin wrote from the profile's
`people`; the builder asks `people` itself
([identitykit](../../shared/identitykit)) only for a record without one, and
for the persons a credential change was done to. It files a person with each
row that names someone, and the actors each person was seen as. The renderer
has no profile and resolves no one: a name that neither a record nor state
gives a person is its own, lowercased.

Each probe takes every record: one it cannot read, with no `_norm.actor` or
from a source it does not know, the builder passes over and the renderer
passes on unchanged. A state call that fails fails the call; in the judge,
`drop_on_error: false` then sends the records on without `_baseline`.

## Keys

In the namespace `baseline`, with no partition: each key starts with the org.
`|` separates the parts, `%` and `|` in a value are %-escaped, `ms13` is epoch
milliseconds in 13 digits. Nothing expires.

| key | value |
|---|---|
| `<org>\|a\|<actor>\|first` | when first seen (a minimum) |
| `<org>\|a\|<actor>\|d\|<day>` | events that day |
| `<org>\|a\|<actor>\|h\|<hour>` | events per five minutes of the hour, by category, as JSON |
| `<org>\|a\|<actor>\|n\|<dim>\|<value>` | when first seen, for `api`, `src`, `res`, `client`, `oauth`, `device`, `token`, `cat`, `mfa`, `by` (who assumed this role), and `person` (its person, when who is who names one: how a federated user's issuer is found) |
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
`cloudtrail`, `github_audit`, `okta`, `google_workspace`, `app`, `gha_ci` and
`runner_egress`. A record of any other source with an `event` is read as
`app`'s: the app pack's audit shape under a name in its `sources` or
`extra_sources`. The packs could later write these as a normalized field
instead, and this table would go.

## Tests

```bash
make -C packs test-baseline      # cargo test in kit, builder and renderer, then story.sh
make -C packs fixtures-baseline  # story.sh write: tests/expected.json from the story
```

`tests/input.json` is a sign-in story, records as the packs write them: office
logins, a failed one from another country, an MFA reset by an admin, the
sign-in from the new country, an aliased IAM user's bucket policy twenty
minutes later, an AssumeRole and the role's call, an SSO session, and an
agent's record with no actor. [test.yaml](test.yaml) sends each record to the
renderer and then the builder, a call each, over one state, so its
`_baseline` is what the records before it taught. `tangent plugin test` runs
one plugin, so [story.sh](story.sh) runs the pipeline and compares.
