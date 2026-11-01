# history

Core's plugin for `_hist`: what the state store knew about an actor before the
first of its records in a call. [packs/core](../README.md#history) says what it
learns and what `_hist` says; this is how.

| `config.mode` | where | what it does |
|---|---|---|
| `learn` | ingest | files the profile's `people`, folds every record into state; emits nothing |
| `header` | the judge | puts `_hist` on each of an actor's records, with the `people` learn filed; writes nothing |
| `both` | tests | files `people`, then header, then learn, in one call |

Its probe takes every record: one it cannot read, with no `_norm.actor` or
from a source it does not know goes on unchanged. A state call that fails
fails the call; in the judge, `drop_on_error: false` then sends the records on
without `_hist`.

## Keys

In its namespace, `history`, with no partition: each key but `people` starts
with the org.
`|` separates the parts, `%` and `|` in a value are %-escaped, `ms13` is epoch
milliseconds in 13 digits. Nothing expires.

| key | value |
|---|---|
| `people` | the profile's `people` as learn last read it, JSON; the only key without an org |
| `<org>\|a\|<actor>\|first` | when first seen (a minimum) |
| `<org>\|a\|<actor>\|d\|<day>` | events that day |
| `<org>\|a\|<actor>\|h\|<hour>` | events per five minutes of the hour, by category, as JSON |
| `<org>\|a\|<actor>\|n\|<dim>\|<value>` | when first seen, for `api`, `src`, `res`, `client`, `oauth`, `device`, `token`, `cat`, `mfa`, and `by` (who assumed this role) |
| `<org>\|p\|<person>\|n\|<asn, cc or net>\|<value>` | when the person first used that network |
| `<org>\|o\|n\|<asn, cc or net>\|<value>` | when the org did |
| `<org>\|o\|ip\|<ip>\|<actor>` | when an actor first used an address |
| `<org>\|p\|<person>\|e\|<ms13>\|<hash>` | a sign-in or credential change, as JSON; a redelivered record writes the same row |
| `<org>\|p\|<person>\|f\|<minute, ms13>` | failed sign-ins that minute |
| `<org>\|s\|<account>\|<role>\|<session>\|<hour>` | who assumed the session that hour, as JSON |

Every read in `header` is cut at the window's start: first-seen times only
when earlier, rows only from before it, whole days and whole five-minute slots
only when they ended before it.

## Sources

[src/kinds.rs](src/kinds.rs) reads each source's own fields: its verb,
category, resources, client, identity and sign-in or credential event, for
`cloudtrail`, `github_audit`, `okta`, `google_workspace`, `app`, `gha_ci` and
`runner_egress`. A record of any other source with an `event` is read as
`app`'s: the app pack's audit shape under a name in its `sources` or
`extra_sources`. The packs could later write these as a normalized field
instead, and this table would go.

## Tests

```bash
make -C packs test-history   # cargo test, then tests/ through test.yaml in `mode: both`
make -C packs fixtures-history
```

`tests/input.json` is a sign-in story, records as the packs write them: office
logins, a failed one from another country, an MFA reset by an admin, the
sign-in from the new country, an aliased IAM user's bucket policy twenty
minutes later, an AssumeRole and the role's call, an SSO session, and an
agent's record with no actor. Each record is a call of its own, so its `_hist`
is what the records before it taught.
