# baseline

| directory | plugin | where | what it does |
|---|---|---|---|
| [kit](kit) | | both link it | the keys, the event read off each source's record, the time helpers, the store as `Kv` |

## Keys

In the namespace `baseline`, with no partition: each key starts with the org.
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
make -C packs test-baseline      # cargo test in kit
```
