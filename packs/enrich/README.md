# enrich

Not a provider: the tables every pack's plugin reads. A config includes
[ingest.yaml](ingest.yaml) beside the packs it uses, or includes
[judge](../judge), which includes it.

## ingest.yaml

**The enrichment tables** every pack's plugin reads, from the files
[examples/enrich/setup.sh](/examples/enrich/setup.sh) fetches into
examples/enrich/data. The runtime loads each once when the pipeline starts,
and every plugin on every worker reads that one copy. A missing file is a
warning, and the fields it would give are left out.

| table | type | file | what a pack reads from it |
|---|---|---|---|
| `geo` | `mmdb` | GeoLite2-Country.mmdb | an address's country |
| `asn` | `mmdb` | GeoLite2-ASN.mmdb | its network's number and owner |
| `networks` | `cidr` | networks.csv | its labels: `tor` and `vpn` are flags, and any other label names its hosting provider (`aws`, `gcp`, `cloudflare`, `datacenter`; the first in the file wins) |
| `profile` | `document` | yours | everything else ([below](#the-profile)); your config names it |

**The state store's expiry** (`runtime.state`), as
[behavior-baseline](../behavior-baseline)'s file sets it: nothing expires
unless its writer says when, and no TTL is capped. A polling source remembers
each event it sent for three times its lateness, three hours for
google_workspace. Under the runtime's default cap of an hour it would forget
them while its polls still read them, and send them again.

No sources, no sinks and no profile: your config adds them, sends each source
to its pack, and sends each pack's plugin on to where its records go: a sink
of yours ([the lake](../README.md#the-lake) in the examples),
[behavior-baseline](../behavior-baseline), the judge. A plugin with no edge
out keeps nothing: its records go nowhere.

## The record

What a pack's plugin writes, the same for every provider (`factskit::Finished`):

| field | always there | what it is |
|---|---|---|
| `ts` | yes, text | the record's own `ts`, or the time its pack read it when it had none |
| `source` | yes, text | the shape it came in as: `cloudtrail`, `okta`, `app`, ... |
| `_norm` | yes, an object | who did what, from where, to what (below); each field may be missing |
| `_facts` | yes, an object | what the profile and the tables say (below) |
| the rest | no | the provider's fields the pack kept, as the pack trimmed them |

A sink can rely on `ts`, `source` and `_norm` and nothing else: the
examples' lake keeps `ts` and `_norm` as columns, and the judge groups by
`_norm`'s org and actor. A raw record's own
`_norm`, `_facts`, `_ip`, `_context` and `_baseline` (and a key naming a field
under one, such as `_facts.notes`) are dropped first, so a log line cannot
speak for the pipeline.

`_norm`, who did what, from where, to what:

| field | what it is |
|---|---|
| `actor` | who acted: a user, role or key; for CI and an agent's run, the repository or nothing |
| `person` | the person the actor is, when the profile's `people` names them |
| `org` | whose it is: your org for your own sources (the profile's `org` and `own_sources`), else the record's own tenant |
| `kind` | what happened: the event's name |
| `ip` | the address it came from; this one is enriched |
| `country` | the country the provider says it came from |
| `dest` | the host it went to |
| `target` | what it touched: a bucket, role, repository, application |

`actor` and `org` are always kept, since they route the record, and so is
`person`; the others only when the record does not already hold the value.

`_facts`, what the profile and the tables say:

| key | when |
|---|---|
| `principal` | `member`, `service`, `key` or `unknown` from the profile; `aws` when AWS acted on its own; none for a repository |
| `about` | the profile's line on the actor |
| `watch` | the kind is one you watch |
| `ip` | the address: `country`, `asn`, `org`, `hosting`, and `tor`, `vpn`, `ours`, `private` when true |
| `ips` | other addresses a pack names, by field (no pack does yet) |
| `key_known_network` | a key called from the networks you expect, or not |
| `dest_known` | the destination is a host you talk to, or not |
| `sensitive_target` | the target is one you mark sensitive |
| `notes` | what a model reads all this by, a sentence each |

A pack adds what only its provider's log says: `aws` adds `root` and
`world_open`, each with its note after the watch note.

## The profile

Every fact about your company that no log carries, as the `profile` table: a
YAML file your config names. Who is who is resolved as records come in, and
reaches the judge on the records (`_norm.person`) and in the state store.

```yaml
enrichment_tables:
  profile: { type: document, path: profile/profile.yaml, required: true }
```

| key | what it is | what the record gets |
|---|---|---|
| `org`, `own_sources` | your name; the `source` values that are your own infrastructure | their records are all one org, yours; list your packs' sources here |
| `countries` | where your people are | a note when a person acts from elsewhere |
| `cloud` | where your services run | a note when a service acts from any other hosting |
| `networks` | your own addresses: office, VPN, CI egress | `ip.ours`; never a person's odd location |
| `members` | people: IdP emails, GitHub handles | `principal: member`; anyone else is `unknown` |
| `people` | each person's other names, a list or one: IAM users, GitHub handles (`dave@example.com: [dave]`) | `_norm.person` when the actor is the person or one of their names; `principal` still comes from `members`, so list the names there too; in `_baseline`, their history, sign-ins and networks as one person's |
| `services` | IAM users and roles, bots | `principal: service`, held to `cloud` |
| `keys` | API keys and the networks each calls from | `principal: key`, `key_known_network` |
| `about` | who an actor is, when its name does not say | `about`, in place of the "nobody it knows" note |
| `hosts` | hosts your systems talk to (`*.suffix` allowed) | `dest_known` |
| `sensitive` | buckets, repos, tables | `sensitive_target` |
| `watch` | kinds a person wants seen every time | `watch` |

`required: true` makes a mistyped path an error that stops `tangent run`;
without it the table would answer nothing, with one warning, and every record
would go on as nobody the org knows, in the provider's org rather than yours.

## Writing a pack's plugin

A pack's plugin links [packs/shared](/packs/shared) by path:

- `normkit::Norm` is `_norm`, filled from the raw record. It also carries what
  the pack read off the log for `_facts`: `principal` (`"aws"` or `"none"`;
  unset, the profile decides), `fact(key, note)` for what only that log says,
  and `address(field, text)` for another address to enrich.
- `normkit::record`, `scrubbed` and the trim helpers give the record without
  the reserved keys, its per-event ids or its URL signatures.
- `normkit::Stamp::of(&read)` takes `ts` and `source` from the record as
  read, before the trim, so a trim need not keep them.
- `factskit::kit(lookup)` reads the profile and the tables once per instance,
  and `kit.write(out, stamp, record, norm)` folds the org, names the person,
  enriches the addresses, adds the facts and writes the record as one line.

The pack's `ingest.yaml` defines the plugin and its fixtures, and no edges:
the config that includes the pack routes it to its sink, `baseline_builder`
and `baseline_renderer`. behavior-baseline reads a source it knows from its
own table ([kit/src/kinds](../behavior-baseline/kit/src/kinds)); a new pack's records pass
through it unlearned, with no `_baseline`, until the table knows their
`source`, unless they carry an `event`, which it reads as the app pack's audit
shape.
