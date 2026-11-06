# core

Not a provider: what every pack needs, and the judge. Every config includes
it, beside the packs it uses.

```
your sources ──▶ a pack's plugin ──┬──▶ lake/, every record as Parquet                      (ingest.yaml)
                   reads the       ├──▶ baseline_builder ──▶ state.sqlite
                   tables geo,     │                              │
                   asn, networks   └──▶ baseline_renderer ◀───────┘                         (judge.yaml)
                   and profile          an actor's records, a group at a time
                                             │
                                             ▼
                                           judge ──▶ fresh ──┬──▶ flagged ──▶ alerts.ndjson
                                                             └──▶ verdicts.ndjson
```

One config holds all of it, in one process: the judge's groups run on
workers of their own, so ingest never waits on the model. A pack defines
only its plugin, and your config draws its three edges, to `lake`,
`baseline_builder` and `baseline_renderer`. Without judge.yaml, draw only
the one to `lake`: every record kept, and no model.

## ingest.yaml

- **The enrichment tables** every pack's plugin reads, from the files
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

- **`lake`**, a sink your config routes each pack's plugin to: every record, kept, as Parquet files in
  `lake/logs/<year>/<month>/<day>/<hour>/`, each record whole in `record` and
  `ts`, `source` and `_norm`'s org, actor, kind and ip as columns, for
  DataFusion or DuckDB to read. Kind and ip are empty when the log already
  holds them ([below](#the-record)), which is nearly always, so a query
  reads them from `record` (CloudTrail's `eventName`, Okta's
  `client.ipAddress`). A file is sealed an hour after it opened, and
  only sealed files are there; delete old hours to trim it.
- **`baseline_builder`**, a plugin your config routes each pack's plugin to
  as well: it folds each record into the state store, under the namespace
  `baseline`, and passes nothing on ([below](#baseline)). Only
  `baseline_renderer` reads it, so a pipeline without the judge routes
  nothing here.
- **The runtime**: a worker per CPU ([below](#baseline)); plugins compiled
  into `plugins/` beside the config you run; and the state store at
  `state.sqlite`, where the API sources keep their place and baseline its
  keys.

No sources and no profile: your config adds them, sends each source to its
pack, and sends each pack's plugin on to `lake`, `baseline_builder` and
`baseline_renderer`. A plugin with no edge out keeps nothing: its records go
nowhere.

## The record

What a pack's plugin writes, the same for every provider (`factskit::Finished`):

| field | always there | what it is |
|---|---|---|
| `ts` | yes, text | the record's own `ts`, or the time its pack read it when it had none |
| `source` | yes, text | the shape it came in as: `cloudtrail`, `okta`, `app`, ... |
| `_norm` | yes, an object | who did what, from where, to what (below); each field may be missing |
| `_facts` | yes, an object | what the profile and the tables say (below) |
| the rest | no | the provider's fields the pack kept, as the pack trimmed them |

A sink can rely on `ts`, `source` and `_norm` and nothing else: the lake
keeps `ts` and `_norm` as columns, and the judge groups by `_norm`'s org and
actor. A raw record's own
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

## judge.yaml

- **baseline_renderer**, which your config routes each pack's plugin to too,
  groups each actor's records (`group_by: [_norm.org, _norm.actor]`): a group
  closes at 30 records (`max_events`) or an hour after its first record
  (`max_age_ms`), however busy the actor; an hour's quiet (`expire_after_ms`)
  closes it no sooner. It reaches the renderer whole, on workers of its own,
  with the actor's latest ten records again ahead of it, marked `_context`
  (`history: 10`): forty records a request, which `batch_bytes` is sized for.
  Groups wait in memory; the end of input closes every open one, and so does
  a signal (Ctrl-C, a restart). After a signal, though, the model calls have
  ended: the groups it closes and the requests the model has not answered
  get no verdict, their records in the lake and the baseline, and nothing
  asks about them again. The renderer
  reads `state.sqlite` and puts the same `_baseline` on each of an actor's
  records ([below](#baseline)). `drop_on_error: false`: when it fails, the
  records go to the model without `_baseline` rather than not at all.
- **judge** reads an actor's records together and writes a verdict per record,
  with [judge.md](judge.md) as its brief and
  [judge.schema.json](judge.schema.json) as the verdict's shape.
- **fresh** drops the `_context` records; **flagged** keeps the verdicts a
  person should see, in `alerts.ndjson`. Every verdict goes to
  `verdicts.ndjson`.

The judge is any OpenAI-compatible model, set by the environment:

| variable | default |
|---|---|
| `JUDGE_URL` | `http://localhost:8080/v1` |
| `JUDGE_MODEL` | `glm-4.7-flash` |
| `JUDGE_API_KEY` | `none` |

Serve the default with llama.cpp's `llama-server`, which turns the verdict
schema into a grammar, so replies are always well-formed and their reasons
stay short (`mlx_lm.server` accepts the schema and ignores it, and with this
model about one reply in five came back unreadable):

```bash
llama-server -m GLM-4.7-Flash-Q4_K.gguf --port 8080 -np 1 -c 32768 -ngl 99 -fa on --jinja --temp 0.7 --top-p 0.95
```

For a hosted one: `JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY`.

The records go to the model as logfmt, a line each, each source written as
its row under `encoding.sources` says: about half the tokens of JSON lines, and an
actor's `_baseline` written once per request. Your config can send JSON
lines instead, where `_baseline` is on every record; give it half the
`batch_bytes` then, so a request fits a 32k context:

```yaml
inference:
  judge:
    batch_bytes: 48000
    encoding:
      type: ndjson
```

Measured on the requests of the corpus the packs are evaluated on, an
actor's forty records each, with GLM-4.7-Flash's tokenizer, `judge.md` and
the schema included:

| encoding | `batch_bytes` | requests | tokens, median | 90th percentile | largest |
|---|---|---|---|---|---|
| logfmt, without `_baseline` | 48000 | 487 | 6,508 | 8,298 | 15,928 |
| logfmt (the default) | 96000 | 485 | 6,817 | 8,604 | 18,560 |
| JSON lines, without `_baseline` | 48000 | 487 | 9,089 | 10,323 | 21,086 |
| JSON lines | 96000 | 485 | 20,262 | 22,620 | 33,303 |
| JSON lines | 48000 | 872 | 12,738 | 15,689 | 20,200 |

Logfmt was measured with Claude Haiku 5.5, where it caught what JSON caught;
the default model has not been measured with either.

### Slack

What **flagged** keeps can go to a channel too: add a `slack` sink and its
edge to your config, and export `SLACK_WEBHOOK_URL`, an incoming
webhook's URL.

```yaml
sinks:
  slack:
    type: slack
    webhook_url: ${SLACK_WEBHOOK_URL}
    text: "*{_norm.actor}* {_norm.kind|eventName|eventType|action|event|step|events.0.name} from {_norm.ip|sourceIPAddress|client.ipAddress|ipAddress|actor_ip|ip|client_ip}: {judge.why|not judged: judge.error}"
dag:
  - from: flagged
    to: [slack]
```

In `text`, `{a.b}` is the record's field (`-` when it has none) and `{x|words y}`
the first of them it has, so a record the model could not judge says why. A
field's value is escaped: a log line cannot mention anyone, link or start a
line in the message. A post that can work later (a 429, after its
`retry-after`, a 5xx, a timeout) is tried again, and its record is acked
only once Slack has it; one Slack refuses (another 4xx) is logged and
dropped.

## baseline

Two plugins, in [baseline](baseline): in ingest `baseline_builder` learns
what each actor does, in the judge `baseline_renderer` tells the model. They
answer the question one window of forty records cannot, "is this normal for
this identity?", from the state store alone, and only from what happened
before the actor's first record in the request: nothing at or after it
counts, so a window never explains itself.

**What it learns** (ingest, `baseline_builder`), from every record with a
`_norm.actor` whose `source` it knows: `cloudtrail`, `github_audit`, `okta`,
`google_workspace`, `app`, `gha_ci` and `runner_egress`, and any other with an
`event`, as the app pack's audit shape under a name of yours. Others, `agent`
among them, it passes over.

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

**What it stamps** (judge, `baseline_renderer`): `_baseline`, one object,
the same on each of an actor's records in the request, `_context` ones
included; a key is left out when it has nothing to say.

| key | what it says |
|---|---|
| `actor` | who it is to the profile, its person and the actors that person was seen as before (when they name anyone but the actor), the identities seen here (IAM user, assumed role, okta user, ...); for a role, who assumed it, and who assumed them, up to four hops and twelve hours back, or its SSO permission set; the privilege it used before (admin, write, read-only, none); a session's MFA against its earlier sessions' |
| `seen` | how long it has been seen, its events and active days, from which sources; or `none: first seen in this window` |
| `new` | what it does here for the first time, or first did within the hour: `api`, `resource`, `client`, `network` (with `(new to org)`), `source`, `oauth`, `device`, `token`; or `nothing: ...` |
| `volume` | the last hour against the p90 of its active hours in 28 days, in total and per category, as `normal`, `2-5x`, `5-10x`, `>10x` or `none before` |
| `time` | the window's hours against its usual hours in 28 days, and how long since it was last active; or dormant |
| `auth` | the last 72 hours of its person, and of whoever is behind a role: logins per source, failed logins, MFA, password, key and grant changes, and the last login's address, network and MFA |
| `links` | when an OAuth app in the window was first seen; a login from the window's address; a login from a network new for the person in the hour before; an address new for this actor, and who used it before |

The text is printable ASCII with no `"`, `\` or `=`, so logfmt writes each
key as a bare line, once per request with the fields an actor's records
share; JSON lines repeat it on every record ([the cost](#judgeyaml)).

- **People**: `people` in the profile ([above](#the-profile)) says which names
  are one person, as an IAM user `dave` is `dave@example.com`; a name not
  there is its own person, and one other name may be written bare
  (`dave@example.com: dave`). Who is who is resolved once, in ingest: a
  pack's plugin writes the actor's person on the record as `_norm.person`,
  and the builder files each person with what they did: the actors they were
  seen as, their sign-ins and credential changes, the role sessions they
  assumed. The renderer reads persons off the records and out of state and
  resolves no one.
- **Any number of workers**: baseline's keys carry the org, and the builder
  reads an hour's counts, adds to them and writes them back, as it does
  first-seen times and role sessions, all in one state transaction per batch,
  so no worker's update lands inside another's.
- **Fail-open**: in the judge, `drop_on_error: false`: when a call fails, the
  records go on without `_baseline`.

[baseline/README.md](baseline/README.md) has the keys they keep and how they
are tested.

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
the config that includes the pack routes it to `lake`, `baseline_builder`
and `baseline_renderer`. Baseline reads a source it knows from its own table
([baseline/kit/src/kinds](baseline/kit/src/kinds)); a new pack's records pass
through it unlearned, with no `_baseline`, until the table knows their
`source`, unless they carry an `event`, which it reads as the app pack's audit
shape.

## What include cannot do

- **Take a stage out of, or put one into, core's chain.** Edges add up and none
  can be removed, so `baseline_renderer -> judge` stays. Another plugin
  between them, or a judge without `baseline_renderer`, means copying
  `judge.yaml` instead of including it, or a change here that every config
  gets, as baseline was. Each pack's edges are your config's own, so where a
  pack's records go is yours to say.
- **Keep a batch's order.** A source routed to several packs hands each its own
  records in turn, and several workers hand an actor's records to its group as
  they finish, so a group holds them in the order they arrived, not by
  `ts`.
