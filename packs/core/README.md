# core

Not a provider: what every pack needs, and the judge. Every config includes
it, beside the packs it uses.

```
your sources ──▶ a pack's plugin ──▶ logs.sqlite                                      (ingest.yaml)
                   reads the tables geo, asn, networks and profile

logs.sqlite ──▶ actors ──▶ judge ──▶ fresh ──▶ flagged ──▶ alerts.ndjson              (judge.yaml)
                                       └──▶ verdicts.ndjson
```

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

- **`logs`**, the sink every pack's plugin has an edge to: `logs.sqlite`, each
  record once, at its `ts`, with two partitions, the org and the org and actor.
  Nothing is deleted; trimming it is yours to do.
- **The runtime**: two workers, plugins compiled into `plugins/` beside the
  config you run, and the state store at `state.sqlite`, where the API
  sources keep their place.

No sources and no profile: your config adds them, and sends each source to
its pack.

## The record

What a pack's plugin writes, the same for every provider. A raw record's own
`_norm`, `_facts`, `_ip`, `_context` and `_hist` (and a key naming a field
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
YAML file your ingest config names.

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
| `people` | each person's other names, a list or one: IAM users, GitHub handles (`dave@example.com: [dave]`) | `_norm.person` when the actor is the person or one of their names; `principal` still comes from `members`, so list the names there too |
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

- **actors** reads `logs.sqlite` back forty records of one actor at a time,
  oldest first, topped up with the actor's latest records already judged,
  marked `_context`.
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

The records go to the model as JSON lines. One line in your judge config
sends them as logfmt instead, about half the tokens, each source written as
its pack's `judge.yaml` says:

```yaml
inference:
  judge:
    encoding:
      type: logfmt
```

### Slack

What **flagged** keeps can go to a channel too: add a `slack` sink and its
edge to your judge config, and export `SLACK_WEBHOOK_URL`, an incoming
webhook's URL.

```yaml
sinks:
  slack:
    type: slack
    webhook_url: ${SLACK_WEBHOOK_URL}
    text: "*{_norm.actor}* {_norm.kind|eventName|eventType|action|event|step|events.0.name} from {_norm.ip|sourceIPAddress|client.ipAddress|ipAddress|actor_ip|ip|client_ip}: {judge.why|not judged: judge.error}"
dag:
  - from: { kind: plugin, name: flagged }
    to: [{ kind: sink, name: slack }]
```

In `text`, `{a.b}` is the record's field (`-` when it has none) and `{x|words y}`
the first of them it has, so a record the model could not judge says why. A
field's value is escaped: a log line cannot mention anyone, link or start a
line in the message. A post that fails is logged and dropped.

## Writing a pack's plugin

A pack's plugin links [packs/shared](/packs/shared) by path:

- `normkit::Norm` is `_norm`, filled from the raw record. It also carries what
  the pack read off the log for `_facts`: `principal` (`"aws"` or `"none"`;
  unset, the profile decides), `fact(key, note)` for what only that log says,
  and `address(field, text)` for another address to enrich.
- `normkit::record`, `scrubbed` and the trim helpers give the record without
  the reserved keys, its per-event ids or its URL signatures.
- `factskit::kit(lookup)` reads the profile and the tables once per instance,
  and `kit.write(out, record, norm)` folds the org, names the person,
  enriches the addresses, adds the facts and writes the record as one line.

## What include cannot do

- **Take a stage out of, or put one into, core's chain.** Edges add up and none
  can be removed, so `actors -> judge` stays. A plugin between them (the
  pending `history` stage, say) means copying `judge.yaml` instead of including
  it, or a change here that every config gets.
- **Keep a batch's order.** A source routed to several packs hands each its own
  records in turn, so within a batch every `cloudtrail` record reaches
  `logs.sqlite` before every `okta` one. `actors` reads back by time, so only
  records with the same `ts` can change places, and no verdict moves.
