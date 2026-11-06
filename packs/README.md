# packs

A pack is what tangent knows about one provider's logs, and it is one plugin,
named after the provider. Tangent's own sources read the logs (`cloudtrail`,
`github_audit`, `okta` and `google_workspace` read the providers' APIs; `tcp`
takes anything else as NDJSON with a `source` field), and the pack's plugin
turns each raw record into a finished one:

1. drops the keys only the pipeline writes (`_norm`, `_facts`, `_ip`,
   `_context`, `_baseline`), so a log line cannot speak for it;
2. normalizes it: `_norm` says who did what, from where, to what (`actor`,
   `person`, `org`, `kind`, `ip`, `country`, `dest`, `target`), the same way
   for every provider;
3. strips what is redundant or never decides a verdict: per-event ids,
   hashes, pagination, issued credentials, URL signatures;
4. enriches its address from the [enrichment tables](../docs/enrichment-tables.md):
   country, network and owner, hosting provider, Tor exit, VPN, one of yours;
5. adds under `_facts` what your company profile says about it (a member, a
   service, a key or nobody known; a usual place, destination and target or
   not; a kind you watch) and what only that provider's log can say.

A pack only adds facts. Whether a record is worth a person's time is the
judge's call ([core](core)).

| directory | takes `source` | from | of its own |
|---|---|---|---|
| [aws](aws) | `cloudtrail` | the `cloudtrail` source: a trail's files, every account of an organization trail | the role or SSO user as actor, `aws:<service>` when AWS acts on its own; `_facts.root`, `_facts.world_open` |
| [github](github) | `github_audit`, `gha_ci`, `runner_egress` | the `github_audit` source; Actions job logs and runner egress as NDJSON | a CI run told per repository, which is not a principal |
| [okta](okta) | `okta` | the `okta` source: the System Log | Okta's own risk signals kept |
| [google_workspace](google_workspace) | `google_workspace` | the `google_workspace` source: login, token, drive and admin activity | each event's parameters by name |
| [app](app) | `app`, `agent`, and names you list | NDJSON from your product: its audit log, an AI agent's run log | the customer's tenant as the org |
| [macos](macos) | | planned, not built: Endpoint Security events from laptops ([#85](https://github.com/EthanBlackburn/tangent/pull/85) is in review) | |
| [core](core) | | not a provider: what every pack needs, and the judge | |
| [shared](shared) | | not a pack: the Rust libraries every pack's plugin links | |

A `source` value belongs to one pack: two packs taking it would store each
record twice.

## core and shared

A pack is a directory: the plugin (`Cargo.toml`, `src/`, Rust compiled to
WASM), its fixtures (`tests/`), `ingest.yaml` (the plugin and its fixtures;
your config says where its records go), and a README saying what it reads,
writes, drops and enriches, with the lines to paste.

[core](core) is not a provider but what every pack needs: the runtime, the
enrichment tables, the lake and `baseline_builder` (`ingest.yaml`), and
`baseline_renderer`, which groups each actor's records, and the judge, with
a row for each pack's sources (`judge.yaml`). A pipeline includes
`ingest.yaml`, and `judge.yaml` for the model, in one config, and routes each
pack's plugin to them. Its README has the record contract and the profile's
keys. The packs' shared code is in [shared](shared): `normkit` (`_norm` and
the scrubs), `enrichkit` (an address from the tables), `identitykit` (which
names are one person) and `factskit` (the profile's facts), Rust libraries
each plugin links.

No pack loads IP data itself. Core's `ingest.yaml` names it as enrichment
tables (GeoLite2 Country and ASN, and `networks.csv`, the cloud, datacenter,
VPN and Tor ranges in one file), the runtime loads each once when the
pipeline starts, and every pack's plugin, on every worker, reads that one
copy: five packs load GeoLite2 once.
[examples/enrich/setup.sh](../examples/enrich/setup.sh) fetches the files.
Your profile is a table too, `profile`, which your config names.

The judge also sees each actor's history, which one window of its records
cannot show. Core's baseline folds every finished record into the state store
(`baseline_builder`), and in the judge puts the same `_baseline` on each of an
actor's records (`baseline_renderer`): who it is and, for a role, who assumed
it; how long it has been seen; what it does for the first time (an API, a
resource, a client, a network, an OAuth app); its volume and hours against
its own 28 days; its person's sign-ins and credential changes in every source
over three days (the profile's `people` says which names are one person); and
addresses it shares with other actors. Only what happened before the actor's
first record counts, and it is facts, not verdicts
([core](core/README.md#baseline)). The judge writes records as logfmt,
`_baseline` once per request, about 5% more tokens than without it; JSON lines
(`encoding: {type: ndjson}`) repeat it on every record, at three times
logfmt's tokens ([core](core/README.md#judgeyaml) has the numbers).
Baseline's folds are one state transaction per batch, so ingest runs as many
workers as it likes.

## Using packs

Packs need a tangent newer than 0.1.9, the latest release as this is written:
`include:` and `enrichment_tables` are newer, and that release stops at
``missing field `runtime` ``. Until the next release, build one from the
checkout (`cargo install --path ../tangent/crates/cli --locked`) and
`tangent plugin compile` the packs where the lines below fetch them: their
prebuilt plugins are released with tangent, from the next release on.

A pipeline is one config: core's `ingest.yaml` and `judge.yaml`, and the
`ingest.yaml` of each pack you want. It names your sources, sends each to its
pack and each pack's records on to the lake, `baseline_builder` and the judge
(`baseline_renderer`), and points the `profile` table at your company. Leave
out `judge.yaml` and the edges to those two and it keeps finished records in
the lake, with no model. Each actor's records wait in a group for the judge
(`baseline_renderer`'s `group_by`), and the judge runs on workers of its own,
so ingest never waits on the model. To watch AWS and Okta from a directory
beside your tangent checkout:

```yaml
# tangent.yaml
include:
  - ../tangent/packs/core/ingest.yaml
  - ../tangent/packs/core/judge.yaml
  - ../tangent/packs/aws/ingest.yaml
  - ../tangent/packs/okta/ingest.yaml

enrichment_tables:
  profile: { type: document, path: profile/profile.yaml, required: true }   # your company

sources:
  cloudtrail: { type: cloudtrail, region: "${AWS_REGION}", queue_url: "${CLOUDTRAIL_QUEUE_URL}" }
  okta_audit: { type: okta, org_url: "${OKTA_ORG_URL}", token: "${OKTA_API_TOKEN}" }

dag:
  - from: cloudtrail
    to: [aws]
  - from: okta_audit
    to: [okta]
  - from: aws     # kept, learned, judged
    to: [lake, baseline_builder, baseline_renderer]
  - from: okta
    to: [lake, baseline_builder, baseline_renderer]
```

```yaml
# profile/profile.yaml: what no log says about you; packs/core/README.md lists every key
org: example
own_sources: [cloudtrail, okta]      # their records are all this org's
countries: [US]
members: [alice@example.com, bob@example.com]
people: { alice@example.com: [alice] }   # one person under each name: the IAM user alice is alice@example.com
networks: [203.0.113.0/24]           # your office and VPN egress
watch: [StopLogging, user.mfa.factor.deactivate]
```

```bash
brew install gh                                   # once: it checks each prebuilt plugin's attestation; no login
../tangent/examples/enrich/setup.sh data          # once: GeoLite2, cloud ranges, Tor exits
tangent plugin fetch --config tangent.yaml        # the packs' plugins, prebuilt and attested
tangent plugin test --config tangent.yaml         # the aws and okta fixtures
tangent run --config tangent.yaml
```

`tangent plugin compile` in place of `fetch` builds the packs here, from
source, for a pack you are changing, and until a release carries them: it
needs the Rust WASM toolchain, which `../tangent/examples/enrich/setup.sh`
(without `data`) installs.

The judge is any OpenAI-compatible model: `JUDGE_URL` (a local model on
`:8080` by default), `JUDGE_MODEL` (`glm-4.7-flash`), `JUDGE_API_KEY`. Each
pack's README has its source's block and credentials, and [core](core) the
profile's keys and the judge's settings.

A `tcp` source can feed several packs: route it to each, and each takes only
its own `source` values. A record whose `source` no included pack takes goes
no further, and `tangent run` says so. To keep records in your own shape,
send them as `source: app`, or list their name under
`plugins.app.config.extra_sources`: baseline learns them as it does `app`'s,
and the judge's logfmt wants a row for the name in your config, under
`inference.judge.encoding.sources`, as [core's judge.yaml](core/judge.yaml)
has for `app`, for its times and the fields on every line.

[examples/cloud-infra](../examples/cloud-infra) is every pack for a fictional
company, whose cases in [tangent-eval's cases](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases)
it is measured against; [examples/app-log](../examples/app-log) is the app
pack, one org per customer. What a pack costs to run is in
[docs/cost.md](../docs/cost.md).

## Testing

Each pack's `tests/` holds its fixtures and the tables they were written
against: `profile.yaml`, the company; `addresses.yaml`, GeoLite2's answers for
their addresses; `networks.csv`, those addresses' hosting, Tor and VPN labels.
Its `ingest.yaml` names them for the test, so the fixtures mean the same in
any config and on any day, with no data fetched.

```bash
make -C packs test                # each pack alone: compile, then its fixtures; core's baseline; and the libraries' tests
make -C packs fixtures-okta       # okta/tests/expected.json from the plugin's output; read the diff
                                  # (a test that fails for any reason but its output leaves it as it was)
make -C packs fixtures-baseline   # core/baseline/tests/expected.json, the same way
make -C packs dist                # every plugin as a component in target/packs, what a release ships (dist.sh)
```

A release is to carry each plugin compiled, with its build's attestation, and
`tangent plugin fetch` takes them from there: a user needs no toolchain
([prebuilt plugins](../docs/prebuilt-plugins.md)).

## Writing a pack

1. Copy [okta](okta), the smallest, to a directory beside it under `packs/`,
   and rename its package and plugin; a name may not contain `-`. Its
   `Cargo.toml` finds `normkit` and `factskit` at `../shared/`; a copy kept
   elsewhere points them at `<checkout>/packs/shared/...`. Its `probe()` takes
   your `source` values and nothing else: a value belongs to one pack.
   `process_logs` fills a `normkit::Norm`, trims the record, and hands both to
   `factskit`, which enriches the address, adds the profile's facts and writes
   the record ([core](core/README.md#writing-a-packs-plugin) has the
   contract).
2. `ingest.yaml`: the plugin, named after the pack, with its fixtures under
   `tests:` and the tables they were written against (`tests/profile.yaml`,
   and `tests/addresses.yaml` and `tests/networks.csv` for the addresses). No
   `dag`: the config that includes the pack routes it. Baseline learns a
   `source` only once [its table](core/baseline/kit/src/kinds/mod.rs) knows
   it.
3. A row for each of its sources in [core's judge.yaml](core/judge.yaml),
   under `inference.judge.encoding.sources.<source>`: the fields naming the
   principal and the time, and those the model should see on every line. Your
   pack's own `_facts` keys go in its sources' `keep` too: core's top-level
   `keep` is every pack's.
4. `tests/input.json` from synthetic records, two or more, with the tables
   they read: the company they describe, and what you want their addresses to
   be (`203.0.113.9` a Tor exit, say). `make -C packs fixtures-<name>` writes
   `tests/expected.json` from the plugin's output; read it before committing.
   `make -C packs test` runs every pack's fixtures on its own.
5. So the release ships it prebuilt: `prebuilt:` on the plugin in
   `ingest.yaml`, as the other packs have it, and the plugin in
   [dist.sh](dist.sh)'s list and [dist-workspace.toml](../dist-workspace.toml)'s
   `extra-artifacts`. `packs/dist.sh check` (CI runs it) says when the list and
   the configs disagree; `make -C packs dist` builds what the release would.
