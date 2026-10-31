# app

Your product's own logs into finished records, by one plugin, `app`. They
come as NDJSON (a `tcp` source), one JSON line per record with `ts` and a
`source` naming its shape:

- **the audit shape**, `source: app`: your product's audit log, a line like
  `{"ts", "source": "app", "event", "actor", "org", "ip", "target" or "key_id"}`,
  where `org` is the customer's tenant;
- **the agent shape**, `source: agent`: an AI agent's structured run log, a
  line like `{"ts", "source": "agent", "event", "organization_id", "task_id",
  "plan_id", "step_id", "client_ip", "destination", "path", "tool", "api"}`.

Other names take either shape through `config:`:

```yaml
plugins:
  app:
    config:
      sources: [app]              # the audit shape; replaces the default
      extra_sources: [billing]    # the audit shape too, beside `sources`
      agent_sources: [agent]      # the agent shape; replaces the default
```

## What it writes

`_norm` ([packs/core](../core#the-record) has the contract):

| field | audit shape | agent shape |
|---|---|---|
| `actor` | `actor` | none: a plan id is new every time, so a run is judged per organization |
| `org` | `org`, the customer | `organization_id`, the customer |
| `kind` | `event` | `event` |
| `ip` | `ip` | `client_ip` |
| `dest` | | the host of `destination` |
| `target` | `target`, else `key_id` | `path` for `secret_read`, `tool` for `tool_result`, `api` for `aws_api_call` |

`_facts` is what the profile and the tables say; an agent's run has no
principal.

## What it drops

Records stay as sent, less their per-event ids (`request_id`, `trace_id`,
`span_id` and their Datadog and camel-case forms) and the signatures in their
URLs (`sig`, `X-Amz-Signature`, `X-Goog-Signature`, `token`, ...). The agent
shape also loses Datadog's bookkeeping: `dd.version`, `ddsource`, and
`dd.service` and `dd.env` when they repeat `service` and `env`.

## What it enriches

`ip`, or `client_ip`, as `_norm.ip`: its country, network and owner, hosting,
Tor and VPN, and whether it is one of yours (`_facts.ip`). An IPv4 address as
a dual-stack server logs it, `::ffff:192.0.2.1`, with brackets and a port or
without, is the IPv4 address. Next, through
`Norm::address`: a forwarded-for chain, when your log keeps one, and an
agent's `destination` when it is an address rather than a host.

## Use it

In your ingest config, beside [core](../core):

```yaml
include:
  - ../tangent/packs/core/ingest.yaml
  - ../tangent/packs/app/ingest.yaml

enrichment_tables:                  # your company: packs/core/README.md#the-profile
  profile: { type: document, path: profile/profile.yaml, required: true }

sources:
  events:
    type: tcp
    bind_address: 0.0.0.0:9401

dag:
  - from: { kind: source, name: events }
    to: [{ kind: plugin, name: app }]
```

`org` is your customer, not you: leave these sources out of the profile's
`own_sources`, and each customer's actors are kept and judged apart. In your
judge config, `../tangent/packs/app/judge.yaml`.

One profile serves every customer for now, which is wrong in principle and
fine for a first customer: members, keys and their networks, hosts and
sensitive assets are the customer's, not yours. Per-customer profiles, picked
by `_norm.org`, are still to do.

Kinds worth the profile's `watch`: `member.role.changed`,
`org.settings.changed`, `api_key.create`, `org.export.started`; for the agent,
`aws_api_call`, `step_failed`.

## Test it

[tests/input.json](tests/input.json): audit and agent records from the cases, one carrying the
keys only the pipeline writes, and four at the end from the office network
(the profile's `networks`) and in IPv4-mapped form. [tests/expected.json](tests/expected.json) is
what the plugin makes of them against the tables in `tests/` (the company,
GeoLite2's answers for their addresses, their labels), which `ingest.yaml`
names for the test in any config.

```bash
make -C packs test-app
make -C packs fixtures-app        # after a change; read the diff
```

Cases: all 17 in [examples/app-log](/examples/app-log/cases), and in
[examples/cloud-infra](/examples/cloud-infra/cases)
`ai-agent/agent-escapes-task-uses-found-credentials`,
`negative/oncall-3am-login`, `negative/secret-rotation-after-vendor-notice`,
`vendor/ai-app-oauth-token-stolen-sso-pivot` and
`vendor/platform-breach-env-secrets-reused`.
