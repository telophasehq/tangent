# github

GitHub into finished records, by one plugin, `github`. It takes three
`source` values:

- `github_audit`: the organization's audit log, web and git events, as
  tangent's `github_audit` source reads it;
- `gha_ci`: an Actions job's log lines, one JSON line each with `repo`,
  `run_id`, `job`, `step`, `line`, sent as NDJSON (a `tcp` source);
- `runner_egress`: the connections a runner made, one JSON line each with
  `repo` and `dst_host`, sent the same way.

## What it writes

`_norm` ([packs/core](../core#the-record) has the contract):

| field | `github_audit` | `gha_ci` | `runner_egress` |
|---|---|---|---|
| `actor` | `actor` | `repo` | `repo` |
| `org` | `org` | the repo's owner | the repo's owner |
| `kind` | `action` | `step` | `egress` |
| `ip` | `actor_ip` | | |
| `country` | `actor_location.country_code` | | |
| `dest` | | | `dst_host` |
| `target` | `repo` | `repo` | `repo` |

A CI run's story is told per repository: a run id is new every time, and the
repository is not a principal, so CI records get no `_facts.principal`.

## What it drops

- `github_audit` keeps `ts`, `source`, `action`, `actor`, `actor_is_bot`,
  `actor_ip`, `actor_location`, `org`, `repo`, `user`, `team`, `visibility`,
  `public_repo`, `programmatic_access_type`, `token_scopes`, `integration`,
  `operation_type`, `permission`, `old_permission`, `name`, `head_branch`,
  `event`, `conclusion`, `runner_name`, `runner_type`, `pull_request_title`,
  `pull_request_url`, `topic`, `ref`, `key`, `fingerprint`, `read_only`,
  `hook_id`, `config`, `target_login`, `reason` and `explanation`. A hosted
  runner's name (`GitHub Actions 12`, its job's VM) and a workflow event's
  `topic` go too; so do `@timestamp`, `created_at`, `_document_id`,
  `request_id`, `hashed_token`, `user_agent` and the rest.
- `gha_ci` and `runner_egress` stay as sent, less their request and trace ids
  and URL signatures.

## What it enriches

`actor_ip`, as `_norm.ip`: its country, network and owner, hosting, Tor and
VPN, and whether it is one of yours (`_facts.ip`). Next, through
`Norm::address`: a runner's destination address, when its egress log has one
(`dst_ip`, or `dst_host` written as an address), for whose network a job
talked to.

## Use it

In your ingest config, beside [core](../core):

```yaml
include:
  - ../tangent/packs/core/ingest.yaml
  - ../tangent/packs/github/ingest.yaml

enrichment_tables:                  # your company: packs/core/README.md#the-profile
  profile: { type: document, path: profile/profile.yaml, required: true }

sources:
  github_audit:
    type: github_audit
    org: ${GITHUB_ORG}                # the organization's login
    token: ${GITHUB_TOKEN}            # read:audit_log
  events:                             # gha_ci and runner_egress, as NDJSON
    type: tcp
    bind_address: 0.0.0.0:9400

dag:
  - from: { kind: source, name: github_audit }
    to: [{ kind: plugin, name: github }]
  - from: { kind: source, name: events }
    to: [{ kind: plugin, name: github }]
```

and `github_audit`, `gha_ci` and `runner_egress` in your profile's
`own_sources`. In your judge config, `../tangent/packs/github/judge.yaml`.

The audit log API is a GitHub Enterprise Cloud feature; the token needs
`read:audit_log`. The source keeps its place in the runtime's state store, so
a restart resumes where it stopped; on first start it reads the last hour
(`since`). Left without a token, it says so once and stays idle.

Kinds worth the profile's `watch`: `protected_branch.destroy`,
`deploy_key.create`, `org.update_member`, `org.self_hosted_runner_register`,
`repo.rename`, `repo.access`, `pull_request_target`.

## Test it

[tests/input.json](tests/input.json): audit log, CI and egress records from the cases, one carrying the
keys only the pipeline writes. [tests/expected.json](tests/expected.json) is
what the plugin makes of them against the tables in `tests/` (the company,
GeoLite2's answers for their addresses, their labels), which `ingest.yaml`
names for the test in any config.

```bash
make -C packs test-github
make -C packs fixtures-github     # after a change; read the diff
```

Cases in [examples/cloud-infra](/examples/cloud-infra/cases) with GitHub
records: `github/*`, `supply-chain/*`, four of `negative/` (Dependabot, a repo
made public as planned, a scanner action's usual run) and
`vendor/platform-breach-env-secrets-reused`.
