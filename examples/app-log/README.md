# app-log

Your product's logs, judged per customer. The same pipeline as
[cloud-infra](../cloud-infra), with a different org: an application's audit
log and an AI agent's logs carry a customer's tenant id, and that is the org.
Each customer's actors are kept and judged apart, forty records at a time.

```
app audit log, agent logs ──▶ app ──▶ logs.sqlite ──▶ actors ──▶ history ──▶ judge ──▶ alerts.ndjson
                               │                                    ▲
                               └──▶ history ──▶ state.sqlite ───────┘
                        (org = the customer's tenant id)
```

It is built only from [packs](/packs): `ingest.yaml` includes
[core](/packs/core) and the [app](/packs/app) pack and adds the NDJSON source
on `:9401` and the profile; `judge.yaml` includes core's judge, which reads
each actor's history from ingest's `state.sqlite`, and app's logfmt row. The
17 cases in [cases/](cases/) are the ones that live in product logs: a key
paging through a whole dataset, an integration's token used from a host it
never used, a role change followed by an export, an agent's plan reading a
secret and posting to a host its steps never named, a malicious MCP server
copying every message out, an agent's model calls redirected by a cloned
repo's settings, and the look-alikes: the deploy and its health checks, the
scanner at the public endpoint, the vendor's 429s, the top-of-hour fan-out.

## Status

Runs today with one profile for every customer, which is wrong in principle
and fine for a first customer. What it still needs:

- **Per-customer profiles.** The profile's facts (members, keys and their
  networks, hosts, sensitive assets) are the customer's, not yours. They
  need one profile per org, written at onboarding and picked by the app
  pack from the record's `_norm.org`.
- **Direct sources** for the product's log stores, as cloudtrail, okta and
  the others have.

```bash
make setup && make test && make run
```
