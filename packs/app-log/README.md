# app-log

Your product's logs, judged per customer. The same pipeline as
[cloud-infra](../cloud-infra), partitioned differently: an application's audit
log and an AI agent's logs carry a customer's tenant id, and that is the
partition. Each customer gets their own history, their own window, their own
judge.

```
app audit log, agent logs ──▶ norm ──▶ enrich ──▶ facts ──▶ window ──▶ judge ──▶ alerts.ndjson
                                 (partition = the customer's tenant id)
```

The plugins are cloud-infra's, referenced by path; nothing is duplicated. The
17 cases in [cases/](cases/) are the ones that live in product logs: a key
paging through a whole dataset, an integration's token used from a host it
never used, a role change followed by an export, an agent's plan reading a
secret and posting to a host its steps never named, a malicious MCP server
copying every message out, an agent's model calls redirected by a cloned
repo's settings, and the look-alikes: the deploy and its health checks, the
scanner at the public endpoint, the vendor's 429s, the top-of-hour fan-out.

## Status

Runs today with one profile for every customer, which is wrong in principle
and fine for a first customer. What this pack still needs:

- **Per-customer profiles.** The profile's facts (members, keys and their
  networks, hosts, sensitive assets) are the customer's, not yours. They
  belong in the customer's partition as state rows written at onboarding and
  read by `facts` with the same `get` it uses now, so a plugin never has to
  know which customer it is serving.
- **Direct sources** for the product's log stores, the same work as
  cloud-infra's.

```bash
make setup && make test && make run
```
