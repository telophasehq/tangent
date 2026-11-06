# app-log

Your product's logs, judged per customer. The same pipeline as
[cloud-infra](../cloud-infra), with a different org: an application's audit
log and an AI agent's logs carry a customer's tenant id, and that is the org.
Each customer's actors are kept and judged apart, a group of records at a time.

```
app audit log, agent logs: NDJSON on :9401
        │
        ▼
       app ──┬──▶ lake/, every record as Parquet
             ├──▶ baseline_builder ──▶ state.sqlite
             │                              │
             └──▶ baseline_renderer ◀───────┘
                  an actor's records, a group at a time
                  (org = the customer's tenant id)
                       │
                       ▼
                     judge ──▶ fresh ──┬──▶ flagged ──▶ alerts.ndjson
                                       └──▶ verdicts.ndjson
```

It is built only from [packs](/packs): `tangent.yaml` includes
[core](/packs/core), its judge, and the [app](/packs/app) pack, and adds the
NDJSON source on `127.0.0.1:9401` (this machine only, since nothing
authenticates a record), the plugin's edges to the lake, the baseline and
the judge, and the profile. One config, one process; the
judge reads each actor's history from the same `state.sqlite`. The
18 cases in [tangent-eval's cases](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases)
written only in product logs, `app` and `agent` lines, are this example's, 13
that should flag and 5 that must not: a key paging through a whole dataset,
an integration's token used from a host it never used, a role change
followed by an export, an agent's plan reading a secret and posting to a
host its steps never named, a malicious MCP server copying every message
out, an agent's model calls redirected by a cloned repo's settings, and the
look-alikes: the deploy and its health checks, the scanner at the public
endpoint, the vendor's 429s, the top-of-hour fan-out. Seven more mix product
logs with CloudTrail, GitHub, Okta or Google Workspace lines, which
[cloud-infra](../cloud-infra) reads whole.

## Install and run

```bash
make setup          # GeoLite2, cloud ranges, Tor exits into examples/enrich/data
make test           # the packs' plugins, then the app pack's fixtures; no model needed
make run            # one process: the source, the app pack and the judge; listens on :9401
```

As in cloud-infra, `make test` and `make run` take the packs' plugins
prebuilt from the tangent release of this tangent's version, checked
against GitHub's attestation of their build (it needs `gh`, not a login).
Until the release after 0.1.9 there are none to fetch, and the packs need a
tangent built from the checkout anyway, so build them here, as for a pack
you are changing: `make test PLUGINS=compile` and `make run PLUGINS=compile`,
which install the Rust WASM toolchain first.

The judge defaults to a local model on `:8080`
([packs/core](/packs/core#judgeyaml) says how to serve it); `JUDGE_URL`,
`JUDGE_MODEL` and `JUDGE_API_KEY` point it at any OpenAI-compatible one (the
Makefile has a hosted example). For a hosted one, include
`../../packs/core/judge-hosted.yaml` in `tangent.yaml` in place of
`judge.yaml`, which leaves out the local model's sampling settings that a
hosted model may refuse.

## Try it

With `make run` going, send it one of the cases from another terminal: an
API key paging through every finding in four minutes, its first 30 records.

```bash
curl -fsSL https://raw.githubusercontent.com/EthanBlackburn/tangent-eval/main/cases/data-exfil/bulk-pagination-export/events.ndjson | head -n 30 | nc -w 1 localhost 9401
```

Thirty records close the key's group, so they reach the judge together as
soon as they arrive, in one request, and a verdict per record lands in
`verdicts.ndjson`, the flagged ones in `alerts.ndjson` too, each flag with
the model's `why`:

```bash
jq -c '{path, judge}' verdicts.ndjson | head -3
```

GLM-4.7-Flash, the default, served locally, answered in 6 to 30 seconds and
flagged the 30 in only 2 of 11 runs; it passed them in the other nine, and
in all five runs at temperature 0. The case should flag: the default model
has not been measured on the cases.

A group closes at 30 records, an hour after its first, or after an hour's
quiet, so fewer records wait up to an hour for a verdict. Stopping the
process (Ctrl-C) closes the open groups but does not wait for the model:
what it has not answered then gets no verdict, though it is in `lake/` and
the baseline all the same. Send all 40 and the 10 left over wait; and since
a group holds records in the order the workers finish them, the 30 judged
first are any 30 of the 40, not the first.

## Status

Runs today with one profile for every customer
([profile/profile.yaml](profile/profile.yaml);
[packs/core](/packs/core#the-profile) lists the keys), which is wrong in
principle and fine for a first customer. What it still needs:

- **Per-customer profiles.** The profile's facts (members, keys and their
  networks, hosts, sensitive assets) are the customer's, not yours. They
  need one profile per org, written at onboarding and picked by the app
  pack from the record's `_norm.org`.
- **Direct sources** for the product's log stores, as cloudtrail, okta and
  the others have.
