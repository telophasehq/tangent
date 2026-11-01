# packs

One directory per provider, each holding one plugin that turns that provider's
raw records into finished ones: normalized into `_norm`, stripped of what never
decides a verdict, its address enriched, and given what your company profile
says about it in `_facts`. How to use them is in the main
[README](/README.md#packs).

| directory | what it is |
|---|---|
| [core](core) | not a provider: what every pack needs. `ingest.yaml`: the enrichment tables and the `logs` sink; `judge.yaml`: the judge. Its README has the record contract and the profile's keys. Every config includes it. |
| [aws](aws) | `cloudtrail` |
| [github](github) | `github_audit`, `gha_ci`, `runner_egress` |
| [okta](okta) | `okta` |
| [google_workspace](google_workspace) | `google_workspace` |
| [app](app) | your product's NDJSON: `app`, `agent`, and names you list |
| [macos](macos) | planned, not built |
| [shared](shared) | not a pack: `normkit`, `enrichkit` and `factskit`, the Rust libraries every pack's plugin links |

A `source` value belongs to one pack: two packs taking it would store each
record twice.

Each pack's `tests/` holds its fixtures and the tables they were written
against: `profile.yaml`, the company; `addresses.yaml`, GeoLite2's answers for
their addresses; `networks.csv`, those addresses' hosting, Tor and VPN labels.
Its `ingest.yaml` names them for the test, so the fixtures mean the same in
any config and on any day, with no data fetched.

```bash
make test                # each pack alone: compile, then its fixtures; and the libraries' tests
make fixtures-okta       # okta/tests/expected.json from the plugin's output; read the diff
                         # (a test that fails for any reason but its output leaves it as it was)
```
