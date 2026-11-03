# Results

No result has been measured on this layout yet. Every number below came from
the same corpus run through an earlier harness, outside this repository, and
none of them is this repository's until the runs are repeated with
`harness/` and the packs as they are here. What differs:

- **The packs.** The earlier runs used one cloud-infra pack (normalizer,
  enrichment and facts plugins, and a history plugin stamping `_hist`). Here
  each provider has its own pack, enrichment comes from the runtime's tables,
  norm drops fields that never decide a verdict, the profile's `people` sets
  the person, and core's baseline_builder and baseline_renderer give the
  judge `_baseline`. Each of these changes what the model reads.
- **The stream.** The agent's log was a source of another name; here it is
  `agent`, with renamed services. Every CloudTrail record gen.py writes now
  carries `recipientAccountId`, as the cases have since #48; the Stratus
  session tokens are redacted (the aws pack dropped them before the model
  either way). Because `agent/*` sorts first, 48 of the 201 cases land on
  other days.

## What exists

**v3 corpus, 201 cases** (the corpus here). Claude Haiku 5.5 on Bedrock,
logfmt, ten shards, four complete runs on 2026-10-31 and 11-01, two without
a history header and two with one (the `_hist` that `_baseline` replaced):

| | without the header | with it |
|---|---|---|
| our 45 attacks caught | 45/45, 45/45 | 45/45, 45/45 |
| ordinary records flagged, of 6,587 judged | 124 and 133 (1.9%, 2.0%) | 51 and 59 (0.8%, 0.9%) |
| cost per run | about $0.98 | about $1.02 |

Our 16 must-not-flag cases: 10 or 11 flagged in every run. Each run sent
921 requests. Run-to-run agreement per record: 98.2% without
the header, 99.3% with it. Two further runs were discarded because a shard
exited early.

**v2 corpus, 191 cases** (without the ten history cases), Haiku 5.5,
2026-10-30 and 10-31: our 40 attacks caught 40/40 in each of the four runs
re-scored for it.

**examples/cloud-infra's README** cites the shape before the actor-of-forty
judge (Haiku 4.5, every record judged in batches with each org's history):
38 to 39 of 40 attacks, 1 of 11 negatives, 13 of 16,199 ordinary records.

**Stratus Red Team**: every number measured on it is withdrawn
([stratus/README.md](stratus/README.md#not-citable-yet)).

## What the numbers can say

Our cases are the development set: the judge's brief was revised while
running them, so recall on them is in-sample. The history pairs were written
to measure the history header. Nothing here estimates alerts a day for a real
company: only window hours are judged, they sit around business hours, and
acme has nine people.

When this layout is measured, record here: the date, the tangent commit, the
enrichment data's date, the arm (`isolated` or `shipped`), the model, and
`score_v2.py`'s and `agree.py`'s output for two runs.
