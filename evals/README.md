# evals

The cases tangent's security packs are measured against, the year of
ordinary traffic they are replayed inside, and the harness that replays them
through [examples/cloud-infra](/examples/cloud-infra) and scores the verdicts.
Generating the corpus and checking the harness need no model; only the judged
replay calls one.

Everything is synthetic except the Stratus Red Team records, which are
Datadog's recordings of real detonations in their own test accounts,
anonymized by them ([stratus/NOTICE](stratus/NOTICE)). The company in every
case, acme, is fictional; so are its people, accounts and repos.

## The corpus

| suite | should flag | must not flag | where |
|---|---|---|---|
| our cases | 45 | 16 | [cases/](cases/), written by `gen.py` |
| Stratus Red Team | 70 | 70 | [stratus/cases.json](stratus/), written by `stratus/splice.py` |
| all | 115 | 86 | 201 cases |

- **Our cases** ([cases/README.md](cases/README.md) lists every one): 61
  event sequences in the shapes of CloudTrail, the GitHub audit log, Okta,
  Google Workspace, GitHub Actions job lines, runner egress, a product's audit
  log and an AI agent's log. 15 of the 45 attacks follow compromises disclosed
  in 2025 and 2026 (TeamPCP, the axios hijack, Shai-Hulud, tj-actions,
  Railway, Vercel via Context.ai, Salesloft Drift, postmark-mcp and others).
  10 are five attacks and their twins, the same person, hour and actions from
  where they always are, which only history tells apart. The 16 must-not-flag
  cases are what looks alarming and is not: Dependabot, the on-call's 3 a.m.
  login, a deploy's restart burst, scanner probes, a rotation after a vendor's
  notice, and the five twins.
- **Stratus Red Team** ([stratus/README.md](stratus/README.md)): the 35 AWS
  techniques Datadog recorded, each spliced in twice as a member whose
  credentials were taken, once from a hosting address (`away`) and once from
  the office (`usual`); and for each, a control with every call made a
  harmless read.

Two things limit what these numbers can say:

- **Our cases are in-sample.** The judge's brief (packs/core/judge.md) and the
  profile were revised while running them, and the history pairs were written
  to measure the judge's `_baseline`. A score on them says the pipeline still
  does what it was built to do, not how it does on attacks nobody here wrote.
- **Stratus numbers are not citable yet.** An audit on 2026-10-28 found that
  the splice can be told from acme's own traffic by things unrelated to the
  attack (every control is on another member than its attack, controls carry
  no request parameters, every spliced case starts at 13:00Z, and more). In
  every run since, the controls were flagged about as often as the attacks.
  [stratus/README.md](stratus/README.md#not-citable-yet) lists the tells.

## The stream

`stream.py v3` puts every case on a day of its own (Tuesday to Saturday;
two cases that share an actor at least ten days apart) inside thirteen months
of acme's ordinary traffic from every source: sign-ins, the product's API
traffic, pushes and pull requests, CI builds, each member's own AWS work, the
agent's health checks, poller and plans. 197,367 records:

| source | records |
|---|---|
| `agent` | 129,642 |
| `cloudtrail` | 20,719 |
| `app` | 19,002 |
| `okta` | 12,725 |
| `github_audit` | 8,725 |
| `gha_ci` | 3,121 |
| `runner_egress` | 2,588 |
| `google_workspace` | 845 |

`truth-v3.json` says, per case, whether it should flag, its actor and org as
the packs normalize them, the times of its events and the stream lines that
are its records. `past.py` moves both 104 weeks back, because the sqlite sink
takes no time from the future. `windows.py` makes the judged windows: an hour
either side of every case event, merged, 211 windows and 434 hours. Records
outside a window are ingested as history and never judged.

## Generate (no model)

Python 3.10 or later, with PyYAML importable under `python3 -I` (installed
for the interpreter or in a venv, not with `pip --user`). From the repository
root:

```bash
python3 -I evals/gen.py                      # rewrites evals/cases; git status stays clean
python3 -I evals/stream.py v3                # evals/runs/stream/{stream,truth}-v3.*
python3 -I evals/harness/past.py v3          # evals/runs/stream/{stream,truth}-past-v3.*
python3 -I evals/harness/windows.py evals/runs/stream/truth-past-v3.json evals/runs/windows-v3.json
python3 -I evals/harness/isolation.py --v3   # cases with another case among an actor's latest 40 records
```

Everything generated goes under `evals/runs/`, which git ignores. `stream.py`
with no argument writes the older 191-case stream, without the history cases.

## Build tangent and the plugins

```bash
cargo build --release -p tangent-cli
export TANGENT=$PWD/target/release/tangent
make -C examples/cloud-infra compile TANGENT=$TANGENT
```

`make compile` runs `setup.sh` first: the WASM toolchain, and the enrichment
data (GeoLite2, the cloud providers' ranges, the Tor exit list) into
examples/enrich/data. That data is fetched when you run it and changes over
time, so note the date with a result. A compiled plugin (`.cwasm`) only loads
in the tangent build that compiled it: compile again after every rebuild.
`replay.py` refuses to start without `TANGENT`.

## Make a run directory

```bash
python3 -I evals/harness/maincfg.py isolated evals/runs/h1             # Haiku 5.5 on Bedrock
python3 -I evals/harness/maincfg.py isolated evals/runs/l1 --judge local
```

`maincfg.py` merges examples/cloud-infra's `ingest.yaml` and `judge.yaml`
with their includes and changes only what a replay needs: one tcp source for
the stream, paths into the run directory, the eval profile
([profile/profile.yaml](profile/profile.yaml)), and the judge's model.

- `isolated` folds the ordinary stream plus only the current window's cases
  into the state the judge's `_baseline` is read from, so no case sees
  another's history. Use it for numbers about the corpus.
- `shipped` is the example exactly as it runs, with every record, every
  case's included, folded into one state; an earlier case's calls and
  addresses are then "seen" for a later one. Report it beside `isolated`.
- `--judge bedrock` (the default) is Claude Haiku 5.5: thinking off,
  `max_tokens` 400, no temperature or top_p (5.5 refuses them), 16 requests
  at once. `BEDROCK_MODEL` and `BEDROCK_REGION` (us-west-2) override it, and
  `AWS_PROFILE` names the profile it signs in with.
- `--judge local` is packs/core's judge as it ships: any OpenAI-compatible
  server at `JUDGE_URL` (http://localhost:8080/v1), `JUDGE_MODEL`
  (glm-4.7-flash), one request at a time.

Use a new run directory for every run: the shards' sinks append.

## Check it for free

In a run directory of its own, so no judged run starts with its files:

```bash
python3 -I evals/harness/maincfg.py isolated evals/runs/sim
TRUTH=evals/runs/stream/truth-past-v3.json SIMULATE=1 \
  python3 -I evals/harness/replay.py evals/runs/sim evals/runs/stream/stream-past-v3.ndjson evals/runs/windows-v3.json
```

`SIMULATE=1` runs ingest for real and forms the judge's requests without
sending them: `sim.json` in the run directory has the groups, requests,
records, context records and bytes a judged run will send. Each window's line
on stderr says how many rows of finished cases left `logs.sqlite`; that they
are found at all says truth's keys match what the packs wrote. Look in
`ingest.log` for records no pack took, and on stderr for "records not
stored". The judged runs below get directories of their own (`h1`,
`h2`).

`JUDGE=judge-sim.yaml` with `TRUTH` and without `SIMULATE`, again in a
directory of its own, stamps every judged record with its `_baseline` and
writes it to `headers.ndjson`, still with no model; `harness/headers.py TRUTH
WORK` prints what the judge would read for each attack and its twin.

## Run it

Bedrock, ten shards:

```bash
export TANGENT=... TRUTH=evals/runs/stream/truth-past-v3.json
export AWS_PROFILE=...                 # one that can call Bedrock in BEDROCK_REGION; maincfg.py writes it into judge.yaml
export AWS_WAIT_PROFILE=$AWS_PROFILE   # on errors, wait for this profile's credentials, put the records back, go on
python3 -I evals/harness/maincfg.py isolated evals/runs/h1
python3 -I evals/harness/shard.py evals/runs/h1 evals/runs/stream/stream-past-v3.ndjson evals/runs/windows-v3.json 10 \
  > evals/runs/h1/shard.out 2>&1
```

Shard k replays the windows before its own as history, so the result is the
same as one replay, about N times sooner. Each shard's progress is in
`s<k>/run.log`. A run stops on "judge stalled" (nothing judged for `STALL`
seconds, 900), or after three credential recoveries in ten minutes; a single
`replay.py` resumes with `FROM_WINDOW=k`. When judge.yaml names a profile,
replay.py drops `AWS_BEARER_TOKEN_BEDROCK` from the judge's environment,
because a token there is used instead of the profile; to sign in with a
Bedrock bearer token, leave `AWS_PROFILE` unset when you run maincfg.py, and
the token stays.

On the earlier plugin set a run took about 11 minutes at ten shards and cost
about $1 on Haiku 5.5 (921 requests, about 7 million tokens in). This layout
has not been run yet. Haiku 5.5 takes no sampling settings and two runs agree
on 98 to 99% of records, so make two runs of each arm and compare them with
`agree.py`.

A local model: serve it with the `llama-server` line in
[packs/core/README.md](/packs/core/README.md), one model server at a time,
make the run directory with `--judge local`, and run `shard.py` the same way.
The server is the bottleneck: at about five seconds a request on one
machine, a run takes well over an hour. It has not been measured on this
shape.

## Score it

```bash
python3 -I evals/harness/score_v2.py evals/runs/stream/truth-past-v3.json evals/runs/h1
python3 -I evals/harness/agree.py evals/runs/stream/truth-past-v3.json evals/runs/h1 evals/runs/h2
```

What `score_v2.py` reports:

- **recall**: an attack is caught when the judge flags any record of the
  case's actor (or of its org, for a case with no actor; agent records have
  only an org) in the two hours ending at the case's last event. The stricter
  count needs the flag on one of the case's own records.
- **negatives flagged**: the same rule over the 86 must-not-flag cases.
- **ordinary**: judged records that are no case's, flagged over judged. They
  are acme's synthetic traffic inside the windows, which sit around business
  hours, so this is a flag rate, not alerts a day for a real company.
- **not judged**: records whose verdict is an error (a failed request, or a
  record the model's answer skipped). Counted, not retried.
- **by suite**, each **attack against its twin**, **Stratus by what decides
  it** (stratus/grounding.yaml: one event, the actor's habits, or volume), and
  the **requests**, records read (context included) and megabytes the model
  read (measured as JSON; logfmt sends fewer).

`agree.py` matches two runs record by record (a record's content without its
verdict) and says how often the verdict agrees and which attacks only one run
caught.

`score_union.py` scores the judge and an ingest-side tripwire as one: recall
for each and for either, flags on ordinary records, and incidents (one org
and actor's flags with no gap over an hour) per 24 judged hours. It needs a
tripwire, and none is in this repository yet: it scores a run directory made
with `maincfg.py ... --tripwire NAME=PATH` (a deterministic ingest plugin
that passes on only the records it trips on, each with a `tripwire` key) and
compiled with `$TANGENT plugin compile --config WORK/ingest.yaml`, which
`make compile` does not do. After `shard.py`, join the shards' output first:

```bash
cat evals/runs/t1/s*/tripwire.ndjson > evals/runs/t1/tripwire.ndjson
python3 -I evals/harness/score_union.py evals/runs/stream/truth-past-v3.json evals/runs/t1 evals/runs/windows-v3.json [TRIPWIRE.ndjson]
```

[RESULTS.md](RESULTS.md) says which results exist.

## Layout

```
evals/
  cases/             61 cases, one directory each (case.yaml, events.ndjson); README.md lists them
  gen.py             writes cases/; stream.py imports its case list
  stream.py          writes runs/stream/: the stream and its truth file
  stratus/           Stratus Red Team v2.37.0: fetch.py, techniques.json, logs/, splice.py, cases.json, NOTICE, LICENSE
  profile/           the eval's profile of acme
  harness/           maincfg.py, replay.py, shard.py, past.py, windows.py, and the scorers
  runs/              everything generated (ignored by git)
  RESULTS.md
```
