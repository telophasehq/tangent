# githubwebhooks

Flags an npm package published from a GitHub Actions run, read from the run's
own logs.

The `github_webhook` source takes GitHub's webhook deliveries on port 9000 at
`/webhook`. When a `workflow_run` delivery says `completed`, it downloads that
run's logs from the GitHub API with `GITHUB_TOKEN` and sends on the delivery,
then each line of the logs as a record, the line's timestamp moved to
`key.timestamp`:

```json
{"kind":"github_ci_log","github":{"run_id":1001,"repo":"acme/widgets","sha":"3f2a9c41d07e5b86a1c2f0e9d8b7a6c5e4f3a2b1","log_file":"build/7_Publish.txt"},"message":"+ acme-widgets@1.4.2","key":{"timestamp":"2026-11-05T20:22:09.1234567Z"}}
```

Any other delivery goes on as it came, one record per line of its body; a
`ping` is answered and dropped.

The Go plugin ([main.go](main.go)) follows a commit's `npm publish` step
through the state store, by `github.sha` and `github.log_file`:
`##[group]Run npm publish` marks that the commit began a publish in that log;
`npm notice name:` and `npm notice shasum:` keep the package and its tarball's
hash; and a `+ package@version` line in the same log is the publish going
through, `{"triggered": true}`. A `+` line on a commit that ran no publish step
stays `false`. Each line the probe matches gets one output record, and the
pipeline appends them to `alerts.ndjson`.

The config runs one worker (`runtime.workers: 1`): records are dealt out to the
workers one at a time, so with several a run's `+` line can be handled before
its publish step, and the publish is missed.

## Setup

Needs Go 1.26 or later, TinyGo and wasm-tools.

```bash
./setup.sh
```

It installs TinyGo and wasm-tools if they are missing; installing TinyGo also
adds `PATH` and `TINYGOROOT` lines to your shell profiles. Open a new shell
after it: a `TINYGOROOT` left from another TinyGo version fails the build with
`open .../tinygo/<version>/src/...: no such file or directory`.

## Test
```bash
make test
```

That is `./setup.sh`, then:

```bash
tangent plugin compile --config tangent.yaml
tangent plugin test --config tangent.yaml
```

[tests/input.json](tests/input.json) is one commit's publish step as the source
writes its lines, then a `+` line on a commit that ran no publish step;
[tests/expected.json](tests/expected.json) flags only the first `+` line.

## Simulate a webhook delivery
```bash
make run          # compiles, then: tangent run --config tangent.yaml
```

From a second terminal, deliver the test's log lines. A delivery that is not a
`workflow_run` goes on line by line, so these reach the plugin as a completed
run's logs would, with no token and no GitHub. `ok` is the source taking the
delivery; the plugin's output lands in `alerts.ndjson` a moment later:

```bash
jq -c '.[]' tests/input.json | curl -s -w '\n' --data-binary @- localhost:9000/webhook
sleep 1; cat alerts.ndjson
```

```
ok
{"triggered":false}
{"triggered":false}
{"triggered":false}
{"triggered":true}
{"triggered":false}
```

What the plugin keeps about each commit is in `state.sqlite`, for 15 minutes.
Read it while the pipeline runs:

```bash
tangent state --config tangent.yaml scan --prefix npm-publish-
```

```
{"ns":"plugins","key":"npm-publish-started-3f2a9c41d07e5b86a1c2f0e9d8b7a6c5e4f3a2b1-logname","value":{"kind":"str","value":"build/7_Publish.txt"},...}
{"ns":"plugins","key":"npm-publish-started-3f2a9c41d07e5b86a1c2f0e9d8b7a6c5e4f3a2b1-npmSHA","value":{"kind":"str","value":"9c1e7b2a4d6f8e0c3b5a7d9f1e2c4b6a8d0f2e4c"},...}
{"ns":"plugins","key":"npm-publish-started-3f2a9c41d07e5b86a1c2f0e9d8b7a6c5e4f3a2b1-pkgname","value":{"kind":"str","value":"acme-widgets"},...}
{"ns":"plugins","key":"npm-publish-success-acme-widgets@1.4.2","value":{"kind":"bool","value":true},...}
```

Delete `state.sqlite*` and `alerts.ndjson` to start over.

## From GitHub

GitHub has to reach port 9000: run tangent on a host with a public address, or
behind a tunnel to it. The token reads the runs' logs: a fine-grained token
with read access to the repository's Actions, or a classic token with `repo`.

```bash
GITHUB_TOKEN=<token> make run
```

In the repository's Settings, Webhooks, Add webhook: the payload URL is
`http://<your host>:9000/webhook` (tangent serves plain HTTP), or the tunnel's
`https://` address with `/webhook`; the content type `application/json` (the source
reads the body as JSON), and under "Let me select individual events", Workflow
runs. When a run completes, tangent logs `fetching github actions logs` and
the plugin reads every line. The plugin's probe takes only log lines, so the
deliveries themselves (`requested`, `in_progress`, `completed`) are dropped and
counted in `tangent_unrouted_records_total`.

Give the webhook a secret and set the same one as the source's `secret` in
[tangent.yaml](tangent.yaml), and a delivery without GitHub's signature is
refused.
