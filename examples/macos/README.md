# macos

A Mac where AI coding agents run, read by
[packs/macos](/packs/macos) and judged by [packs/core](/packs/core), as
[cloud-infra](../cloud-infra) is for a company's cloud: each record goes to
the lake and into its program's baseline in `state.sqlite`, and waits in its
program's group for the judge, which reads the group with the program's
latest records before it and writes what it flags to `alerts.ndjson`. One
config, one process.

```bash
tangent plugin compile --config tangent.yaml
tangent run --config haiku.yaml &              # Claude Haiku on Bedrock, AWS_PROFILE set; or tangent.yaml, a local model
sudo eslogger exec fork exit open close create rename unlink btm_launch_item_add tcc_modify | nc -U es.sock
tail -f alerts.ndjson
```

`eslogger` needs root, and the terminal it runs in needs Full Disk Access
(System Settings, Privacy & Security), restarted after the grant.

- `tangent.yaml`: core's ingest and judge, the macos pack, the socket `nc`
  writes, and packs/macos's brief, `judge.md`, in place of core's; a local
  OpenAI-compatible model by default (`JUDGE_URL`, `JUDGE_MODEL`,
  packs/core/README.md).
- `haiku.yaml`: the same on Claude Haiku 5.5 through Bedrock,
  `us.anthropic.claude-haiku-5-5` (the cross-region profile; Converse refuses
  the bare model id on demand), thinking off. It includes `hosted.yaml`,
  which takes out what only a local server reads (its url, key, sampling and
  chat template): Haiku 5.5 refuses sampling settings.

The pack runs the pipeline on one worker, which keeps a process's events in
order, so the model gets one request at a time, a program's group of up to
thirty records.

What is kept stays here: `lake/` every record as Parquet (delete old hours to
trim it), `state.sqlite` each program's history and latest ten records. The
records hold command lines.
