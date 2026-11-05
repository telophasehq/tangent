# macos

A Mac where AI coding agents run, read by
[packs/macos](/packs/macos) and judged by [packs/core](/packs/core), as
[cloud-infra](../cloud-infra) is for a company's cloud: ingest writes each
record to `logs.sqlite`, the judge's queue, to the lake, and into each
program's baseline in `state.sqlite`; the judge reads a program's records back
together, forty at a time with the ones before them, and writes what it
flags to `alerts.ndjson`.

```bash
tangent plugin compile --config ingest.yaml && tangent plugin compile --config judge.yaml
tangent run --config ingest.yaml &
sudo eslogger exec fork exit open close create rename unlink btm_launch_item_add tcc_modify | nc -U es.sock
tangent run --config judge-haiku.yaml          # Claude Haiku on Bedrock, AWS_PROFILE set; or judge.yaml, a local model
tail -f alerts.ndjson
```

`eslogger` needs root, and the terminal it runs in needs Full Disk Access
(System Settings, Privacy & Security), restarted after the grant. Start
ingest before the judge: the judge opens ingest's `state.sqlite` read-only and
refuses one that is not there yet.

- `ingest.yaml`: core's ingest and the macos pack, and the socket `nc` writes.
- `judge.yaml`: core's judge with packs/macos's brief, `judge.md`, in place of
  core's; a local OpenAI-compatible model by default (`JUDGE_URL`,
  `JUDGE_MODEL`, packs/core/README.md).
- `judge-haiku.yaml`: the same on Claude Haiku 5.5 through Bedrock,
  `us.anthropic.claude-haiku-5-5` (the cross-region profile; Converse refuses
  the bare model id on demand), thinking off. It includes `hosted.yaml`,
  which takes out what only a local server reads (its url, key, sampling and
  chat template): Haiku 5.5 refuses sampling settings.

What is kept stays here: `logs.sqlite` holds what is not judged yet and each
program's latest 39, `lake/` every record as Parquet (delete old hours to trim
it), `state.sqlite` each program's history. The records hold command lines.
