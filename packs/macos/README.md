# macos (planned, not built)

Developer laptops: where nx/s1ngularity, the axios RAT, Shai-Hulud's
postinstalls and the Claude Code base-URL exfiltration actually ran. Not
built yet; this is the plan, so the shape is on record.

## Telemetry

`eslogger`, Apple's built-in Endpoint Security command-line tool, emits
process executions and file opens as JSON with nothing to install:

```bash
sudo eslogger exec open | tangent run --config ingest.yaml
```

That is enough for the paths that matter:

| path | what the events show |
|---|---|
| a package lifecycle script (nx, Shai-Hulud, axios) | `npm`/`bun`/`pnpm` spawning `node` or `bun`, which then opens `~/.npmrc`, `~/.aws/credentials`, `~/.ssh/*`, `~/.kube/config`, the keychain |
| s1ngularity's AI-CLI inventory | `claude`, `gemini` or `q` executed by a package script, not by a terminal |
| Claude Code CVE-2026-21852 | the assistant reading `.claude/settings.json` from a freshly cloned repo before its trust prompt |
| GlassWorm and Open VSX | a new directory under `~/.cursor/extensions` or `~/.vscode/extensions` from a publisher not seen in the profile |
| a dropped binary | something executed from `/tmp`, `~/.cache` or a package's `node_modules` that then opens credential files |

## The pack

One plugin, `macos`, like every pack's: it puts `eslogger` records into
`_norm` with the process tree as the actor (`npm → node → curl`), the file
or host as the target, and the laptop's user as the org; enriches any
address the process connects to from the runtime's tables; and adds the
profile's facts, with a profile of the laptop: which tools the developer
uses, which publishers' extensions, which paths hold credentials. Every
record is kept in `logs.sqlite` by [core](../core), and the judge reads a
process tree's records forty at a time, as it reads an actor's elsewhere.

## What it needs

A corpus first, built the way the examples' cases were: sequences of
`eslogger` events for each of the paths above, with look-alikes (a developer
running `aws configure`, an editor indexing a repo, a legitimate `npm ci`).
Then the plugin, the profile, the fixtures. Egress is the one thing
`eslogger` does not report; `nettop` or a packet filter log can fill it in
later.
