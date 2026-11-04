# macos

One developer's Mac, where AI coding agents run: the machine the [Endpoint AI
Agent Abuse catalog](https://github.com/0x4D31/endpoint-ai-agent-abuse)
describes, where a package script plants a hook in the agent's settings,
poisons its instructions, adds an MCP server, reads its transcripts, or starts
it with its API traffic pointed elsewhere. The telemetry is `eslogger`,
Apple's Endpoint Security command-line tool, already on every Mac. The pack
is one plugin and a judge: nothing is stored, a model reads each event the
plugin lets through, and a file holds what it flagged.

```bash
cd packs/macos
tangent plugin compile --config judge.yaml      # or fetch: the release's components, attested
tangent run --config judge.yaml &               # ingest.yaml included: socket, plugin, judge, flagged.ndjson
sudo eslogger exec fork exit open close create rename unlink btm_launch_item_add tcc_modify | nc -U es.sock
tail -f flagged.ndjson
```

`eslogger` needs root, and the terminal it runs in needs Full Disk Access
(System Settings → Privacy & Security), restarted after the grant. It writes
to stdout only, and tangent reads sockets, so `nc -U` joins them: root writes
the socket, tangent owns it, and nothing listens on a port. The judge is any
OpenAI-compatible server, `JUDGE_URL` and `JUDGE_MODEL` naming it; the default
is a local one (`mlx_lm.server --model mlx-community/GLM-4.7-Flash-4bit --port
8080`), so the records, which carry process arguments, stay on the machine.
`judge-haiku.yaml` is the same pipeline with Claude Haiku on Amazon Bedrock
reading the records, through the AWS credential chain (`AWS_PROFILE`, or the
profile in the file, after `aws sso login`).

## What the plugin keeps

Endpoint Security reports everything: on a developer's Mac, about 13,000
`open` and 2,000 `close` events a minute, 7 GB a day of JSON. The plugin,
`macos`, keeps what a verdict can turn on and drops the rest before any model
sees it:

| event | kept when |
|---|---|
| `exec` | always: every program run, with its arguments and working directory; its environment cut to the names that steer an agent (`ANTHROPIC_BASE_URL`, `OPENAI_BASE_URL`, `GOOGLE_GEMINI_BASE_URL`, `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `GEMINI_CLI_HOME`, `OTEL_*`) and counted, since a program's environment is where its keys are |
| `open` | a write to any group below, or a read of a transcript or a credential |
| `close` | `modified`, on a group |
| `create`, `rename`, `unlink` | on a group (`rename` by either name) |
| `fork`, `exit` | never: they build the process tree |
| anything else (`btm_launch_item_add`, `tcc_modify`) | always, as it came, less its `stat` blocks |

The groups are the catalog's, plus credentials and what runs on its own
(`src/paths.rs` has the paths):

| `_facts` key | files |
|---|---|
| `agent_settings` | `.claude/settings*.json`, `.gemini/settings.json`, `.codex/`, `.vscode/tasks.json` |
| `agent_instructions` | `CLAUDE.md`, `AGENTS.md`, `GEMINI.md`, `~/.claude/projects/*/memory/`, `~/.codex/memories/`, `.cursor/rules/`, `.github/copilot-instructions.md` |
| `agent_mcp` | `~/.claude.json` (and its `.tmp.*`), `.mcp.json`, `.cursor/mcp.json`, `.vscode/mcp.json`, VS Code's `mcp.json` |
| `agent_state` | `~/.claude/projects/**/*.jsonl`, `~/.claude/history.jsonl`, `~/.codex/sessions/`, `~/.codex/history.jsonl`, `~/.gemini/tmp/` |
| `agent_extensions` | `~/.claude/plugins/`, `~/.claude/skills/`, `~/.codex/skills/`, `~/.gemini/extensions/`, `~/.cursor/extensions/`, `~/.vscode/extensions/` |
| `credentials` | `~/.aws/`, `~/.ssh/`, `~/.gnupg/`, `~/.npmrc`, `~/.netrc`, `~/.docker/config.json`, `~/.kube/config`, `~/.config/gh/`, `~/.codex/auth.json`, the keychains, `.env` and `.env.*` |
| `persistence` | `~/.zshrc` and the other rc files, `~/Library/LaunchAgents/`, `/Library/LaunchAgents/`, `/Library/LaunchDaemons/`, `.git/hooks/`, `.git/config`, `.github/workflows/` |

Each record carries the program that acted (`process`: path, signer,
platform, uid, tty, the responsible app) and `parents`, the programs above it
nearest first, from a process tree the plugin keeps in state from `fork`,
`exec` and `exit` (`runtime.workers: 1`, so events arrive in order; the state
is in memory, there is nothing to keep). `_norm.actor` is the program, for
an exec the one that started; `_norm.target` the file; `_facts` adds `agent_env`,
`scratch_program` (runs from /tmp, a cache, Downloads or node_modules),
`unsigned` and `rename_over`. About 700 bytes a record.

## The judge

`judge.yaml` reads each record with the model as it arrives, about sixteen to
a request, and `flagged` keeps what it flagged, and what it could not read.
`judge.md` tells the model what the catalog's techniques look like in these
records and what an agent does to its own files in the ordinary course: its
settings and transcripts, `~/.claude.json` written as a tmp file and renamed
over, a marketplace refresh that writes hundreds of plugin files in a minute,
`git` and the keychain as its children. There is no baseline: what the model
should know of normal is in that brief, so when this Mac's normal differs,
edit the brief.

## Tests

`tests/input.json` is thirty-five synthetic eslogger records (version 10,
schema 1, the shapes of a macOS 26 capture, no real values): a shell, an agent
started from it with `ANTHROPIC_BASE_URL` set, the agent saving its config and
reading the keychain, a package install under it whose `node` reads
credentials and a transcript, plants a settings file and `CLAUDE.md`, and runs
an unsigned program from `/tmp` that installs a LaunchAgent; and around them
the noise the plugin drops. `tests/expected.json` is the nineteen records the
plugin keeps. `make test` in `packs/` runs it; `cargo test` here runs the
path groups' unit tests. The plugin drops records, so `make fixtures-macos`
refuses to rewrite `expected.json`; after a change, run the test and copy
`test_out.ndjson` into it once you have read the diff.

## Not here

Endpoint Security has no network event: where a program connected is not in
these records, only that it ran `curl`. The capture this was built against
shows what an agent does to its own files; how Codex, Gemini and Cursor write
theirs, and what an agent's own exec looks like when a script starts it, are
written from the catalog, not seen. `btm_launch_item_add` and `tcc_modify`
have not been captured either: they pass through whole.
