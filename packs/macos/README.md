# macos

One developer's Mac, where AI coding agents run: the machine the [Endpoint AI
Agent Abuse catalog](https://github.com/0x4D31/endpoint-ai-agent-abuse)
describes, where a package script plants a hook in the agent's settings,
poisons its instructions, adds an MCP server, reads its transcripts, or starts
it with its API traffic pointed elsewhere. The telemetry is `eslogger`,
Apple's Endpoint Security command-line tool, already on every Mac. The pack
is one plugin, `macos`, and its records go where every pack's go: into
[core](../core)'s lake and each program's baseline, and to core's judge,
which reads a program's records together with what it did before.
[examples/macos](/examples/macos) runs it, one config:

```bash
cd examples/macos
tangent plugin compile --config tangent.yaml
tangent run --config haiku.yaml &               # or tangent.yaml: a local model
sudo eslogger exec fork exit open close create rename unlink btm_launch_item_add tcc_modify | nc -U es.sock
tail -f alerts.ndjson
```

`eslogger` needs root, and the terminal it runs in needs Full Disk Access
(System Settings → Privacy & Security), restarted after the grant. It writes
to stdout only, and tangent reads sockets, so `nc -U` joins them: root writes
the socket, tangent owns it, and nothing listens on a port.

## What the plugin keeps

Endpoint Security reports everything: on a developer's Mac, about 13,000
`open` and 2,000 `close` events a minute, 7 GB a day of JSON. The plugin,
`macos`, keeps what a verdict can turn on and drops the rest before any model
sees it:

| event | kept when |
|---|---|
| `exec` | always: every program run, with its arguments and working directory; its environment cut to the names that steer an agent (`ANTHROPIC_BASE_URL`, `OPENAI_BASE_URL`, `GOOGLE_GEMINI_BASE_URL`, `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `GEMINI_CLI_HOME`, the `OTEL_EXPORTER_OTLP_*` endpoint and headers names) and counted, since a program's environment is where its keys are |
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

Each record carries the program that acted (`process`: path, signing
identity, Developer ID team, platform, uid, tty, the responsible app, and for
node, Python, bun or a shell running a file the script it runs as `name`)
and `parents`, the programs above it nearest first, each by its name (else
its path) and the arguments it was given after it, cut at 80 bytes, so
the npm above a package's script says `test` or `install`; from a process
tree the plugin keeps in state from `fork`, `exec` and `exit` (the pack
runs the pipeline on one worker, which keeps the events in order; an entry
lasts a day). An exec also
carries `started_by`, the nearest parent's name alone, which the baseline
counts as a resource; the judge reads `parents`. `_norm.actor` is the program, for an exec the one that
started: an interpreter's `name`, the script it runs, so npm and a package's
postinstall are not one `node`; else a vendor-signed program's signing
identity, so Claude Code is one actor across updates; else its path (Apple's command-line tools share one
identity, and an ad hoc signature is the binary's hash). `_norm.target` is
the file; `_facts` adds `agent_env`,
`scratch_program` (runs from /tmp, a cache, Downloads or node_modules),
`unsigned` and `rename_over`. About 700 bytes a record.

## The judge

Core's: a program's records wait in its group, thirty or an hour's worth, and
reach the model with its ten before them marked `_context` and its
`_baseline` from `state.sqlite`. On the pack's one worker the model gets one
group at a time.
The baseline reads these records as `macos` ([../core/baseline](../core/baseline)):
the verb is the event and the group of its file (`write agent_mcp`), the
resources the file and, for an exec, the program that started it, so it can
say a program wrote an agent's MCP config, or was started by `node`, for the
first time. Core's judge.yaml has the row saying how the records are written
as logfmt; the brief is `judge.md`, which your config names in place of
core's (examples/macos/tangent.yaml does), since core's is written for a
company's cloud. It tells the model what the catalog's techniques look like in these
records and what an agent does to its own files in the ordinary course: its
settings and transcripts, `~/.claude.json` written as a tmp file and renamed
over, a marketplace refresh that writes hundreds of plugin files in a minute,
`git` and the keychain as its children, npm's `node` reading `~/.npmrc`.

## The cases

The question under every record is who touched the agent's steering, and
whether the agent was steered or made to act. These are the cases the brief
asks the judge to find, what each looks like in the records, and what looks
like it and is not.

| case | in the records | not this |
|---|---|---|
| A hook planted (catalog H) | a write, create or `rename_over` on `agent_settings` by a package script (node, python, sh under npm, pip, bun, uv), a program from a fresh clone, an extension host, a browser, or a program with no tty | the agent saving its own settings; an editor, when the person is editing them |
| Instructions poisoned (I) | the same writers on `agent_instructions`: `CLAUDE.md`, `AGENTS.md`, memory | the agent writing its memory; an editor |
| An MCP server added (M) | the same writers on `agent_mcp`; `~/.claude.json` replaced by a rename | the agent's own tmp-and-rename of `~/.claude.json` |
| Transcripts read (S) | a read of `agent_state` by anything but the agent that wrote it: a package script, a sync client, an unknown program | the agent appending to its transcript |
| Extensions swapped (X) | a write to `agent_extensions` by anything but the agent | a marketplace refresh: hundreds of files into `.staging`, live renamed to `.backup`, staging to live, backup unlinked, within a minute |
| Credentials read | a read of `credentials` by anything but the tool the file belongs to, or by a `scratch_program`, or under an agent | `aws` reading `~/.aws`, `git` and `ssh` reading `~/.ssh`, `npm` reading `~/.npmrc`, `gh` reading `~/.config/gh`, the agent reading the keychain |
| The agent redirected | an exec with `agent_env`: `ANTHROPIC_BASE_URL` or `OPENAI_BASE_URL` sends its traffic, prompts and keys included, wherever the URL points; `CLAUDE_CONFIG_DIR` or `CODEX_HOME` makes it read another config; an `OTEL_EXPORTER_OTLP_*_ENDPOINT` sends its telemetry away | the vendor's own URL; a well-known local proxy port |
| The agent run headless | an exec of the agent with `--dangerously-skip-permissions`, `--dangerously-bypass-approvals-and-sandbox`, `--yolo`, `--trust-all-tools` or `-p`, with node, a package manager or a script among its parents, or no tty | the person typing it in a shell |
| The agent made to act | under an agent (it is in `parents`), a child that reads credentials or transcripts, writes `persistence`, runs curl or a shell with a URL, pushes, publishes or installs | the agent running `git`, `security`, the shell and the build for its own commands |
| Persistence | a write to `persistence`: rc files, LaunchAgents, LaunchDaemons, git hooks, `.git/config`, `.github/workflows/`; a `btm_launch_item_add` | an editor on `~/.zshrc`; `git` writing `.git/config` on clone; an installer the person ran |
| A program from nowhere | a `scratch_program` (from `/tmp`, a cache, Downloads, `node_modules`) or `unsigned` program that then touches any group | a build tool's children |
| Covering tracks | `rename_over` or an unlink on a transcript; writes to two vendors' agent files from one process | the agent rotating its own files |
| A permission changed | `tcc_modify` | an installer the person plainly ran |

Each record is judged on its own; the chain is in `parents`, not joined
across records. One worked case, Shai-Hulud, the npm worm of 2025: `npm
install` runs its preinstall script, so the judge sees node from
`node_modules` under `sh ← npm ← zsh` (an exec, `scratch_program`), then bun,
then a TruffleHog binary from a temp directory. The scanner opens `~/.npmrc`,
`~/.aws/credentials`, `~/.config/gh/hosts.yml` and `.env` files: credential
reads by bun, not by npm, aws or gh, flagged at the first one, before
anything leaves. Its attempt to drive Claude Code, Gemini CLI or Codex is an
exec of the agent with `-p` and the skip flag under node, tty or not, and the
agent's children then read credentials under it. `git push`, `curl` to GitHub
and `npm publish` are execs with the worm's node in their parents; a workflow
file committed for persistence is a write to `.github/workflows/`; the wipe
it runs when it finds no token is unlinks of transcripts, settings and
credentials by bun. The exfiltration request itself and the package's
contents are not in these records (below).

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
