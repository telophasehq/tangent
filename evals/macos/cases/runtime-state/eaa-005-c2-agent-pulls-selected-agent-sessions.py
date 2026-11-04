"""EAA-005 as SpecterOps describes Blacklight's research (the technique
page's example; SRC-057, SRC-056): a C2 agent already on the endpoint lists
the known agent session roots, the operator picks up to three session files
per tool, credentials first, and the agent transfers the chosen files over
its channel for offline analysis."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-005-c2-agent-pulls-selected-agent-sessions",
    "title": "A program launchd keeps running lists the agents' session roots and reads chosen transcripts and auth.json",
    "flag": True,
    "eaa": ["EAA-005"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": None,
        "steps": ["technique page EAA-005, example: SpecterOps Blacklight v0.2.0 selected-session transfer over a Mythic C2 channel"],
        "sources": ["SRC-057", "SRC-056", "https://specterops.io/blog/2026/08/12/blacklight-ai-agent-endpoint-artifacts/"],
    },
    "notes": (
        "SpecterOps: Scout, run on the endpoint through a Mythic C2 agent (on the MacBook: Poseidon, loading "
        "Scout's macOS dylib), checks whether each known root exists and what type it is (~/.codex with "
        "sessions/ and archived_sessions/, ~/.claude with projects/ and sessions/, ~/.cursor/chats, "
        "~/.gemini/antigravity-cli/conversations) and returns filesystem metadata only (size and recency; it "
        "reads no artifact bodies); it ranks up to three session-file candidates per tool, credential paths "
        "first; the operator chooses, and the chosen files move over the C2 channel for offline parsing. Where "
        "the C2 agent runs from, and whether the dylib touches disk, are not given: written as an ad hoc signed "
        "binary under ~/Library/Application Support (a made-up name) that launchd runs as dev (how it got there "
        "is another case), doing the listing and the reads in its own process. eslogger has no event for a stat "
        "and the pack does not subscribe to one; ranking candidates by size and recency means listing the "
        "session directories, and listing a directory opens it, so the directory opens are written (an "
        "inference: the post does not name the calls). Chosen files: ~/.codex/auth.json first, two Claude Code "
        "transcripts, two Codex rollouts, a Cursor chat store and an Antigravity conversation database (layout "
        "from the post; file names made up). Not visible: the metadata probe (stat), the operator's choice, the "
        "transfer (network). Key events: the reads of the chosen files."
    ),
}

AGENT = es.dropped("~/Library/Application Support/qorvexd/qorvexd")
O_DIRECTORY = 0x100000
CLAUDE_WEB = es.project_dir("/Users/dev/code/acme-web")
CLAUDE_API = es.project_dir("/Users/dev/code/acme-api")
CODEX = "~/.codex/sessions/2026/10"
DIRS = ["~/.codex/sessions", f"{CODEX}/02", f"{CODEX}/05", "~/.codex/archived_sessions", "~/.claude/projects", CLAUDE_WEB,
        CLAUDE_API, "~/.claude/sessions", "~/.cursor/chats", "~/.gemini/antigravity-cli/conversations"]
CHOSEN = [
    "~/.codex/auth.json",
    f"{CLAUDE_WEB}/5e1d7c3b-9a2f-4b6e-8d0c-2f4e6a8b0c1d.jsonl",
    f"{CLAUDE_API}/a4c6e8f0-1b3d-4f5a-9c7e-3d5f7b9a1c2e.jsonl",
    f"{CODEX}/05/rollout-2026-10-05T09-14-22-6b8d0f2a-4c6e-4a8b-9d1f-3e5a7c9b1d2f.jsonl",
    f"{CODEX}/02/rollout-2026-10-02T15-40-03-1f3b5d7e-9a2c-4e6b-8d0f-5a7c9e1b3d4f.jsonl",
    "~/.cursor/chats/7d9f1b3e/store.db",
    "~/.gemini/antigravity-cli/conversations/c1e3a5b7.db",
]


def build(mac):
    for i, f in enumerate(CHOSEN):
        mac.exists(f, size=20_000 + 150_000 * i, days=1 + 3 * i)
    c2 = rs.app(mac, AGENT)
    for d in DIRS:  # the roots listed: opendir, read the entries, closedir (mac.open would stat it as a file)
        d = mac._p(d)
        mac._stamp()
        mac._event(c2, "open", {"fflag": es.FREAD | es.O_NONBLOCK | O_DIRECTORY | es.O_CLOEXEC, "file": mac._dir(d)},
                   result={"result": {"flags": 4294967295}, "result_type": 1})
        mac._stamp()
        mac._event(c2, "close", {"modified": False, "target": mac._dir(d), "was_mapped_writable": False})
        mac.wait(0.02)
    mac.wait(mac.rng.uniform(120, 300))  # the operator reviews the ranked candidates
    for f in CHOSEN:
        mac.read_file(c2, f, key=True)
        mac.wait(mac.rng.uniform(2, 9))  # each file over the channel (network)
