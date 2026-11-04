"""EAA-016 as SpecterOps' Blacklight Scout does it on macOS (the technique
page's documented procedure): an operator's C2 agent already on the Mac
uploads Scout's dylib to /tmp and loads it in-process, and Scout walks the
known config, session, plugin and extension directories of Codex, Claude
Code and Cursor with opendir and lstat, reading no file bodies."""
from datetime import date, timedelta

import es

CASE = {
    "id": "eaa-016-c2-scout-enumerates-agent-stores",
    "title": "A C2 agent loads a scout dylib from /tmp and opens the session, project, plugin and extension "
             "directories of Codex, Claude Code and Cursor in a second",
    "flag": True,
    "eaa": ["EAA-016"],
    "group": "launcher",
    "grounding": {
        "catalog_case": None,
        "steps": ["technique page EAA-016: Blacklight Scout v0.2.0, POSIX loader"],
        "sources": ["SRC-056", "SRC-057", "https://github.com/SpecterOps/Blacklight/releases/tag/v0.2.0",
                    "https://specterops.io/blog/2026/08/12/blacklight-ai-agent-endpoint-artifacts/"],
    },
    "notes": (
        "Technique page and hunting.md H9: Scout's BOF and POSIX loaders enumerate known authentication, "
        "configuration, rules and session paths for Codex, Claude Code, Cursor and Antigravity CLI from filesystem "
        "metadata and directory entries without reading artifact bodies, and run in-process, so the activity is "
        "the hosting C2 agent's. Scout's README: on macOS, Poseidon's execute_library of /tmp/<dylib>, function "
        "bl_scout_run, no arguments. Its POSIX source (ai_path_scout_posix.c, v0.2.0) uses opendir/readdir and "
        "lstat/stat only, in three passes (run_posix_triage): first each darwin target of catalog/targets.json, "
        "in the file's order, is lstat'd, and each one that is a directory has its children counted "
        "(count_directory_children at depth 1: one opendir, no descent); then scan_dynamic_targets walks "
        "~/.codex/rules, ~/.claude/projects and ~/.cursor/projects six levels down and ~/.cursor/plans one level, "
        "for .rules, .mcp.json, sessions-index.json and .plan.md files; last, scan_session_candidates walks each "
        "session or workspace directory the first pass found (~/.codex/sessions, ~/.claude/projects, "
        "~/.cursor/chats) to the bottom, lstat'ing each file to rank transcripts by size and age. The walks go "
        "depth first and close a directory after its subdirectories. eslogger has no stat or readdir event in "
        "this pack's subscription; opendir opens the directory, so each directory listed is an open (read, "
        "O_DIRECTORY) and a close, and that is all eslogger shows of the scan. Directory opens are assumed from "
        "opendir's open(2); none were in the capture. Targets not on dev's Mac (~/.codex/archived_sessions, "
        "~/.codex/rules, ~/.claude/sessions, ~/.cursor/projects, ~/.cursor's skills, skills-cursor, worktrees, "
        "snapshots, agents and plugins, ~/.gemini/antigravity-cli) fail their lstat and show nothing. The trees "
        "walked are dev's as these evals have them: Claude Code projects for acme-api and acme-web (with a memory "
        "directory), Codex's day directories (two before the timeline, then one for each earlier work day of it, "
        "as background.py's Codex writes them), one Cursor CLI chat; readdir order assumed alphabetical. The C2 "
        "agent (an ad hoc signed binary, its path and start not in the sources: made up), its upload of the dylib "
        "and the output's return are the operator's channel: the upload is a file write, the rest network. The "
        "dylib's name is made up. Key events: the directory opens."
    ),
}

O_DIRECTORY = 0x100000
C2 = es.dropped("~/Library/Application Support/com.acme.syncd/acme-syncd")
DYLIB = "/private/tmp/libscout.dylib"
H = es.HOME
CODEX, CLAUDE, CURSOR = f"{H}/.codex", f"{H}/.claude", f"{H}/.cursor"
# the first pass: Scout's darwin targets that are directories on dev's Mac, in targets.json's order
TARGETS = [CODEX, f"{CODEX}/sessions", CLAUDE, f"{CLAUDE}/projects", f"{CLAUDE}/plugins",
           CURSOR, f"{CURSOR}/chats", f"{CURSOR}/plans", f"{CURSOR}/extensions"]
# the trees the walks descend: {directory: its subdirectories, in readdir order}
PROJECTS = f"{CLAUDE}/projects"
WEB_PROJECT = f"{PROJECTS}/-Users-dev-code-acme-web"
CLAUDE_TREE = {PROJECTS: [f"{PROJECTS}/-Users-dev-code-acme-api", WEB_PROJECT], WEB_PROJECT: [f"{WEB_PROJECT}/memory"]}
CHAT = f"{CURSOR}/chats/5b0e2a61c9d84f3e"
CURSOR_TREE = {f"{CURSOR}/chats": [CHAT], CHAT: [f"{CHAT}/3f9c2d7e-1a4b-4c8d-9e0f-2b6a7c1d5e83"]}
TIMELINE = date(2026, 10, 5)  # gen.py's first work day


def codex_tree(today):
    """~/.codex/sessions/YYYY/MM/DD: two days before the timeline, then each
    work day of it before `today`."""
    days = [date(2026, 9, 28), date(2026, 10, 1)]
    d = TIMELINE
    while d < today:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    root, tree = f"{CODEX}/sessions", {}
    for d in days:
        y = f"{root}/{d.year}"
        m = f"{y}/{d.month:02d}"
        for parent, child in ((root, y), (y, m), (m, f"{m}/{d.day:02d}")):
            kids = tree.setdefault(parent, [])
            if child not in kids:
                kids.append(child)
    return tree


def resident(mac, prog, args, *, cwd="~", started=5400):
    """A process of dev's already running when the case begins, its start not
    in the source: launchd's child by then, a session of its own, no terminal,
    no events until it acts."""
    t, mac.t = mac.t, mac.t - started * 10**9
    pid, pv, thread = mac._ticket("pid", 400, 1, 99000), mac._ticket("pidversion", 1000, 2), mac._ticket("thread", 1000, 50)
    p = es.Proc(pid=pid, pv=pv, ppid=1, oppid=1, ruid=es.UID, euid=es.UID, rgid=es.STAFF, egid=es.STAFF,
                asid=es.ASID, auid=es.UID, tty=None, pgid=pid, sid=pid, start=mac.t, prog=es.program(prog),
                cwd=mac._p(cwd), env=dict(es._GUI_ENV), thread=thread, interactive=False, args=args)
    mac.t = t
    p.responsible = p
    p.parent_token = mac._token(mac.launchd)
    return p


def opendir(mac, proc, path):
    """opendir(3): an open of the directory for reading (a key event)."""
    ev = mac.open(proc, path, es.FREAD | es.O_NONBLOCK | O_DIRECTORY | es.O_CLOEXEC, key=True)
    ev["event"]["open"]["file"] = mac._dir(path)
    return ev


def closedir(mac, proc, path):
    cl = mac.close(proc, path)
    cl["event"]["close"]["target"] = mac._dir(path)


def count(mac, proc, path):
    """count_directory_children(path, 1): list the directory, descend nowhere."""
    opendir(mac, proc, path)
    mac.wait(0.0002)
    closedir(mac, proc, path)


def walk(mac, proc, tree, path, depth=None):
    """scan_dynamic_tree (to `depth` levels below) and walk_session_files (to
    the bottom): list a directory, walk each subdirectory in turn, close it."""
    opendir(mac, proc, path)
    mac.wait(0.0001)
    if depth is None or depth > 0:
        for sub in tree.get(path, []):
            walk(mac, proc, tree, sub, None if depth is None else depth - 1)
    closedir(mac, proc, path)


def build(mac):
    agent = resident(mac, C2, [C2.path])
    # the operator uploads the dylib, then has the agent load it and call bl_scout_run
    mac.write_file(agent, DYLIB, new=True)
    mac.wait(mac.rng.uniform(20, 60))
    mac.read_file(agent, DYLIB)  # dlopen
    mac.wait(0.01)
    # first pass: each target's children counted
    for d in TARGETS:
        count(mac, agent, d)
    # second: ~/.claude/projects six levels down, ~/.cursor/plans one (no ~/.codex/rules or ~/.cursor/projects here)
    walk(mac, agent, CLAUDE_TREE, PROJECTS, depth=6)
    walk(mac, agent, {}, f"{CURSOR}/plans", depth=0)
    # third: the session directories, to the bottom
    walk(mac, agent, codex_tree(date.fromisoformat(es.iso(mac.t)[:10])), f"{CODEX}/sessions")
    walk(mac, agent, CLAUDE_TREE, PROJECTS)
    walk(mac, agent, CURSOR_TREE, f"{CURSOR}/chats")
    # the summary goes back over the C2 channel: network, not here
