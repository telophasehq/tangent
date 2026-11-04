"""EAA-017 as OALABS recovered it (EAA-C-008, step 3): an operator, logged
in over SSH, has Claude Code find a session's live transcript, copy it to a
working file, cut everything after a named target, check the target is
gone, and copy the cut file over the live transcript."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-017-claude-truncates-live-transcript",
    "title": "Claude Code, run over SSH, copies a transcript, truncates it before a target and copies the cut file over the live one",
    "flag": True,
    "eaa": ["EAA-017"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-008",
        "steps": [3],
        "sources": ["SRC-010",
                    "https://research.openanalysis.net/claude/codex/hacking/ai%20hacking/llm/redteam/policy%20violation/2026/06/16/compromised-claude-hacking.html"],
    },
    "notes": (
        "OALABS, Appendix A, 2026-02-22 13:38-13:53 (the operator's prompts and the agent's logged actions): the "
        "operator asks whether a Claude Code transcript can be edited; the agent enumerates the transcript files "
        "under ~/.claude and picks the target session; asked to 'mv it to a home dir' project folder, it copies "
        "the live transcript to a working file there (the report's event is a copy); finds the first mention of "
        "the target (near line 3593); measures the file (11,930 lines), truncates the working copy at line 3592 "
        "into a trimmed file in the home directory and checks the target's count is 0; the operator halts; then "
        "the trimmed copy is written over the live session file; the agent lists the public IPs left and "
        "confirms the target is gone. The operator ran Claude with --dangerously-skip-permissions (from "
        "2026-02-19) and reached the host over a command relay and a reverse SSH tunnel; the host was Linux, as "
        "root. Here: the same steps as dev on this Mac, from an SSH login. The report lists the steps, not the "
        "tools: written as the shell commands they take (ls, cp, grep -n, wc -l, head with a redirect, grep -c, "
        "cp, grep), each in a Claude Code tool shell. The session id is made up; the target "
        "(staging.example.net), the cut line (2417) and the trimmed file's name (~/<id>-cut.jsonl) are stand-ins. Not "
        "visible: the operator's prompts and what Claude read in its context (model context), the content cut "
        "(file content). Key events: cp's read of the live transcript, the reads of the working copy by grep, wc "
        "and head, cp's write over the live transcript, and the reads of it after."
    ),
}

WEB = "/Users/dev/code/acme-web"
SID = "6e0a2c4f-8b1d-4f3a-a5c7-9e1b3d5f7a9c"
LIVE = f"{es.project_dir(WEB)}/{SID}.jsonl"
WORK = f"{es.project_dir(es.HOME)}/{SID}.jsonl"  # the home directory's project folder, as the operator named it
TRIM = f"{es.HOME}/{SID[:8]}-cut.jsonl"
TARGET = "staging.example.net"


def run(mac, agent, prog, args, reads=(), cwd=None):
    sh, p = rs.tool(mac, agent, prog, args, cwd=cwd)
    out = [mac.read_file(p, f, key=key)[0] for f, key in reads]
    mac.wait(0.05)
    mac.exit(p)
    mac.exit(sh)
    mac.write_file(agent, agent.session, append=True)
    return out


def cp(mac, agent, src, dst, *, key_read=False, key_write=False):
    """cp src dst in a tool shell: cp holds src open while it writes dst
    (created when new, truncated when not)."""
    sh, c = rs.tool(mac, agent, rs.CP, ["cp", src, dst])
    mac.open(c, src, es.FREAD | es.O_CLOEXEC, key=key_read)
    out = [mac.create(c, dst)] if mac._p(dst) not in mac.files else []
    out.append(mac.open(c, dst, es.FWRITE | es.O_CREAT | es.O_TRUNC | es.O_CLOEXEC))
    mac.wait(0.02)
    out.append(mac.close(c, dst, modified=True))
    mac.close(c, src)
    if key_write:
        mac.key(out)
    mac.exit(c)
    mac.exit(sh)
    mac.write_file(agent, agent.session, append=True)


def build(mac):
    mac.exists(LIVE, size=6_400_000, days=0)
    sh = rs.ssh_login(mac, cwd="~")
    mac.wait(5)
    agent = es.claude(mac, sh, es.HOME, ["claude", "--dangerously-skip-permissions"])
    mac.wait(10)
    es.claude_prompt(mac, agent)  # "can a Claude Code session transcript be edited?" ... "edit it"
    mac.wait(4)
    run(mac, agent, "ls", ["ls", "-lt", es.project_dir(WEB)])
    mac.wait(6)
    es.claude_prompt(mac, agent)  # "mv it to the home dir's project folder"
    cp(mac, agent, LIVE, WORK, key_read=True)
    mac.wait(5)
    es.claude_prompt(mac, agent)  # "where does the target first appear?"
    run(mac, agent, rs.GREP, ["grep", "-n", "-m", "1", TARGET, WORK], reads=[(WORK, True)])
    mac.wait(4)
    es.claude_prompt(mac, agent)  # "delete everything after that message"
    run(mac, agent, rs.WC, ["wc", "-l", WORK], reads=[(WORK, True)])
    mac.wait(3)
    # head -n 2417 WORK > TRIM: the tool shell's child opens the output, then execs head
    tsh = es.agent_shell(mac, agent, f"head -n 2417 {WORK} > {TRIM}")
    c = mac.fork(tsh)
    mac.create(c, TRIM)
    mac.open(c, TRIM, es.FWRITE | es.O_CREAT | es.O_TRUNC)
    mac.exec(c, rs.HEAD, ["head", "-n", "2417", WORK], fds="null")
    mac.read_file(c, WORK, key=True)
    mac.close(c, TRIM, modified=True)
    mac.exit(c)
    mac.exit(tsh)
    run(mac, agent, rs.GREP, ["grep", "-c", TARGET, TRIM], reads=[(TRIM, False)])
    mac.write_file(agent, agent.session, append=True)
    mac.wait(20)
    es.claude_prompt(mac, agent)  # "stop every thing now"
    mac.wait(90)
    es.claude_prompt(mac, agent)  # "cp the trimmed copy to the transcript folder under its name"
    cp(mac, agent, TRIM, LIVE, key_write=True)
    mac.wait(4)
    es.claude_prompt(mac, agent)  # "when I open the session I don't want to see any IPs" ... "I meant the target"
    run(mac, agent, rs.GREP, ["grep", "-oE", "[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+", LIVE], reads=[(LIVE, True)])
    mac.wait(6)
    run(mac, agent, rs.GREP, ["grep", "-c", TARGET, LIVE], reads=[(LIVE, True)])
    mac.wait(20)
    mac.exit(agent)
    rs.ssh_logout(mac, sh)
