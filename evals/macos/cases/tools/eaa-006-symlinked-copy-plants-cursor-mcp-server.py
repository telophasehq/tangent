"""EAA-006 as Adversa's SymJack chain did it against Cursor CLI (EAA-C-014,
steps 2 and 3): the agent's approved `cp` follows a repository symlink to the
global ~/.cursor/mcp.json; restarted, cursor-agent spawns the planted
server."""
import es

CASE = {
    "id": "eaa-006-symlinked-copy-plants-cursor-mcp-server",
    "title": "Cursor CLI's approved cp follows a repo symlink over ~/.cursor/mcp.json; on restart it starts the planted server",
    "flag": True,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {
        "catalog_case": "EAA-C-014",
        "steps": [2, 3],
        "sources": ["SRC-029", "https://adversa.ai/blog/the-approval-prompt-is-lying-to-you-symlink-rce-in-five-ai-coding-agents-claude-code-cursor-antigravity-copilot-grok-build/"],
    },
    "notes": (
        "Adversa (SymJack): Cursor does not auto-accept project MCP servers, so the proof of concept targets the "
        "global ~/.cursor/mcp.json; the report lists Cursor CLI 2026.05.20 and says a restart spawns the planted "
        "server in every product. It does not say whether the symlink's target is relative or absolute (an absolute "
        "one here; eslogger shows the resolved path either way), nor give Cursor's restart details: the server is "
        "written as Claude Code's was, `node -e` opening Calculator (TextEdit here, a stand-in). The repository instruction that asks for the copy "
        "(step 1, EAA-004) is content; the approval is a keypress. cursor-agent's install layout and the shell it "
        "runs commands in are not verified (es.py), and its version there (2026.10.22, in the path) is not the "
        "report's 2026.05.20."
    ),
}

REPO = "~/code/globex-media-kit"
CP = es.Program("cp", "/bin/cp", "com.apple.cp")
OPEN = es.Program("open", "/usr/bin/open", "com.apple.open")
DEMO = "require('child_process').exec('open -a TextEdit')"


def cursor_agent(mac, sh):
    a = mac.spawn(sh, "cursor-agent", ["cursor-agent"], cwd=REPO)  # env, then bash running the launcher script
    mac.exec(a, "cursor-agent-node", [f"{es.CURSOR_AGENT}/node", f"{es.CURSOR_AGENT}/index.js"])
    mac.read_file(a, "~/.cursor/mcp.json")
    mac.read_file(a, f"{REPO}/AGENTS.md")
    return a


def build(mac):
    for f, size in (("AGENTS.md", 23_400), ("README.md", 2400), ("media/intro.mp4", 1800), (".git/config", 300)):
        mac.exists(f"{REPO}/{f}", size=size, days=0)
    mac.exists("~/.cursor/mcp.json", size=180)
    sh = mac.terminal(cwd=REPO)
    mac.wait(5)
    agent = cursor_agent(mac, sh)
    mac.wait(15)  # the person asks to tidy the docs; the agent asks to copy a clip, and the person approves
    tool = es.agent_shell(mac, agent, "cp media/intro.mp4 docs/intro-a.mp4")
    cp = mac.spawn(tool, CP, ["cp", "media/intro.mp4", "docs/intro-a.mp4"])
    mac.read_file(cp, f"{REPO}/media/intro.mp4")
    mac.write_file(cp, "~/.cursor/mcp.json", key=True)  # the symlink's target
    mac.wait(0.02)
    mac.exit(cp)
    mac.exit(tool)
    mac.wait(25)
    mac.exit(agent)

    mac.wait(10)  # restarted
    agent = cursor_agent(mac, sh)
    server = mac.spawn(agent, "node", ["node", "-e", DEMO], cwd=REPO, fds="pipe", key=True)
    demo = mac.spawn(server, "sh", ["/bin/sh", "-c", "open -a TextEdit"], fds="pipe")
    mac.exec(demo, OPEN, ["open", "-a", "TextEdit"], key=True)
    mac.wait(0.3)
    mac.exit(demo)
    mac.wait(30)
    mac.exit(server)
    mac.exit(agent)
    mac.logout(sh)
