"""EAA-006 as Adversa's SymJack chain did it against Codex CLI (EAA-C-014,
steps 2 and 3): the agent's approved `cp` follows a repository symlink to the
project's .codex/config.toml, where Codex's MCP servers are configured;
restarted, Codex spawns the planted server."""
import es

CASE = {
    "id": "eaa-006-symlinked-copy-plants-codex-mcp-server",
    "title": "Codex CLI's approved cp follows a repo symlink over .codex/config.toml; on restart it starts the planted server",
    "flag": True,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {
        "catalog_case": "EAA-C-014",
        "steps": [2, 3],
        "sources": ["SRC-029"],
    },
    "notes": (
        "Adversa (SymJack) lists Codex CLI 0.133.0 with .codex/config.toml as the overwritten file and says the chain "
        "was confirmed in every product it names, with a restart spawning the planted server; it gives no "
        "Codex-specific restart detail, so the server is written as Claude Code's was (`node -e`, Calculator; TextEdit here, a stand-in), "
        "spawned by the native codex binary. The repository instruction (step 1, AGENTS.md, EAA-004) is read, not "
        "kept; the approval is a keypress. Codex's tool shell (`zsh -lc`) is from its docs, not seen, and any "
        "sandbox wrapper around it is left out (es.py)."
    ),
}

REPO = "~/code/globex-media-kit"
CP = es.Program("cp", "/bin/cp", "com.apple.cp")
OPEN = es.Program("open", "/usr/bin/open", "com.apple.open")
DEMO = "require('child_process').exec('open -a TextEdit')"


def codex(mac, sh):
    js = mac.spawn(sh, "codex", ["codex"], cwd=REPO)  # env, then node running codex.js
    native = mac.spawn(js, "codex-native", ["codex"])
    for f in ("~/.codex/config.toml", f"{REPO}/.codex/config.toml", f"{REPO}/AGENTS.md"):
        mac.read_file(native, f)
    return js, native


def build(mac):
    for f, size in (("AGENTS.md", 23_400), ("README.md", 2400), (".codex/config.toml", 0),
                    ("media/intro.mp4", 1800), (".git/config", 300)):
        mac.exists(f"{REPO}/{f}", size=size, days=0)
    sh = mac.terminal(cwd=REPO)
    mac.wait(6)
    js, agent = codex(mac, sh)
    day, clock = mac.now[:10], mac.now[11:19].replace(":", "-")
    rollout = f"~/.codex/sessions/{day.replace('-', '/')}/rollout-{day}T{clock}-{mac.uuid().lower()}.jsonl"
    mac.write_file(agent, rollout, new=True)
    mac.wait(14)  # the person asks to tidy the docs; the agent asks to copy a clip, and the person approves
    tool = es.agent_shell(mac, agent, "cp media/intro.mp4 docs/intro-a.mp4")
    cp = mac.spawn(tool, CP, ["cp", "media/intro.mp4", "docs/intro-a.mp4"])
    mac.read_file(cp, f"{REPO}/media/intro.mp4")
    mac.write_file(cp, f"{REPO}/.codex/config.toml", key=True)  # the symlink's target
    mac.wait(0.02)
    mac.exit(cp)
    mac.exit(tool)
    mac.write_file(agent, rollout, append=True)
    mac.wait(25)
    mac.exit(agent)
    mac.exit(js)

    mac.wait(9)  # restarted
    js, agent = codex(mac, sh)
    server = mac.spawn(agent, "node", ["node", "-e", DEMO], fds="pipe", key=True)
    demo = mac.spawn(server, "sh", ["/bin/sh", "-c", "open -a TextEdit"], fds="pipe")
    mac.exec(demo, OPEN, ["open", "-a", "TextEdit"], key=True)
    mac.wait(0.3)
    mac.exit(demo)
    mac.wait(30)
    mac.exit(server)
    mac.exit(agent)
    mac.exit(js)
    mac.logout(sh)
