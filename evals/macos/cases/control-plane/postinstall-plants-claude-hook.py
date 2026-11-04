"""EAA-003 as Cisco's proof of concept did it (EAA-C-010, step 1): the person
asks Claude Code to set up a repository and approves `npm install`; a
dependency's postinstall overwrites the user-level ~/.claude/settings.json
with a UserPromptSubmit hook."""
import es

CASE = {
    "id": "postinstall-plants-claude-hook",
    "title": "A dependency's postinstall, under the npm install Claude Code ran, overwrites ~/.claude/settings.json",
    "flag": True,
    "eaa": ["EAA-003"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-010",
        "steps": [1],
        "sources": ["SRC-022", "https://blogs.cisco.com/ai/identifying-and-remediating-a-persistent-memory-compromise-in-claude-code"],
    },
    "notes": (
        "Cisco: the user instructed Claude to set up the repository and approved the dependency install, which the "
        "agent ran; the package's npm postinstall overwrote the global ~/.claude/settings.json with a UserPromptSubmit "
        "hook. The source does not say how the file was written (read first, atomically) or which runtime ran the "
        "script: written as a node script's plain read and truncating write. The hook's command is file content, "
        "which eslogger does not show; so is the registry download (network). Steps 2 and 3 (MEMORY.md and the shell "
        "rc, EAA-004) are not in this case. acme-colors is a made-up package."
    ),
}

WEB = "~/code/acme-web"
PKG = f"{WEB}/node_modules/acme-colors"


def build(mac):
    sh = mac.terminal(cwd=WEB)
    mac.wait(6)
    agent = es.claude(mac, sh, WEB)
    mac.wait(15)
    es.claude_prompt(mac, agent)  # "set up this repo"; the person approves `npm install`
    mac.wait(8)

    tool = es.agent_shell(mac, agent, "npm install")
    npm = mac.spawn(tool, "npm", ["npm", "install"])
    for f in ("~/.npmrc", f"{WEB}/package.json", f"{WEB}/package-lock.json"):
        mac.read_file(npm, f)
    mac.wait(4)
    for f in ("package.json", "index.js", "scripts/postinstall.js"):
        mac.write_file(npm, f"{PKG}/{f}", new=True)

    script = es.npm_lifecycle(mac, npm, PKG, "node scripts/postinstall.js", version="2.4.1")
    mac.exec(script, "node", ["node", "scripts/postinstall.js"], key=True)
    mac.read_file(script, f"{PKG}/scripts/postinstall.js")
    mac.read_file(script, "~/.claude/settings.json")
    mac.write_file(script, "~/.claude/settings.json", key=True)
    mac.wait(0.2)
    mac.exit(script)

    mac.write_file(npm, f"{WEB}/node_modules/.package-lock.json", new=True)
    mac.write_file(npm, f"{WEB}/package-lock.json")
    mac.exit(npm)
    mac.exit(tool)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(20)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(40)
    mac.exit(agent)
    mac.logout(sh)
