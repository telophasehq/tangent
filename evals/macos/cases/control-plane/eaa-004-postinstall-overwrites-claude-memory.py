"""EAA-004 as Cisco's proof of concept did it (EAA-C-010, step 2): under the
`npm install` the person approved for Claude Code, a dependency's postinstall
overwrites every project's MEMORY.md under ~/.claude/projects/ and appends to
~/.zshrc an alias for `claude` that forces auto-memory on; the next claude
the person starts in a new tab carries CLAUDE_CODE_DISABLE_AUTO_MEMORY=0 and
reads the poisoned memory."""
import es

CASE = {
    "id": "eaa-004-postinstall-overwrites-claude-memory",
    "title": "A postinstall under Claude Code's npm install overwrites every project's MEMORY.md and aliases claude in ~/.zshrc",
    "flag": True,
    "eaa": ["EAA-004"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-010",
        "steps": [2],
        "sources": ["SRC-022", "SRC-013", "SRC-014",
                    "https://blogs.cisco.com/ai/identifying-and-remediating-a-persistent-memory-compromise-in-claude-code"],
    },
    "notes": (
        "Cisco: the user asked Claude to set up the repository and approved the dependency install; the npm lifecycle "
        "hook overwrote every project's ~/.claude/projects/*/memory/MEMORY.md and appended to .zshrc an alias for claude "
        "that sets CLAUDE_CODE_DISABLE_AUTO_MEMORY=0 on each launch (SRC-014: 0 forces auto memory on). The directory "
        "listing that finds the projects has no event in this capture's types. Written as a node script's plain "
        "truncating writes and an append (the source does not say how). The last part is the alias taking effect: a "
        "new tab sources ~/.zshrc and `claude` starts with the variable set, then reads its memory. Step 1 (the "
        "UserPromptSubmit hook) is the case postinstall-plants-claude-hook; step 3 (the model following the marker "
        "instruction) is model behaviour, not visible. acme-colors is made up."
    ),
}

WEB = "~/code/acme-web"
PKG = f"{WEB}/node_modules/acme-colors"
PROJECTS = ["acme-web", "acme-api", "globex-sdk"]


def build(mac):
    memories = [f"{es.project_dir(es.HOME + '/code/' + p)}/memory/MEMORY.md" for p in PROJECTS]
    for m in memories:
        mac.exists(m)
    sh = mac.terminal(cwd=WEB)
    mac.wait(6)
    agent = es.claude(mac, sh, WEB)
    mac.wait(14)
    es.claude_prompt(mac, agent)  # "set up this repo"; the person approves `npm install`
    mac.wait(7)

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
    for m in memories:
        mac.write_file(script, m, key=True)
        mac.wait(0.002)
    mac.read_file(script, "~/.zshrc")
    mac.write_file(script, "~/.zshrc", append=True, key=True)
    mac.wait(0.1)
    mac.exit(script)
    mac.write_file(npm, f"{WEB}/node_modules/.package-lock.json", new=True)
    mac.write_file(npm, f"{WEB}/package-lock.json")
    mac.exit(npm)
    mac.exit(tool)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(30)
    mac.exit(agent)
    mac.logout(sh)

    mac.wait(240)
    tab = mac.terminal(cwd="~/code/acme-api")  # reads ~/.zshrc, alias and all
    mac.wait(5)
    again = es.claude(mac, tab, "~/code/acme-api", env_add={"CLAUDE_CODE_DISABLE_AUTO_MEMORY": "0"}, key=True)
    mac.read_file(again, memories[1])
    mac.wait(20)
    es.claude_prompt(mac, again)
    mac.wait(30)
    mac.exit(again)
    mac.logout(tab)
