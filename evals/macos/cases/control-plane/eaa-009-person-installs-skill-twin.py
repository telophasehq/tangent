"""The twin of eaa-009-skill-installs-same-name-skill: the person, in their
own terminal, installs their company's skill with the marketplace's
installer, which writes a new skill into ~/.claude/skills/."""
import es

CASE = {
    "id": "eaa-009-person-installs-skill-twin",
    "title": "The person installs acme's changelog skill with the skill installer from their terminal",
    "flag": False,
    "eaa": ["EAA-009"],
    "group": "control-plane",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-021", "SRC-078"]},
    "notes": (
        "Plainly benign: the rightful user installs a skill from their own company's repository with the same made-up "
        "skillmart CLI the attack cases use, typed in a Terminal tab (tty), into a new directory under ~/.claude/skills/ "
        "(SRC-021's personal location); nothing is overwritten. Key events: the skill writes."
    ),
}

SKILLS = "~/.claude/skills"
NPX = f"{es.HOME}/.npm/_npx/3f2a9c1e7b6d4a05"
SKILLMART = es.Program("skillmart", f"{NPX}/node_modules/skillmart/dist/cli.mjs", dyld=f"{NPX}/node_modules/.bin/skillmart",
                       interp=("env", ["node"]), size=3816)


def build(mac):
    for f in ("package.json", "dist/cli.mjs", "dist/install.mjs"):
        mac.exists(f"{NPX}/node_modules/skillmart/{f}", days=30)
    sh = mac.terminal(cwd="~/code/acme-web")
    mac.wait(6)
    npx = mac.spawn(sh, "npx", ["npx", "skillmart", "add", "acme/agent-skills", "--skill", "acme-changelog"])
    mac.read_file(npx, "~/.npmrc")
    s = mac.spawn(npx, "sh", ["sh", "-c", "skillmart add acme/agent-skills --skill acme-changelog"], fds="pipe")
    mac.exec(s, SKILLMART, ["skillmart", "add", "acme/agent-skills", "--skill", "acme-changelog"])
    mac.read_file(s, f"{NPX}/node_modules/skillmart/dist/install.mjs")
    mac.wait(1.6)
    for f in ("SKILL.md", "templates/entry.md"):
        mac.write_file(s, f"{SKILLS}/acme-changelog/{f}", new=True, key=True)
    mac.exit(s)
    mac.exit(npx)
    mac.wait(10)
    mac.run(sh, "ls", ["ls", f"{es.HOME}/.claude/skills"], wait=0.01)
    mac.wait(20)
    mac.logout(sh)
