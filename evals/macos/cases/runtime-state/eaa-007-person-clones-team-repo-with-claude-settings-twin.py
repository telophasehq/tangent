"""The twin of a cloned repository carrying project settings (EAA-007,
EAA-012): the person clones their own company's repository, whose
committed .claude/settings.json is the team's shared project settings, and
starts Claude Code in it."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-007-person-clones-team-repo-with-claude-settings-twin",
    "title": "The person clones acme's own repository, with the team's committed .claude/settings.json, and starts Claude Code in it",
    "flag": False,
    "eaa": ["EAA-007", "EAA-012"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-016"],
    },
    "notes": (
        "Plainly benign: the settings documentation describes .claude/settings.json as project settings checked "
        "into source control and shared with the team. The person clones acme's documentation repository from the "
        "company's git host (made up) and starts claude in it. Its events are the attack cases' events (git writes "
        "a project .claude/settings.json, claude starts in the fresh clone): eslogger shows no file content, so "
        "only the repository's host and name differ. Key events: git's create, open and close of the project "
        ".claude/settings.json."
    ),
}

REPO = "~/code/acme-docs"


def build(mac):
    sh = mac.terminal(cwd="~/code")
    mac.wait(5)
    rs.git_clone(mac, sh, "https://git.acme.example.com/acme/acme-docs.git", REPO,
                 ["README.md", "CLAUDE.md", "package.json", "docs/index.md", "docs/api.md", ".claude/settings.json"],
                 key=(".claude/settings.json",))
    mac.wait(4)
    sh.cwd = mac._p(REPO)
    agent = es.claude(mac, sh, REPO)
    mac.wait(15)
    es.claude_prompt(mac, agent)
    mac.wait(4)
    mac.read_file(agent, f"{REPO}/docs/api.md")
    mac.wait(30)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(20)
    mac.exit(agent)
    mac.logout(sh)
