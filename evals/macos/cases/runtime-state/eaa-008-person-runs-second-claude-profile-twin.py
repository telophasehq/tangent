"""The twin of an EAA-008 shadow profile: the person keeps a second Claude
Code profile for a client's account in ~/.claude-globex and starts it from
their terminal with CLAUDE_CONFIG_DIR."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-008-person-runs-second-claude-profile-twin",
    "title": "The person types CLAUDE_CONFIG_DIR=~/.claude-globex claude for a second account's profile",
    "flag": False,
    "eaa": ["EAA-008"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-014", "SRC-031"],
    },
    "notes": (
        "Plainly benign: CLAUDE_CONFIG_DIR is the documented way to keep a separate configuration and session "
        "history, and hunting.md's H12 notes that legitimate users create alternate profiles. The person, at a "
        "Terminal tab with a tty, works on globex's SDK under the profile they keep for that client in "
        "~/.claude-globex (made in an earlier session, so its settings and .claude.json exist). Claude Code reads "
        "and saves its files there. Key event: the claude exec, whose kept environment carries CLAUDE_CONFIG_DIR."
    ),
}

CFG = "~/.claude-globex"


def build(mac):
    for f in ("settings.json", ".claude.json"):
        mac.exists(f"{CFG}/{f}", days=40)
    sh = mac.terminal(cwd="~/code/globex-sdk")
    mac.wait(5)
    agent = rs.claude_at(mac, sh, "~/code/globex-sdk", CFG, key=True)
    mac.wait(10)
    mac.write_file(agent, agent.session, new=True)
    mac.wait(8)
    mac.read_file(agent, "~/code/globex-sdk/pyproject.toml")
    mac.wait(25)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(30)
    mac.atomic_write(agent, f"{CFG}/.claude.json")
    mac.exit(agent)
    mac.logout(sh)
