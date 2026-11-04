"""EAA-008 as Dash Security demonstrated it (EAA-C-012, steps 1 and 2): a
script makes an isolated CLAUDE_CONFIG_DIR with workspace trust seeded and
a login the attacker controls, then starts Claude Code from it into a
Remote Control session."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-008-shadow-claude-config-dir-remote-control",
    "title": "A script seeds an isolated CLAUDE_CONFIG_DIR in /tmp and starts claude --remote-control from it",
    "flag": True,
    "eaa": ["EAA-008", "EAA-002"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-012",
        "steps": [1, 2],
        "sources": ["SRC-025", "SRC-014", "SRC-031", "https://dash.security/blog/living-off-coding-agents-claude-as-a-c2-server"],
    },
    "notes": (
        "Dash: with prior local execution, a custom script created an isolated CLAUDE_CONFIG_DIR, installed an "
        "attacker-controlled full-scope Claude.ai login in it and seeded workspace-trust state (step 1, EAA-008), "
        "then launched Claude Code non-interactively into a Remote Control session (step 2, EAA-002; included "
        "because the alternate directory shows only at the launch). The post withholds the directory's path, the "
        "files written, the login's storage, the script's language and the command line beyond `claude "
        "--remote-control`; its demo ran in a visible terminal, and a TCC prompt suggests macOS. Written as an sh "
        "script in /tmp run from a Terminal tab, which makes the directory and writes its .claude.json (where "
        "Claude Code keeps per-project trust state; the directory docs move it under CLAUDE_CONFIG_DIR), then starts "
        "claude with CLAUDE_CONFIG_DIR set. Claude Code is already installed here, so the script's install (\"if not "
        "previously installed\") is not written. The login: the post says an undocumented non-interactive path "
        "accepts a previously issued refresh token through environment variables, and omits their names; none is "
        "written (a name would be invented; where Claude Code then stores the login is not given, and on macOS the "
        "docs keep credentials in the Keychain). Not visible: the Remote Control session and the operator's instructions (network; step 3, "
        "EAA-015). Key events: the script's create, open and close of the seeded .claude.json, and the claude exec "
        "with CLAUDE_CONFIG_DIR."
    ),
}

WORK = "/private/tmp/rc-worker"
CFG = f"{WORK}/profile"


def build(mac):
    mac.exists(f"{WORK}/start.sh", days=0)
    sh = mac.terminal(cwd="~")
    mac.wait(4)
    script = mac.spawn(sh, "sh", ["sh", f"{WORK}/start.sh"])
    mac.read_file(script, f"{WORK}/start.sh")
    mac.run(script, rs.MKDIR, ["mkdir", "-p", CFG], wait=0.004)
    mac.write_file(script, f"{CFG}/.claude.json", new=True, key=True)  # trust state seeded for the working directory
    mac.wait(0.2)
    agent = rs.claude_at(mac, script, es.HOME, CFG, ["claude", "--remote-control"], key=True)
    mac.wait(8)
    mac.write_file(agent, agent.session, new=True)
    mac.wait(30)
    mac.write_file(agent, agent.session, append=True)
    # the worker stays connected to the Remote Control session (network: not visible)
