"""EAA-012 as Bloom Security reports it (EAA-C-013, step 1): a cloned
repository's project .claude/settings.json turns Claude Code's telemetry on,
points its OTLP exporter at a collector the attacker controls and enables
the prompt, tool and raw-body logging options; the person opens Claude Code
in it and works."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-012-cloned-repo-redirects-claude-telemetry",
    "title": "A cloned repository's project .claude/settings.json sends Claude Code's telemetry, prompts included, to a collector",
    "flag": True,
    "eaa": ["EAA-012"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-013",
        "steps": [1],
        "sources": ["SRC-028", "SRC-020", "https://bloom.security/blog/welcome-to-otel-claudeifornia"],
    },
    "notes": (
        "Bloom: a committed project .claude/settings.json (one 21-line file) sets, in its env block, "
        "CLAUDE_CODE_ENABLE_TELEMETRY=1, OTEL_EXPORTER_OTLP_*_ENDPOINT at an attacker-controlled HTTPS host, and "
        "OTEL_LOG_USER_PROMPTS, OTEL_LOG_TOOL_DETAILS, OTEL_LOG_TOOL_CONTENT and OTEL_LOG_RAW_API_BODIES; the "
        "repository arrives by git clone and the person opens Claude Code in it; session and identity data reach "
        "the collector (impact confirmed, medium confidence; Anthropic disputes the characterization). This case is "
        "step 1 alone, without the header helper (step 2 is eaa-012-otel-headers-helper-writes-user-settings). "
        "What eslogger sees: git writing the project settings file (key events) and claude starting in the fresh "
        "clone, reading it, and working. Not visible: the settings' values (file content), the export to the "
        "collector (network). The OTEL_* names are applied from the file inside claude, so they are not in its exec "
        "environment; if Claude Code passes them to its children (the source does not say), its tool shells' execs "
        "would carry them: not modeled. The repository and its host are made up."
    ),
}

REPO = "~/code/lumenfold-dashboard-kit"


def build(mac):
    sh = mac.terminal(cwd="~/code")
    mac.wait(5)
    rs.git_clone(mac, sh, "https://git.example.net/ui-collective/lumenfold-dashboard-kit.git", REPO,
                 ["README.md", "package.json", "src/App.tsx", "src/index.tsx", "tsconfig.json", ".claude/settings.json"],
                 key=(".claude/settings.json",))
    mac.wait(6)
    sh.cwd = mac._p(REPO)
    agent = es.claude(mac, sh, REPO)
    mac.wait(14)
    es.claude_prompt(mac, agent)  # "how is this project laid out?"
    for f in ("package.json", "src/App.tsx", "src/index.tsx"):
        mac.read_file(agent, f"{REPO}/{f}")
    tool = es.agent_shell(mac, agent, "npm ls --depth=0")
    npm = mac.spawn(tool, "npm", ["npm", "ls", "--depth=0"])
    mac.read_file(npm, f"{REPO}/package.json")
    mac.wait(1.5)
    mac.exit(npm)
    mac.exit(tool)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(25)
    es.claude_prompt(mac, agent)
    mac.wait(40)
    mac.exit(agent)
    mac.logout(sh)
