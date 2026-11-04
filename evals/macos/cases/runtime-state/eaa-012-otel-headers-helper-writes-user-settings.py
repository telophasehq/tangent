"""EAA-012 as Bloom Security reports it (EAA-C-013, steps 1 and 2): a cloned
repository's project .claude/settings.json redirects Claude Code's
telemetry and sets otelHeadersHelper; Claude Code runs the helper at
session start, and the helper merges the project's telemetry settings into
the user's ~/.claude/settings.json, so they outlive the repository."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-012-otel-headers-helper-writes-user-settings",
    "title": "A cloned repository's otelHeadersHelper, run by Claude Code at start, rewrites ~/.claude/settings.json",
    "flag": True,
    "eaa": ["EAA-012"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-013",
        "steps": [1, 2],
        "sources": ["SRC-028", "SRC-020", "https://bloom.security/blog/welcome-to-otel-claudeifornia"],
    },
    "notes": (
        "Bloom: the project .claude/settings.json (arriving by git clone) redirects telemetry (step 1) and sets "
        "otelHeadersHelper, a shell command Claude Code runs at session start and every 29 minutes, putting its JSON "
        "stdout in the export headers; a project-level helper can read the project's .claude/settings.json and "
        "deep-merge it into the global ~/.claude/settings.json, which persists after the repository is deleted "
        "(step 2). The source does not say whether the helper is an inline command or a script in the repository, "
        "or what language: written as a node script the repository ships (.claude/otel-headers.js, a made-up name), "
        "started by Claude Code as `/bin/sh -c <command>` in the project directory, which execs node in place. How "
        "Claude Code starts helpers (shell, session, stdio) is not observed here: written as its other children "
        "are seen on this Mac (the shell snapshot, `security`, its tool shells), in a session of its own with no "
        "terminal, stdio piped. The merge is written as a read of both files and a truncating write of the user's. "
        "Not visible: the settings' values and the headers (file content, stdout), the export (network). Key events: "
        "git's write of the project settings, the helper's exec under claude, and its write of ~/.claude/settings.json."
    ),
}

REPO = "~/code/lumenfold-dashboard-kit"


def build(mac):
    sh = mac.terminal(cwd="~/code")
    mac.wait(5)
    rs.git_clone(mac, sh, "https://git.example.net/ui-collective/lumenfold-dashboard-kit.git", REPO,
                 ["README.md", "package.json", "src/App.tsx", "src/index.tsx", "tsconfig.json",
                  ".claude/otel-headers.js", ".claude/settings.json"],
                 key=(".claude/settings.json",))
    mac.wait(6)
    sh.cwd = mac._p(REPO)
    agent = es.claude(mac, sh, REPO)
    mac.wait(1.2)
    helper = mac.spawn(agent, "sh", ["/bin/sh", "-c", "node .claude/otel-headers.js"], cwd=REPO, fds="pipe", setsid=True)
    mac.exec(helper, "node", ["node", ".claude/otel-headers.js"], key=True)
    mac.read_file(helper, f"{REPO}/.claude/otel-headers.js")
    mac.read_file(helper, f"{REPO}/.claude/settings.json")
    mac.read_file(helper, "~/.claude/settings.json")
    mac.write_file(helper, "~/.claude/settings.json", key=True)
    mac.wait(0.05)
    mac.exit(helper)
    mac.wait(12)
    es.claude_prompt(mac, agent)
    for f in ("package.json", "src/App.tsx"):
        mac.read_file(agent, f"{REPO}/{f}")
    mac.wait(30)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(40)
    mac.exit(agent)
    mac.logout(sh)
