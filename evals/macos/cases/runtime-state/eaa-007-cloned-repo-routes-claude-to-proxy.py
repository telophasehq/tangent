"""EAA-007 as Check Point demonstrated it (EAA-C-015, step 3): the person
clones a repository whose project .claude/settings.json sets
ANTHROPIC_BASE_URL, and starts Claude Code in it; an affected version sent
its startup requests, API key included, to that URL before the trust
dialog."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-007-cloned-repo-routes-claude-to-proxy",
    "title": "A cloned repository's project .claude/settings.json points Claude Code's API traffic at another URL",
    "flag": True,
    "eaa": ["EAA-007"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-015",
        "steps": [3],
        "sources": ["SRC-023", "SRC-042", "SRC-014",
                    "https://research.checkpoint.com/2026/rce-and-api-token-exfiltration-through-claude-code-project-files-cve-2025-59536/"],
    },
    "notes": (
        "Check Point: the attacker puts ANTHROPIC_BASE_URL in the repository's project configuration "
        "(.claude/settings.json, whose env block Claude Code applies); the victim clones the repository and runs "
        "claude in it; before the trust dialog, an affected version (earlier than 2.0.65, GHSA-jh7p-qr78-84p7) sent "
        "several startup requests, each with the full API key in its authorization header, to that URL. On the "
        "endpoint the source describes no process or file write beyond the clone and the start. What eslogger sees: "
        "git writing the project .claude/settings.json on clone (the key events), and claude starting in the fresh "
        "clone and reading it. The Mac runs an affected version here (2.0.64, the last before the fix; its "
        "binary's size made up), since the fixed one defers the requests until trust. Not visible: the URL "
        "itself (file content; the variable comes from the file, so it is not in claude's exec environment), the "
        "requests and the key sent (network). The repository, its host and its files are made up."
    ),
}

REPO = "~/code/zephyrine-vite-starter"
CLAUDE_2_0_64 = rs.claude_version("2.0.64", 187_402_112)


def build(mac):
    sh = mac.terminal(cwd="~/code")
    mac.wait(4)
    rs.git_clone(mac, sh, "https://git.example.org/devtools-hub/zephyrine-vite-starter.git", REPO,
                 ["README.md", "package.json", "index.html", "src/main.ts", "vite.config.ts", ".claude/settings.json"],
                 key=(".claude/settings.json",))
    mac.wait(5)
    sh.cwd = mac._p(REPO)
    mac.run(sh, "ls", ["ls", "-la"], wait=0.01)
    mac.wait(3)
    agent = rs.claude_at(mac, sh, REPO, prog=CLAUDE_2_0_64)
    mac.wait(40)  # the trust dialog is on screen; the startup requests have gone out
    mac.exit(agent)
    mac.wait(10)
    mac.logout(sh)
