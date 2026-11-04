"""EAA-011 as the technique page describes it: a repository's .mcp.json names
its server's command as `${ACME_MCP_CMD:-npx}`; Claude Code starts in an
environment where something else has set ACME_MCP_CMD, so the approved
project server resolves to an executable that is not the checked-in one."""
import es

CASE = {
    "id": "eaa-011-mcp-command-resolves-to-planted-binary",
    "title": "Claude Code's project MCP server, `${ACME_MCP_CMD:-npx}` in .mcp.json, starts ~/.cache/acme-tools/mcp-run from the environment",
    "flag": True,
    "eaa": ["EAA-011"],
    "group": "tools",
    "grounding": {
        "catalog_case": None,
        "steps": ["EAA-011 technique page: abuse path (a referenced launch-environment value makes an MCP command "
                  "resolve to an attacker-selected executable) and its examples"],
        "sources": ["SRC-015", "https://code.claude.com/docs/en/mcp"],
    },
    "notes": (
        "Technique page (feasible; official documentation only): Claude Code expands ${VAR} and ${VAR:-default} in an "
        ".mcp.json server's command, args, env, url and headers (SRC-015); an attacker who controls a referenced "
        "value makes a repository-shipped config resolve to an attacker-selected executable, after the ordinary "
        "project-server approval. The page documents no way the value is set; the case starts with it already in the "
        "person's shell (exported by a startup file, as the page's hunting notes list among sources) and the "
        "executable already on disk, both prerequisites, not events here. The approval is a keypress. What is "
        "visible: claude's exec, whose environment holds ACME_MCP_CMD, and the server's exec from ~/.cache with the "
        "checked-in args. The repository, variable and executable are made up."
    ),
}

REPO = "~/code/acme-docs"
PLANTED = es.dropped("~/.cache/acme-tools/mcp-run")


def build(mac):
    for f in ("README.md", "package.json", ".mcp.json", "CLAUDE.md", ".git/config"):
        mac.exists(f"{REPO}/{f}", days=20)
    mac.exists(PLANTED.path, size=2_310_448, days=2)
    sh = mac.terminal(cwd=REPO)
    sh.env["ACME_MCP_CMD"] = PLANTED.path  # set by the shell's startup, before this tab's first command
    mac.wait(8)
    agent = es.claude(mac, sh, REPO, key=True)
    mac.read_file(agent, f"{REPO}/.mcp.json")
    mac.wait(6)  # "New MCP server found in .mcp.json: acme-docs": the person approves
    mac.atomic_write(agent, "~/.claude.json")
    server = mac.spawn(agent, PLANTED, [PLANTED.path, "-y", "@acme/docs-mcp"], cwd=REPO, fds="pipe", key=True)
    mac.wait(30)
    es.claude_prompt(mac, agent)
    mac.wait(120)
    mac.exit(server, status=143)
    mac.exit(agent)
    mac.logout(sh)
