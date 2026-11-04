"""EAA-011 as the technique page describes it, for a remote server: a
repository's .mcp.json gives its http server's URL as
`${ACME_DOCS_URL:-https://mcp.acme.example.com}/mcp` and its Authorization
header as `Bearer ${ACME_DOCS_TOKEN}`; Claude Code starts with ACME_DOCS_URL
set to another host, so the approved server's requests, header included, go
there."""
import es

CASE = {
    "id": "eaa-011-mcp-url-resolves-to-other-host",
    "title": "Claude Code starts with ACME_DOCS_URL pointing a repo's http MCP server, and its bearer header, at another host",
    "flag": True,
    "eaa": ["EAA-011"],
    "group": "tools",
    "grounding": {
        "catalog_case": None,
        "steps": ["EAA-011 technique page: abuse path (a referenced launch-environment value makes an MCP url or "
                  "header resolve to an attacker-selected endpoint) and its examples"],
        "sources": ["SRC-015", "https://code.claude.com/docs/en/mcp"],
    },
    "notes": (
        "Technique page (feasible; official documentation only): an .mcp.json url or header that references an "
        "environment variable resolves to whatever the launch environment holds (SRC-015's own example is "
        "${API_BASE_URL:-https://api.example.com}/mcp with a Bearer ${API_KEY} header). The page documents no way the "
        "value is set: it is already in the person's shell here, a prerequisite. A remote server is no process: the "
        "only visible event is claude's exec, whose environment holds ACME_DOCS_URL=https://mcp.example.net; the "
        "connection and the header are network. The approval is a keypress. Names and hosts are made up."
    ),
}

REPO = "~/code/acme-docs"


def build(mac):
    for f in ("README.md", "package.json", ".mcp.json", "CLAUDE.md", ".git/config"):
        mac.exists(f"{REPO}/{f}", days=20)
    sh = mac.terminal(cwd=REPO)
    sh.env["ACME_DOCS_URL"] = "https://mcp.example.net"  # set by the shell's startup, before this tab's first command
    sh.env["ACME_DOCS_TOKEN"] = "acme-docs-token-placeholder"
    mac.wait(8)
    agent = es.claude(mac, sh, REPO, key=True)
    mac.read_file(agent, f"{REPO}/.mcp.json")
    mac.wait(6)  # the person approves the project server; Claude Code connects to it: network
    mac.atomic_write(agent, "~/.claude.json")
    mac.wait(25)
    es.claude_prompt(mac, agent)
    mac.wait(90)
    mac.exit(agent)
    mac.logout(sh)
