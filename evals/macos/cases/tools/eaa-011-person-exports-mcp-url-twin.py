"""The control for an MCP variable in Claude Code's environment: the person
adds an `export ACME_DOCS_URL=...` for the team's staging docs server to
~/.zshrc in vim, opens a new tab, and starts Claude Code, whose project
.mcp.json reads that variable."""
import es

CASE = {
    "id": "eaa-011-person-exports-mcp-url-twin",
    "title": "The person exports ACME_DOCS_URL in ~/.zshrc (vim), then starts Claude Code in the repo whose .mcp.json reads it",
    "flag": False,
    "eaa": ["EAA-011"],
    "group": "tools",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-015"]},
    "notes": (
        "Plainly benign: environment expansion in .mcp.json exists so each person can supply their own value "
        "(SRC-015); the technique page says expansion and a checked-in placeholder are not malicious by themselves. "
        "The rightful user sets the variable in their own startup file, in their editor, to their team's staging "
        "server, and starts the agent from a terminal. Key events: the save of ~/.zshrc and claude's exec. Names and "
        "hosts are made up."
    ),
}

REPO = "~/code/acme-docs"


def build(mac):
    for f in ("README.md", "package.json", ".mcp.json", "CLAUDE.md", ".git/config"):
        mac.exists(f"{REPO}/{f}", days=20)
    sh = mac.terminal()
    mac.wait(5)
    es.vim_edit(mac, sh, "~/.zshrc", saves=1, key=True)
    mac.wait(6)
    mac.logout(sh)

    mac.wait(3)
    sh = mac.terminal(cwd=REPO)
    sh.env["ACME_DOCS_URL"] = "https://mcp-staging.acme.example.com"  # the new export, from ~/.zshrc
    mac.wait(7)
    agent = es.claude(mac, sh, REPO, key=True)
    mac.read_file(agent, f"{REPO}/.mcp.json")
    mac.wait(20)
    es.claude_prompt(mac, agent)
    mac.wait(90)
    mac.exit(agent)
    mac.logout(sh)
