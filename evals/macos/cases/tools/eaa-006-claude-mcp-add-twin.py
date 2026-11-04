"""The control for an MCP registration: the person adds two servers with
Claude Code's own `claude mcp add`, typed in their shell, one at user scope
(written to ~/.claude.json) and one at project scope (a new .mcp.json)."""
import es

CASE = {
    "id": "eaa-006-claude-mcp-add-twin",
    "title": "The person runs `claude mcp add` twice: ~/.claude.json and a project .mcp.json written by Claude Code",
    "flag": False,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-015"]},
    "notes": (
        "Plainly benign: the rightful user registers MCP servers through the agent's own command, as its docs give "
        "it (SRC-015): `--scope user` is stored in ~/.claude.json, `--scope project` in .mcp.json at the project root, "
        "which the command creates. "
        "Claude Code writes ~/.claude.json as a tmp file renamed over (as seen on this Mac); how it writes .mcp.json "
        "is not seen: a plain write of a new file here. Key events: the rename over ~/.claude.json and the .mcp.json "
        "create, write and close. The servers are made up."
    ),
}

WEB = "~/code/acme-web"


def build(mac):
    sh = mac.terminal(cwd=WEB)
    mac.wait(6)
    c = mac.spawn(sh, "claude", ["claude", "mcp", "add", "--scope", "user", "--transport", "http", "acme-docs",
                                 "https://mcp.acme.example.com/mcp"])
    mac.read_file(c, "~/.claude/settings.json")
    mac.read_file(c, "~/.claude.json")
    mac.atomic_write(c, "~/.claude.json", key=True)
    mac.wait(0.2)
    mac.exit(c)
    mac.wait(14)
    c = mac.spawn(sh, "claude", ["claude", "mcp", "add", "--scope", "project", "--transport", "stdio", "acme-lint",
                                 "--", "npx", "-y", "@acme/lint-mcp"])
    mac.read_file(c, "~/.claude/settings.json")
    mac.read_file(c, "~/.claude.json")
    mac.write_file(c, f"{WEB}/.mcp.json", new=True, key=True)
    mac.wait(0.2)
    mac.exit(c)
    mac.wait(9)
    mac.run(sh, "git", ["git", "status", "--short"], wait=0.06)
    mac.wait(20)
    mac.logout(sh)
