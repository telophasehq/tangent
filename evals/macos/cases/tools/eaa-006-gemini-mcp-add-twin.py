"""The control for a write to a project's .gemini/settings.json: the person
adds an MCP server to the project with Gemini CLI's own `gemini mcp add`,
typed in their shell."""
import es

CASE = {
    "id": "eaa-006-gemini-mcp-add-twin",
    "title": "The person runs `gemini mcp add --scope project`; Gemini CLI writes the project's .gemini/settings.json",
    "flag": False,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-036"]},
    "notes": (
        "Plainly benign: Gemini CLI keeps its MCP servers under mcpServers in settings.json (SRC-036) and its "
        "`gemini mcp add` command writes them, user or project scope (`--` before the server's own flags, as the docs "
        "give it). How it writes the file is not seen: a plain write of a new file here; Gemini CLI is a node script, "
        "so the writer is node. Key events: the create, write and close of .gemini/settings.json. The server is "
        "made up."
    ),
}

API = "~/code/acme-api"


def build(mac):
    sh = mac.terminal(cwd=API)
    mac.wait(6)
    g = mac.spawn(sh, "gemini", ["gemini", "mcp", "add", "--scope", "project", "acme-lint", "npx", "--", "-y", "@acme/lint-mcp"])
    mac.read_file(g, "~/.gemini/settings.json")
    mac.write_file(g, f"{API}/.gemini/settings.json", new=True, key=True)
    mac.wait(0.2)
    mac.exit(g)
    mac.wait(10)
    mac.run(sh, "git", ["git", "status", "--short"], wait=0.06)
    mac.wait(20)
    mac.logout(sh)
