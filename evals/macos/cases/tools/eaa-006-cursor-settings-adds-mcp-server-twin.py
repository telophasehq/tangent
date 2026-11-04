"""The control for a write to ~/.cursor/mcp.json: the person adds an MCP
server from Cursor's settings, which opens ~/.cursor/mcp.json in Cursor's
editor; they save it, and Cursor starts the new server."""
import es

CASE = {
    "id": "eaa-006-cursor-settings-adds-mcp-server-twin",
    "title": "The person adds an MCP server in Cursor's settings; Cursor saves ~/.cursor/mcp.json and starts it",
    "flag": False,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-029", "SRC-077"]},
    "notes": (
        "Plainly benign: the rightful user edits Cursor's global MCP configuration (the file SRC-029 names) in Cursor "
        "itself and saves it. Which Cursor process saves an edited file, and which starts MCP servers, is not seen: "
        "the app and its extension host here. The server is the person's own, started with node from its checkout. "
        "Key events: the save of ~/.cursor/mcp.json and the server's start. Names are made up."
    ),
}

SERVER = "~/code/acme-lint-mcp/dist/index.js"


def build(mac):
    mac.exists("~/.cursor/mcp.json", size=150)
    mac.exists(SERVER, size=5200, days=2)
    cursor = mac.app("Cursor")
    host = mac.spawn(cursor, "Cursor Helper (Plugin)", [es.PROGRAMS["Cursor Helper (Plugin)"].path, "--type=utility",
                     "--utility-sub-type=node.mojom.NodeService", "--lang=en-US", "--service-sandbox-type=none"], cwd="/")
    mac.read_file(host, "~/.cursor/mcp.json")
    mac.wait(30)  # Settings, MCP, "Add new global MCP server": the file opens in an editor tab
    mac.read_file(cursor, "~/.cursor/mcp.json")
    mac.wait(55)  # the person types the entry and saves
    mac.write_file(cursor, "~/.cursor/mcp.json", key=True)
    mac.wait(0.8)
    mac.read_file(host, "~/.cursor/mcp.json")
    server = mac.spawn(host, "node", ["node", mac._p(SERVER)], cwd="/", fds="pipe", key=True)
    mac.read_file(server, SERVER)
    mac.wait(240)
    mac.exit(server, status=143)
