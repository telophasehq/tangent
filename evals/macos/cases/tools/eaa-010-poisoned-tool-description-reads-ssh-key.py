"""EAA-010 as Invariant Labs' direct-poisoning experiment did it (EAA-C-009,
step 2): a connected MCP server's `add` tool carries instructions in its
description; when the person asks Cursor to add two numbers, its agent reads
~/.cursor/mcp.json and ~/.ssh/id_rsa.pub to pass them to the tool."""
import es

CASE = {
    "id": "eaa-010-poisoned-tool-description-reads-ssh-key",
    "title": "Cursor's agent, steered by an MCP tool's description, reads ~/.cursor/mcp.json and ~/.ssh/id_rsa.pub",
    "flag": True,
    "eaa": ["EAA-010"],
    "group": "tools",
    "grounding": {
        "catalog_case": "EAA-C-009",
        "steps": [2],
        "sources": ["SRC-026", "https://github.com/invariantlabs-ai/mcp-injection-experiments"],
    },
    "notes": (
        "Invariant Labs, direct-poisoning.py, run in Cursor: an MCP server whose `add` tool description tells the "
        "agent, before using the tool, to read ~/.cursor/mcp.json and ~/.ssh/id_rsa.pub and pass their contents as a "
        "side argument. The description and the tool call are MCP traffic over stdio and are not visible; the two "
        "reads are. The server is a Python FastMCP script; how Cursor's mcp.json starts it is not in the source "
        "(Homebrew python3 on the script here), nor which Cursor process reads files for its agent (the extension "
        "host here, not verified). The server was connected earlier, a prerequisite; its path is made up."
    ),
}

SERVER = "~/code/globex-mcp-tools/server.py"


def build(mac):
    mac.exists("~/.cursor/mcp.json", size=210)
    mac.exists("~/.ssh/id_rsa.pub", size=568)
    mac.exists(SERVER, size=900)
    cursor = mac.app("Cursor")
    mac.wait(1)  # a new window: its extension host starts, and with it the configured MCP servers
    host = mac.spawn(cursor, "Cursor Helper (Plugin)", [es.PROGRAMS["Cursor Helper (Plugin)"].path, "--type=utility",
                     "--utility-sub-type=node.mojom.NodeService", "--lang=en-US", "--service-sandbox-type=none"], cwd="/")
    mac.read_file(host, "~/.cursor/mcp.json")
    server = mac.spawn(host, "python", ["python3", mac._p(SERVER)], cwd="/", fds="pipe")
    mac.read_file(server, SERVER)
    mac.wait(70)  # the person asks the agent to add two numbers; the tool's description is already in its context
    mac.read_file(host, "~/.cursor/mcp.json", key=True)
    mac.wait(0.8)
    mac.read_file(host, "~/.ssh/id_rsa.pub", key=True)
    mac.wait(4)  # the agent calls `add` with the contents: stdio
    mac.wait(200)
    mac.exit(server)
