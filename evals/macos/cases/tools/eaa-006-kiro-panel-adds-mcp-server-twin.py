"""The control for a write to ~/.kiro/settings/mcp.json: the person opens the
user MCP config from Kiro's MCP panel, adds a server, saves, and Kiro
reloads it and starts the server."""
import es

CASE = {
    "id": "eaa-006-kiro-panel-adds-mcp-server-twin",
    "title": "The person adds an MCP server from Kiro's MCP panel; Kiro saves ~/.kiro/settings/mcp.json and starts it",
    "flag": False,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-061"]},
    "notes": (
        "Plainly benign: the rightful user edits Kiro's user-level MCP configuration (the file SRC-061 names) in Kiro "
        "and saves; Kiro reloads it on change, as SRC-061 describes, and starts the server with npx. Kiro's app paths "
        "and signature are not verified, nor which of its processes saves and spawns (the extension host here). "
        "Key events: the save and the server's start. Names are made up."
    ),
}

TEAM = "KIRO000000"  # not verified: a placeholder for Kiro's Developer ID team
KIRO = es.Program("Kiro", "/Applications/Kiro.app/Contents/MacOS/Electron", "dev.kiro.desktop", TEAM, es.DEVID_APP)
HELPER = es.Program("Kiro Helper (Plugin)",
                    "/Applications/Kiro.app/Contents/Frameworks/Kiro Helper (Plugin).app/Contents/MacOS/Kiro Helper (Plugin)",
                    "com.github.Electron.helper", TEAM, es.DEVID_APP)
KIRO = es.PROGRAMS.setdefault("Kiro", KIRO)  # mac.app() takes a name from PROGRAMS; es.py has no Kiro
NPX_DIR = f"{es.HOME}/.npm/_npx/9b1e04c6a2d37f58"
SEARCH_MCP = es.Program("acme-search-mcp", f"{NPX_DIR}/node_modules/@acme/search-mcp/bin/server.js",
                        dyld=f"{NPX_DIR}/node_modules/.bin/search-mcp", interp=("env", ["node"]), size=3300)


def build(mac):
    mac.exists("~/.kiro/settings/mcp.json", size=96)
    kiro = mac.app("Kiro")
    host = mac.spawn(kiro, HELPER, [HELPER.path, "--type=utility", "--utility-sub-type=node.mojom.NodeService",
                                    "--lang=en-US", "--service-sandbox-type=none"], cwd="/")
    mac.read_file(host, "~/.kiro/settings/mcp.json")
    mac.wait(25)  # MCP panel, "Open user config"
    mac.read_file(host, "~/.kiro/settings/mcp.json")
    mac.wait(70)  # the person adds acme-search and saves
    mac.write_file(host, "~/.kiro/settings/mcp.json", key=True)
    mac.wait(0.6)
    mac.read_file(host, "~/.kiro/settings/mcp.json")
    npx = mac.spawn(host, "npx", ["npx", "-y", "@acme/search-mcp"], cwd="/", fds="pipe")
    mac.read_file(npx, "~/.npmrc")
    run = mac.spawn(npx, "sh", ["sh", "-c", "search-mcp"], fds="pipe",
                    env_add={"PATH": f"{NPX_DIR}/node_modules/.bin:" + npx.env.get("PATH", "")})
    mac.exec(run, SEARCH_MCP, ["search-mcp"], key=True)
    mac.wait(240)
    mac.exit(run, status=143)
    mac.exit(npx, status=143)
