"""EAA-006 as Socket reported SANDWORM_MODE's McpInject routine (EAA-C-019,
steps 2 and 4): code in a typosquatted npm dependency, running in the
developer's own node process, deploys a local MCP server into a hidden
directory in home and writes server entries into the configuration paths of
several agents."""
import es

CASE = {
    "id": "eaa-006-npm-payload-registers-rogue-mcp-server",
    "title": "A dependency's code in the person's node app drops an MCP server in ~ and writes entries for it into Claude Desktop's, Claude Code's, Cursor's and Windsurf's configs",
    "flag": True,
    "eaa": ["EAA-006", "EAA-014"],
    "group": "tools",
    "grounding": {
        "catalog_case": "EAA-C-019",
        "steps": [2, 4],
        "sources": ["SRC-047", "https://socket.dev/blog/sandworm-mode-npm-worm-ai-toolchain-poisoning",
                    "https://www.kodemsecurity.com/resources/sandworm-mode-a-new-shai-hulud-style-npm-worm-threatening-developer-ai-toolchain-security",
                    "https://lilting.ch/en/articles/npm-supply-chain-worm-sandworm-mode-0224"],
    },
    "notes": (
        "Socket (SANDWORM_MODE, February 2026; its page refused this author's fetch, so the details are from the catalog's "
        "case and from Kodem's and lilting.ch's summaries of it): typosquatted npm packages run on import, not at install; a second "
        "stage, held back 48 to 96 hours on a developer's machine, deploys a rogue MCP server into a hidden directory "
        "in home (Kodem's example is a dot-directory) and writes server entries to the paths it treats as agent "
        "configuration: Claude Desktop's claude_desktop_config.json, Claude Code's ~/.claude/settings.json (which "
        "Claude Code ignores for mcpServers, so the catalog does not count it as a registration), ~/.cursor/mcp.json "
        "and ~/.codeium/windsurf/mcp_config.json; Continue's target is its extension's settings, a path the sources "
        "do not give, so it is left out. The sources do not say which process runs the second stage: it is the node "
        "process of the person's own `npm start`, days after the package was installed. The server's tool "
        "descriptions (step 3, EAA-010) are content; no source shows an agent loading it, so no server process is "
        "started here. Stage one's secret harvesting (EAA-016, EAA-015) is not in this case. The hidden directory's own "
        "creation (a create event for a directory, which es.py does not build) is left out, and the reads before "
        "each write (to merge the JSON) are assumed. The package, the directory and the server's file name (main.js) "
        "are made up (the real ones are IOCs; Kodem's example directory is ~/.dev-utils/, Socket's example server "
        "file server.js)."
    ),
}

WEB = "~/code/acme-web"
PKG = f"{WEB}/node_modules/acme-ansi-palette"
DESKTOP = "~/Library/Application Support/Claude/claude_desktop_config.json"
WINDSURF = "~/.codeium/windsurf/mcp_config.json"
SERVER = "~/.globex-devtools/main.js"


def build(mac):
    for f in ("package.json", "index.js", "lib/palette.js"):
        mac.exists(f"{PKG}/{f}", days=4)
    mac.exists(f"{WEB}/dist/index.js", days=1)
    for f in (DESKTOP, "~/.cursor/mcp.json", WINDSURF):
        mac.exists(f, size=240)
    sh = mac.terminal(cwd=WEB)
    mac.wait(6)
    npm = mac.spawn(sh, "npm", ["npm", "start"])
    mac.read_file(npm, f"{WEB}/package.json")
    env = {"npm_lifecycle_event": "start", "npm_lifecycle_script": "node dist/index.js", "npm_package_name": "acme-web",
           "npm_package_version": "0.9.0", "npm_package_json": mac._p(f"{WEB}/package.json"), "npm_command": "start",
           "npm_config_user_agent": "npm/11.6.0 node/v26.0.0 darwin arm64 workspaces/false", "npm_node_execpath": es.NODE,
           "npm_execpath": "/opt/homebrew/lib/node_modules/npm/bin/npm-cli.js", "NODE": es.NODE, "INIT_CWD": mac._p(WEB),
           "COLOR": "1", "EDITOR": "vi", "PATH": mac._p(f"{WEB}/node_modules/.bin:") + npm.env.get("PATH", "")}
    app = mac.spawn(npm, "sh", ["sh", "-c", "node dist/index.js"], env_add=env)
    mac.exec(app, "node", ["node", "dist/index.js"])  # sh -c with one command execs it in place
    for f in ("dist/index.js", "node_modules/acme-ansi-palette/package.json", "node_modules/acme-ansi-palette/index.js",
              "node_modules/acme-ansi-palette/lib/palette.js"):
        mac.read_file(app, f"{WEB}/{f}")
    mac.wait(1.2)

    # the second stage: the server, then an entry in each agent's configuration
    mac.write_file(app, SERVER, new=True, key=True)
    for cfg in (DESKTOP, "~/.claude/settings.json", "~/.cursor/mcp.json", WINDSURF):
        mac.read_file(app, cfg)
        mac.write_file(app, cfg, key=True)
        mac.wait(0.004)
    mac.wait(600)  # the app runs on; the person stops it
    mac.exit(app, status=130)
    mac.exit(npm, status=130)
    mac.wait(20)
    mac.logout(sh)
