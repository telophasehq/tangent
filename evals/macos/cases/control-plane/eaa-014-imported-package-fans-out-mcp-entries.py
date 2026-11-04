"""EAA-014 (with EAA-006's deployment) as SANDWORM_MODE's payload carried it
(EAA-C-019, steps 2 and 4): a dependency's loader runs when the person's dev
server imports it; past its 48-96 hour gate, Stage 2 is written to a hidden
temp file, required and deleted, writes a rogue MCP server under a hidden
directory in the home, and adds an entry for it to five tools' MCP
configuration in one pass."""
import es

CASE = {
    "id": "eaa-014-imported-package-fans-out-mcp-entries",
    "title": "A dependency imported by npm run dev writes an MCP server into ~/.<name>/ and adds it to five tools' configs",
    "flag": True,
    "eaa": ["EAA-014", "EAA-006"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-019",
        "steps": [2, 4],
        "sources": ["SRC-047", "https://socket.dev/blog/sandworm-mode-npm-worm-ai-toolchain-poisoning",
                    "https://k-reel.github.io/sandworm-mode-npm-worm-ai-toolchain-poisoning"],
    },
    "notes": (
        "Socket (read through a mirror; the original refused the fetch): the loader runs at import, not install; Stage "
        "1 waits 5-30 s and harvests with file reads only, .npmrc's auth tokens among them, before the gate; Stage 2 "
        "runs only once 48-96 h have passed since the package directory's mtime; Stage 2 is written "
        "to a transient .node_<hex>.js in /dev/shm or the OS temp directory, required and deleted; it writes server.js "
        "under a hidden home directory with a generated name (e.g. ~/.dev-utils/; no other file is named) and injects "
        "an mcpServers entry for it into every assistant config it finds on disk: ~/.claude/settings.json, Claude "
        "Desktop's config, ~/.cursor/mcp.json, ~/.continue/config.json and ~/.windsurf/mcp.json (another write-up "
        "gives ~/.codeium/windsurf/mcp_config.json). Claude Desktop's macOS path is its documented one. Chosen: the "
        "import is the person's `npm run dev`; the package was installed four days earlier; all five configs exist; "
        "each is read, then written plainly; the generated directory is ~/.build-utils; the stage file's and the "
        "server's names in the events are stand-ins, .mod-<hex>.js and main.js; Stage 1's other reads "
        "(hardhat.config, foundry.toml, .env files) find nothing in acme-web and are not written. The catalog "
        "has the routine present, not seen completing on a victim. Key events: the five config writes. "
        "acme-ansi-palette is made up."
    ),
}

WEB = "~/code/acme-web"
PKG = f"{WEB}/node_modules/acme-ansi-palette"
TMPDIR = "/private/var/folders/7x/k2m9q4z51b3c0000gn/T"
CONFIGS = ["~/.claude/settings.json", "~/Library/Application Support/Claude/claude_desktop_config.json",
           "~/.cursor/mcp.json", "~/.continue/config.json", "~/.windsurf/mcp.json"]


def build(mac):
    for f in ("package.json", "index.js"):
        mac.exists(f"{PKG}/{f}", days=4)
    for f in ("~/Library/Application Support/Claude/claude_desktop_config.json", "~/.cursor/mcp.json",
              "~/.continue/config.json", "~/.windsurf/mcp.json", f"{WEB}/scripts/dev.js"):
        mac.exists(f, days=40)
    sh = mac.terminal(cwd=WEB)
    mac.wait(5)
    npm = mac.spawn(sh, "npm", ["npm", "run", "dev"])
    mac.read_file(npm, f"{WEB}/package.json")
    s = mac.spawn(npm, "sh", ["sh", "-c", "node scripts/dev.js"], cwd=WEB, fds="pipe",
                  env_add={"npm_lifecycle_event": "dev", "npm_lifecycle_script": "node scripts/dev.js",
                           "npm_package_name": "acme-web", "npm_command": "run-script", "INIT_CWD": mac._p(WEB)})
    mac.exec(s, "node", ["node", "scripts/dev.js"])
    mac.read_file(s, f"{WEB}/scripts/dev.js")
    mac.read_file(s, f"{PKG}/package.json")
    mac.read_file(s, f"{PKG}/index.js")
    mac.wait(17)  # Stage 1's jitter
    mac.read_file(s, "~/.npmrc")  # Stage 1's harvest, before the time gate
    mac.wait(0.2)

    stage = f"{TMPDIR}/.mod-9c2e71f04ab3.js"
    mac.write_file(s, stage, new=True)
    mac.read_file(s, stage)
    mac.unlink(s, stage)
    mac.write_file(s, "~/.build-utils/main.js", new=True)
    for cfg in CONFIGS:
        if mac._p(cfg) in mac.files:
            mac.read_file(s, cfg)
        mac.write_file(s, cfg, key=True)
        mac.wait(0.004)

    mac.wait(300)  # the dev server keeps running
    mac.exit(s, status=130)
    mac.exit(npm, status=130)
    mac.logout(sh)
