"""EAA-006 as Mitiga's proof of concept did it (EAA-C-011, step 1): a
dependency's npm postinstall edits the global ~/.claude.json, pre-trusting
clone locations and pointing an existing MCP server's URL at a local proxy."""
import es

CASE = {
    "id": "eaa-006-postinstall-rewrites-claude-mcp-endpoint",
    "title": "A dependency's postinstall, under the person's npm install, rewrites ~/.claude.json",
    "flag": True,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {
        "catalog_case": "EAA-C-011",
        "steps": [1],
        "sources": ["SRC-024", "https://www.mitiga.io/blog/claude-code-mcp-token-theft-mitm"],
    },
    "notes": (
        "Mitiga: a malicious npm package's postinstall edits the global ~/.claude.json, the file that holds MCP "
        "routing and trust state: it pre-sets trust for common clone locations and rewrites an existing OAuth-backed "
        "MCP server's URL to a localhost proxy. The source does not say who ran the install, so it is the person's "
        "own `npm install` in their terminal; nor how the file was written: a node script's read and truncating "
        "write. Which keys changed is file content, which eslogger does not show; the registry download and the "
        "proxied OAuth traffic (step 3, EAA-015) are network. Step 2 (a project SessionStart hook that reasserts the "
        "URL, EAA-003) is not in this case. acme-logfmt is a made-up package."
    ),
}

WEB = "~/code/acme-web"
PKG = f"{WEB}/node_modules/acme-logfmt"


def build(mac):
    sh = mac.terminal(cwd=WEB)
    mac.wait(7)
    npm = mac.spawn(sh, "npm", ["npm", "install", "acme-logfmt"])
    for f in ("~/.npmrc", f"{WEB}/package.json", f"{WEB}/package-lock.json"):
        mac.read_file(npm, f)
    mac.wait(3)
    for f in ("package.json", "index.js", "scripts/postinstall.js"):
        mac.write_file(npm, f"{PKG}/{f}", new=True)

    script = es.npm_lifecycle(mac, npm, PKG, "node scripts/postinstall.js", version="0.3.2")
    mac.exec(script, "node", ["node", "scripts/postinstall.js"], key=True)
    mac.read_file(script, f"{PKG}/scripts/postinstall.js")
    mac.read_file(script, "~/.claude.json")
    mac.wait(0.05)
    mac.write_file(script, "~/.claude.json", key=True)
    mac.wait(0.1)
    mac.exit(script)

    mac.write_file(npm, f"{WEB}/node_modules/.package-lock.json", new=True)
    mac.write_file(npm, f"{WEB}/package.json")
    mac.write_file(npm, f"{WEB}/package-lock.json")
    mac.exit(npm)
    mac.wait(40)
    mac.logout(sh)
