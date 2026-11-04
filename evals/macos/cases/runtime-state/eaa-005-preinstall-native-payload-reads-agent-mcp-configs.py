"""EAA-005 as Socket's analysis of the Jscrambler npm compromise describes
the artifact (EAA-C-017, step 2): a malicious release's preinstall runs a
bundled native payload (macOS arm64), a credential stealer whose selectors
collect agent and MCP configuration for Claude Desktop, Cursor, Windsurf,
Factory, Zed, VS Code and opencode."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-005-preinstall-native-payload-reads-agent-mcp-configs",
    "title": "A dependency's preinstall drops a native binary that reads seven agents' and editors' MCP configuration",
    "flag": True,
    "eaa": ["EAA-005"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-017",
        "steps": [2],
        "sources": ["SRC-043", "SRC-044", "https://socket.dev/blog/jscrambler-supply-chain-attack",
                    "https://www.stepsecurity.io/blog/jscrambler-npm-package-publishes-malicious-preinstall-binary"],
    },
    "notes": (
        "The catalog (Socket, SRC-043; the vendor advisory, SRC-044): releases 8.14, 8.16 and 8.17 were "
        "preinstall-triggered; the embedded native payload (bundled for macOS arm64 among others) is a credential "
        "stealer with product-specific paths and selectors for Claude Desktop, Cursor, Windsurf, Factory, Zed, "
        "VS Code, opencode and MCP configuration, which can hold API keys and server credentials; upload was "
        "implemented separately. The outcome is `present`: capability in the artifact, no confirmed victim. How it "
        "starts is StepSecurity's analysis of the same release: the preinstall hook runs a loader script in "
        "dist/, which reads one container file in the package (an entry per platform, gzip-compressed), picks "
        "the entry for process.platform, writes it decompressed to a randomly named dotfile in the OS temp "
        "directory, sets its executable bit (no event here) and spawns it detached and unref'd, so it keeps "
        "running after npm install exits: written as node exiting right after the spawn and the payload's reads "
        "coming after, once launchd is its parent. Socket's page could not be fetched here, so the exact paths "
        "are each product's documented configuration file on macOS, not the artifact's selectors. The stealer's "
        "other targets (browser stores, wallets, cloud and registry credentials) are not written, so the case "
        "holds the agent-configuration procedure alone. The person runs `npm install` in their own terminal. The "
        "package (codeshield-cli), its file names (lib/install.js, lib/index.js, lib/assets.dat: stand-ins for "
        "StepSecurity's dist/ loader, Gen 2 loader and container) and the dropped file's name are made up. Not visible: the "
        "upload (network). Key events: the payload's exec and its reads of the configuration files."
    ),
}

WEB = "~/code/acme-web"
PKG = f"{WEB}/node_modules/codeshield-cli"
DROP = f"{rs.TMPDIR}/.cs-4f1c9a"
TARGETS = [
    "~/Library/Application Support/Claude/claude_desktop_config.json",
    "~/.cursor/mcp.json",
    "~/.codeium/windsurf/mcp_config.json",
    "~/.factory/mcp.json",
    "~/.config/zed/settings.json",
    "~/Library/Application Support/Code/User/mcp.json",
    "~/Library/Application Support/Code/User/settings.json",
    "~/.config/opencode/opencode.json",
    "~/.claude.json",
]


def build(mac):
    for f in TARGETS:
        mac.exists(f, days=50)
    sh = mac.terminal(cwd=WEB)
    mac.wait(5)
    npm = mac.spawn(sh, "npm", ["npm", "install", "--save-dev", "codeshield-cli"])
    for f in ("~/.npmrc", f"{WEB}/package.json", f"{WEB}/package-lock.json"):
        mac.read_file(npm, f)
    mac.wait(3)
    for f in ("package.json", "lib/index.js", "lib/install.js", "lib/assets.dat"):
        mac.write_file(npm, f"{PKG}/{f}", new=True)
    s = es.npm_lifecycle(mac, npm, PKG, "node lib/install.js", event="preinstall", version="2.4.0")
    mac.exec(s, "node", ["node", "lib/install.js"])
    mac.read_file(s, f"{PKG}/lib/install.js")
    mac.read_file(s, f"{PKG}/lib/assets.dat")
    mac.write_file(s, DROP, new=True)
    payload = mac.spawn(s, es.dropped(DROP), [DROP], fds="null", setsid=True, key=True)  # detached, unref'd
    mac.wait(0.004)
    mac.exit(s)  # the loader is done; the payload outlives it
    rs.orphan(mac, payload)
    mac.wait(0.03)
    for f in TARGETS:
        mac.read_file(payload, f, key=True)
    mac.write_file(npm, f"{WEB}/node_modules/.package-lock.json", new=True)
    mac.write_file(npm, f"{WEB}/package.json")
    mac.write_file(npm, f"{WEB}/package-lock.json")
    mac.exit(npm)
    mac.wait(1.5)  # the upload (network)
    mac.exit(payload)
    mac.wait(8)
    mac.logout(sh)
