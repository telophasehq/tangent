"""EAA-005 as GitGuardian's ChainDrop deobfuscation describes the artifact
(EAA-C-023, step 2): a malicious release's npm preinstall fetches the Bun
runtime and runs an obfuscated second stage with it, which looks for agent
configuration and state (Cursor, OpenClaw, Codex, OpenCode, Gemini, Hermes)
alongside registry and cloud credentials."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-005-preinstall-bun-stage-reads-agent-configs",
    "title": "A dependency's preinstall fetches Bun and runs a second stage that reads six agents' config and auth files",
    "flag": True,
    "eaa": ["EAA-005"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-023",
        "steps": [2],
        "sources": ["SRC-059", "SRC-058", "https://blog.gitguardian.com/keyv-mini-shai-hulud/",
                    "https://www.microsoft.com/en-us/security/blog/2026/08/04/chaindrop-supply-chain-compromise-anatomy-self-propagating-worm/"],
    },
    "notes": (
        "GitGuardian: the infection is an npm preinstall script that downloads the Bun runtime and uses it to "
        "load a heavily obfuscated JavaScript second stage; the recovered code holds hundreds of hardcoded secret "
        "locations, with product-specific paths for Cursor, OpenClaw, OpenAI Codex, OpenCode, Gemini and Hermes "
        "alongside cloud, developer, browser and registry credentials. Microsoft: the preinstall runs a setup "
        "script with node, which launches the Bun bundle (node is bun's parent); on a developer's machine the "
        "payload starts a detached copy of itself in the background, which continues after the install. The "
        "outcome is `present`: capability in the artifact, no named victim's files collected. Neither source "
        "gives the paths: they are each product's documented configuration and auth files, not the artifact's "
        "list. Where Bun is put and how the detached copy is started are not given: written as an official Bun "
        "build (Oven's signature) unpacked by node under $TMPDIR (the reports' own folder name is not used), and "
        "the copy as the same bun and script in a session of its own, whose parent exits so launchd adopts it. "
        "Two credential reads stand for the cloud and registry set; browser stores are not written. The person "
        "runs `npm install` in their own terminal; the package (kvstash) and its file names (the "
        "loader, bootstrap.mjs, a stand-in for the sources' setup.mjs) are made up. Not "
        "visible: the Bun download and the exfiltration (network). Key events: the bun execs, and the detached "
        "copy's reads of the agent files and the two credentials."
    ),
}

WEB = "~/code/acme-web"
PKG = f"{WEB}/node_modules/kvstash"
BUN_DIR = f"{rs.TMPDIR}/bun-darwin-aarch64"
BUN = es.Program("bun (fetched)", f"{BUN_DIR}/bun", "bun", "7FRXF46ZSN", es.DEVID_TOOL, size=63072928)
AGENT_FILES = [
    "~/.cursor/mcp.json",
    "~/.cursor/cli-config.json",
    "~/.openclaw/openclaw.json",
    "~/.codex/config.toml",
    "~/.codex/auth.json",
    "~/.config/opencode/opencode.json",
    "~/.local/share/opencode/auth.json",
    "~/.gemini/settings.json",
    "~/.gemini/oauth_creds.json",
    "~/.hermes/config.yaml",
]
CREDS = ["~/.npmrc", "~/.aws/credentials"]


def build(mac):
    for f in AGENT_FILES:
        mac.exists(f, days=35)
    sh = mac.terminal(cwd=WEB)
    mac.wait(5)
    npm = mac.spawn(sh, "npm", ["npm", "install", "kvstash"])
    for f in ("~/.npmrc", f"{WEB}/package.json", f"{WEB}/package-lock.json"):
        mac.read_file(npm, f)
    mac.wait(3)
    for f in ("package.json", "index.js", "bootstrap.mjs", "stage.js"):
        mac.write_file(npm, f"{PKG}/{f}", new=True)
    s = es.npm_lifecycle(mac, npm, PKG, "node bootstrap.mjs", event="preinstall", version="3.2.1")
    mac.exec(s, "node", ["node", "bootstrap.mjs"])
    mac.read_file(s, f"{PKG}/bootstrap.mjs")
    mac.wait(2.2)  # the Bun download (network)
    mac.write_file(s, BUN.path, new=True)
    bun = mac.spawn(s, BUN, [BUN.path, "stage.js"], fds="pipe", key=True)
    mac.read_file(bun, f"{PKG}/stage.js")
    mac.wait(0.08)
    copy = mac.spawn(bun, BUN, [BUN.path, "stage.js"], fds="null", setsid=True, key=True)  # the detached copy
    mac.wait(0.005)
    mac.exit(bun)
    mac.exit(s)
    rs.orphan(mac, copy)
    mac.read_file(copy, f"{PKG}/stage.js")
    mac.write_file(npm, f"{WEB}/node_modules/.package-lock.json", new=True)
    mac.write_file(npm, f"{WEB}/package.json")
    mac.write_file(npm, f"{WEB}/package-lock.json")
    mac.exit(npm)
    mac.wait(0.4)
    for f in AGENT_FILES + CREDS:
        mac.read_file(copy, f, key=True)
    mac.wait(1.8)  # exfiltration (network)
    mac.exit(copy)
    mac.wait(8)
    mac.logout(sh)
