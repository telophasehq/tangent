"""The twin of eaa-001-editor-extension-spawns-agents-detached: Cursor's own
agent-worker extension, as its extension host starts, launches cursor-agent's
background worker for the open workspace, detached and with no terminal, as
it does on the Mac this pack was built on."""
import es

CASE = {
    "id": "eaa-001-editor-extension-spawns-agents-detached-twin",
    "title": "Cursor's extension host starts Cursor's own cursor-agent worker for the workspace, detached, no tty",
    "flag": False,
    "eaa": ["EAA-001"],
    "group": "launcher",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-004"],
    },
    "notes": (
        "Plainly benign: the vendor's own extension (Cursor's anysphere.cursor-agent-worker, installed with Cursor) "
        "starting the vendor's agent. Seen on the build Mac (ps, not a capture): a cursor-agent worker started in "
        "the same second as Cursor's extension host, no tty, launchd its parent by then, running Cursor's bundled "
        "node (signed `node`, team HX7739G8FX) with argv[0] the worker's bin/cursor-agent and the arguments "
        "`--use-system-ca <versions/.../index.js> ... worker start --worker-dir <workspace> --name <workspace> @ "
        "<host>`; the launcher is a `#!/usr/bin/env bash` script that execs node under its own name. The chain "
        "from the extension host (a detached spawn of that script) is inferred, not seen. Left out of the "
        "arguments: an --api-key with the account's key and an --endpoint with Cursor's host, which the real "
        "command line carries (no real secrets or hosts here), and its --label flags. The same editor and the "
        "same extension host as the attack case; the extension and the agent are Cursor's own, and the agent's "
        "arguments carry no prompt and no approval bypass. Key event: the worker's node exec."
    ),
}

WS = "/Users/dev/code/acme-api"
AGENT = f"{es.HOME}/Library/Application Support/Cursor/User/globalStorage/anysphere.cursor-agent-worker/agent-cli"
VER = f"{AGENT}/.local/share/cursor-agent/versions/2026.10.22-6f1b2c4"
LAUNCHER = es.Program("cursor-agent-worker", f"{VER}/cursor-agent", dyld=f"{AGENT}/.local/bin/cursor-agent",
                      interp=("env", ["bash"]), size=1604)
NODE = es.Program("cursor-agent-worker-node", f"{VER}/node", "node", "HX7739G8FX", es.DEVID, size=112718208)
HOST = es.PROGRAMS["Cursor Helper (Plugin)"]


def build(mac):
    cursor = mac.app("Cursor")
    mac.wait(3)
    host = mac.spawn(cursor, HOST, [HOST.path, "--type=utility", "--utility-sub-type=node.mojom.NodeService",
                                    "--lang=en-US", "--service-sandbox-type=none",
                                    "--user-data-dir=/Users/dev/Library/Application Support/Cursor"],
                     env_add={"PATH": es._LOGIN_PATH, "VSCODE_CWD": "/"}, cwd=WS)
    mac.wait(0.4)
    args = ["worker", "start", "--worker-dir", WS, "--name", f"{WS} @ dev's Mac"]
    w = mac.spawn(host, LAUNCHER, [LAUNCHER.dyld] + args, cwd=WS, setsid=True, fds="null")
    mac.exec(w, NODE, [LAUNCHER.dyld, "--use-system-ca", f"{VER}/index.js"] + args,
             env_add={"CURSOR_INVOKED_AS": "cursor-agent"}, key=True)
    mac.read_file(w, f"{VER}/index.js")
    # the worker stays up for as long as Cursor does
