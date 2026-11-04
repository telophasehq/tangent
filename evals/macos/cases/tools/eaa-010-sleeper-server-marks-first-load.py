"""EAA-010 as Invariant Labs' sleeper experiment did it (EAA-C-009, step 2):
an MCP server presents a benign tool on its first load and records that load
by touching a marker file in home; on its next load it finds the marker and
presents a changed, malicious tool definition."""
import es

CASE = {
    "id": "eaa-010-sleeper-server-marks-first-load",
    "title": "An MCP server under Cursor runs sh -c touch ~/.<marker> on first load, then presents a changed tool when reloaded",
    "flag": True,
    "eaa": ["EAA-010"],
    "group": "tools",
    "grounding": {
        "catalog_case": "EAA-C-009",
        "steps": [2],
        "sources": ["SRC-026", "https://github.com/invariantlabs-ai/mcp-injection-experiments/blob/main/whatsapp-takeover.py"],
    },
    "notes": (
        "Invariant Labs, whatsapp-takeover.py, run in Cursor: a 'random fact of the day' server that, when the marker "
        "file is absent, serves a benign tool and runs os.system('touch ~/<marker>'); when it is present, it serves a "
        "tool whose description shadows another server's send_message to leak messages to the attacker's number. "
        "Visible: the first load's sh -c and touch, the marker's creation, and the second load's server start. The "
        "marker check is a stat, which eslogger is not asked for; the definitions and the shadowed send are MCP "
        "traffic and the other server's network, not visible. How Cursor starts the server is not in the source "
        "(Homebrew python3 on the script here); the marker's and the server's names are made up (the experiment's "
        "marker is ~/.mcp-triggered)."
    ),
}

SERVER = "~/code/globex-weather-mcp/server.py"
MARKER = "~/.globex-weather-seen"
TOUCH = es.Program("touch", "/usr/bin/touch", "com.apple.touch")
HELPER_ARGS = ["--type=utility", "--utility-sub-type=node.mojom.NodeService", "--lang=en-US", "--service-sandbox-type=none"]


def window(mac, cursor):
    host = mac.spawn(cursor, "Cursor Helper (Plugin)", [es.PROGRAMS["Cursor Helper (Plugin)"].path] + HELPER_ARGS, cwd="/")
    mac.read_file(host, "~/.cursor/mcp.json")
    return host


def build(mac):
    mac.exists("~/.cursor/mcp.json", size=260)
    mac.exists(SERVER, size=1400)
    cursor = mac.app("Cursor")
    mac.wait(1)  # first load
    host = window(mac, cursor)
    server = mac.spawn(host, "python", ["python3", mac._p(SERVER)], cwd="/", fds="pipe")
    mac.read_file(server, SERVER)
    mac.wait(0.3)
    sh = mac.spawn(server, "sh", ["sh", "-c", "touch ~/.globex-weather-seen"], cwd="/", fds="pipe", key=True)
    mac.exec(sh, TOUCH, ["touch", mac._p(MARKER)], key=True)  # sh -c with one command execs it in place
    mac.create(sh, MARKER, key=True)
    mac.open(sh, MARKER, es.FWRITE | es.O_CREAT)
    mac.close(sh, MARKER)
    mac.exit(sh)
    mac.wait(240)  # the person reloads the window
    mac.exit(server, status=143)
    mac.exit(host)

    mac.wait(3)  # second load: the server finds the marker and presents the changed tool
    host = window(mac, cursor)
    server = mac.spawn(host, "python", ["python3", mac._p(SERVER)], cwd="/", fds="pipe", key=True)
    mac.read_file(server, SERVER)
    mac.wait(300)
    mac.exit(server, status=143)
