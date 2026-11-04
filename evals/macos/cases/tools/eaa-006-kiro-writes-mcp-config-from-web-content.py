"""EAA-006 as Intezer and Kodem demonstrated it against Kiro (EAA-C-025, steps
2 and 3): after a web fetch the person approved, Kiro's agent writes a new
server into the user-level ~/.kiro/settings/mcp.json with its file-write
tool, reloads it unasked, and starts the server: `node -e` with an inline
script that calls back every ten seconds."""
import es

CASE = {
    "id": "eaa-006-kiro-writes-mcp-config-from-web-content",
    "title": "Kiro's agent writes ~/.kiro/settings/mcp.json and, reloading it, starts node -e",
    "flag": True,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {
        "catalog_case": "EAA-C-025",
        "steps": [2, 3],
        "sources": ["SRC-061", "SRC-062", "https://research.intezer.com/blog/2026/07/remote-code-execution-kiro/"],
    },
    "notes": (
        "Intezer and Kodem (Kiro 0.9.2 on macOS; fixed in 0.11.130, CVE-2026-10591): the person asked Kiro to fetch a "
        "URL and follow its instructions and approved the fetch; hidden instructions in the page (step 1, EAA-018: "
        "network and model context, not visible) made the agent write "
        "a server named telemetry into ~/.kiro/settings/mcp.json through its fsWrite tool, without approval; Kiro "
        "reloaded the file at once, even with its warning dismissed, and started `node -e` with an inline script using "
        "only Node's built-ins that sent the hostname, username and platform as query parameters in a GET to "
        "localhost:8080 every ten seconds (step 3, EAA-015: network). The source names no process within Kiro: the "
        "writer and the spawner are Kiro's extension host here; nor does it say whether the agent read the file "
        "before writing it (no read here). Kiro's app paths and signature are not verified (a placeholder team). "
        "The inline script is a plain stand-in that sends nothing, to a local port."
    ),
}

TEAM = "KIRO000000"  # not verified: a placeholder for Kiro's Developer ID team
KIRO = es.Program("Kiro", "/Applications/Kiro.app/Contents/MacOS/Electron", "dev.kiro.desktop", TEAM, es.DEVID_APP)
HELPER = es.Program("Kiro Helper (Plugin)",
                    "/Applications/Kiro.app/Contents/Frameworks/Kiro Helper (Plugin).app/Contents/MacOS/Kiro Helper (Plugin)",
                    "com.github.Electron.helper", TEAM, es.DEVID_APP)
KIRO = es.PROGRAMS.setdefault("Kiro", KIRO)  # mac.app() takes a name from PROGRAMS; es.py has no Kiro
WEB = "~/code/acme-web"
BEACON = "setInterval(() => fetch('http://localhost:7340/'), 15000)"


def build(mac):
    mac.exists("~/.kiro/settings/mcp.json", size=96)
    kiro = mac.app("Kiro")
    mac.wait(1)  # the person opens acme-web in a new window
    host = mac.spawn(kiro, HELPER, [HELPER.path, "--type=utility", "--utility-sub-type=node.mojom.NodeService",
                                    "--lang=en-US", "--service-sandbox-type=none"], cwd="/")
    mac.read_file(host, "~/.kiro/settings/mcp.json")
    for f in ("package.json", "src/app.ts"):
        mac.read_file(host, f"{WEB}/{f}")
    mac.wait(40)  # the person asks Kiro to fetch a URL and follow its instructions, and approves the fetch
    mac.write_file(host, "~/.kiro/settings/mcp.json", key=True)  # fsWrite
    mac.wait(0.6)  # the reload
    mac.read_file(host, "~/.kiro/settings/mcp.json")
    server = mac.spawn(host, "node", ["node", "-e", BEACON], cwd=mac._p(WEB), fds="pipe", key=True)
    mac.wait(300)
    mac.exit(server, status=143)
