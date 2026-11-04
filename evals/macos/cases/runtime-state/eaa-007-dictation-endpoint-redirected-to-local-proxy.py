"""EAA-007 as Wardle's Muse proof of concept describes it (EAA-C-037, step
1): code running as the user starts a local WebSocket proxy, changes the
dictation app's endpoint preference to it and restarts the app, so the next
dictation session passes through the proxy."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-007-dictation-endpoint-redirected-to-local-proxy",
    "title": "A script from Downloads points the dictation app's endpoint preference at a local proxy and restarts the app",
    "flag": True,
    "eaa": ["EAA-007"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-037",
        "steps": [1],
        "sources": ["SRC-080", "https://github.com/pwardle/not-a-mused/tree/1e5615be645c64791600c947d8d5988205fd1514"],
    },
    "notes": (
        "The catalog: the PoC, a Python script run as the local user, changes the app's "
        "endo_voyager_dictation_endpoint preference to a local WebSocket proxy and restarts the app; the next "
        "dictation session passes through the proxy, which captures the account token and can replace the "
        "returned text. The PoC at the cited commit (notamused.py, `#!/usr/bin/env python3`, Python 3.10+, so "
        "Homebrew's python, not the Command Line Tools' 3.9) does, on start: `/usr/bin/defaults write <domain> "
        "endo_voyager_dictation_endpoint -string <a ws:// URL on the loopback address>`, `/usr/bin/defaults "
        "read` of it to check, `/usr/bin/pgrep -x -u <uid> Muse`, SIGTERM to the app and pgrep again until it is gone, then "
        "`/usr/bin/open -a Muse`; each a child with stdio piped. Written as the script run from a Terminal tab "
        "(the README runs it that way; a real attacker needs code execution as the user first). cfprefsd saves "
        "the preference as the app's plist (a tmp file renamed over; the tmp file's name is assumed) while "
        "defaults waits; the relaunched app reads its preferences through cfprefsd, not from the file. Launch "
        "Services has launchd start the app (xpcproxy, then the app: from memory, not verified). Stand-ins: the "
        "app's preference domain and signature, the script's name and place, the URL (localhost, a made-up port "
        "and path). Not "
        "visible: the signals (not subscribed), the dictation traffic through the proxy, the token captured and "
        "the replaced text (network, app memory); step 2's token reuse (EAA-015) is not in this case. Key "
        "events: the script's start, the defaults write, and cfprefsd's rename over the plist."
    ),
}

MUSE = es.Program("Muse", "/Applications/Muse.app/Contents/MacOS/Muse", "com.example.muse", "A1B2C3D4E5", es.DEVID_APP)  # stand-in signature
SCRIPT = f"{es.HOME}/Downloads/dictation-proxy/proxy.py"  # `#!/usr/bin/env python3`
DOMAIN = "com.example.muse"
PLIST = f"~/Library/Preferences/{DOMAIN}.plist"
ENDPOINT = "ws://localhost:8765/dictation"


def build(mac):
    mac.exists(PLIST)
    app = rs.app(mac, MUSE)
    mac.exists(SCRIPT, size=58210, days=0)
    sh = mac.terminal(cwd="~/Downloads/dictation-proxy")
    mac.wait(5)
    # ./proxy.py: the kernel runs its shebang's env, which finds python3 on PATH: Homebrew's (the PoC needs 3.10+)
    proxy = mac.fork(sh)
    e = mac.exec(proxy, "env", ["/usr/bin/env", "python3", "./proxy.py", "--port", "8765"])
    e["event"]["exec"]["script"] = mac._file(SCRIPT)
    mac.key(mac.exec(proxy, "python", ["python3", "./proxy.py", "--port", "8765"]))
    mac.read_file(proxy, SCRIPT)
    mac.wait(0.6)
    w = mac.spawn(proxy, rs.DEFAULTS, ["/usr/bin/defaults", "write", DOMAIN, "endo_voyager_dictation_endpoint", "-string",
                                       ENDPOINT], fds="pipe", key=True)
    mac.wait(0.01)
    prefs = mac.daemon("cfprefsd", user=True)
    mac.atomic_write(prefs, PLIST, key=True)  # defaults waits for cfprefsd to save
    mac.wait(0.005)
    mac.exit(w)
    mac.run(proxy, rs.DEFAULTS, ["/usr/bin/defaults", "read", DOMAIN, "endo_voyager_dictation_endpoint"], wait=0.03, fds="pipe")
    pgrep = ["/usr/bin/pgrep", "-x", "-u", str(es.UID), "Muse"]
    mac.run(proxy, rs.PGREP, pgrep, wait=0.02, fds="pipe")
    mac.wait(0.01)  # SIGTERM to the app (no event here)
    mac.wait(0.2)
    mac.exit(app)
    mac.wait(0.05)
    mac.run(proxy, rs.PGREP, pgrep, wait=0.02, status=1, fds="pipe")  # gone
    mac.wait(1)
    mac.run(proxy, rs.OPEN, ["/usr/bin/open", "-a", "Muse"], wait=0.2, fds="pipe")
    a = rs.launchd_job(mac, f"application.{DOMAIN}.{mac.rng.randint(10**6, 10**7)}.{mac.rng.randint(10**6, 10**7)}", user=True)
    mac.exec(a, MUSE, [MUSE.path], env=dict(es._GUI_ENV))
    # the proxy keeps listening; the person's next dictation goes through it (network: not visible)
