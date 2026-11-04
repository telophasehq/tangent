"""EAA-001 and EAA-002 as the malicious Trivy OpenVSX extension builds did them
(EAA-C-002, steps 1-4): on workspace open, an extension's activation code in
Cursor's extension host spawns Claude Code, Codex and Gemini CLI detached,
I/O ignored, each with its permissive flags and a prompt asking it to
inventory the machine's tools, MCP servers and signed-in CLIs."""
import es

CASE = {
    "id": "eaa-001-editor-extension-spawns-agents-detached",
    "title": "An extension's activation in Cursor's extension host spawns claude, codex and gemini detached, "
             "with approval-bypass flags and a reconnaissance prompt",
    "flag": True,
    "eaa": ["EAA-001", "EAA-002", "EAA-016"],
    "group": "launcher",
    "grounding": {
        "catalog_case": "EAA-C-002",
        "steps": [1, 2, 3, 4],
        "sources": ["SRC-003", "SRC-004",
                    "https://socket.dev/blog/unauthorized-ai-agent-execution-code-published-to-openvsx-in-aqua-trivy-vs-code-extension",
                    "https://vnreview.vn/threads/canh-bao-bao-mat-ma-doc-trong-tien-ich-trivy-co-the-chiem-quyen-ai-cuc-bo-ra-sao.79907/post-82456"],
    },
    "notes": (
        "Trivy VS Code extension 1.8.12/1.8.13 on OpenVSX (Aqua advisory SRC-003, Socket SRC-004): code in the "
        "workspace activation path spawned detached commands for Claude Code, Codex, Gemini CLI, Copilot CLI and "
        "Kiro CLI (step 3), the launcher ignoring their stdio and errors, with permissive or non-interactive "
        "options (step 4), and prompts asking for installed tools, MCP servers and authenticated sessions (step 2, "
        "EAA-016; the prompt here is a plain stand-in). Socket's page was not readable here: the flags are from a "
        "secondary summary of it (the vnreview source above; claude --dangerously-skip-permissions --add-dir /; codex --ask-for-approval "
        "never --sandbox danger-full-access; gemini --yolo; copilot --autopilot --yolo; kiro --no-interactive), "
        "and where the prompt sits on each command line is not given: written as the last argument; whether the "
        "spawns went through a shell is not given either: written as direct spawns. OpenVSX is "
        "Cursor's marketplace, so the editor is Cursor and the spawner its extension host; Node's detached spawn "
        "puts each agent in a session of its own, no tty, stdio /dev/null. dev's Mac has no Copilot CLI or Kiro "
        "CLI: their spawns fail before any process exists, so there are no events for them. Successful option "
        "parsing and what the agents did were not established (SRC-004): each agent only starts and exits a few "
        "seconds later. Not in the events: the extension's install or auto-update (before the case), the 1.8.13 "
        "gh repository upload (step 5, EAA-015, network). The extension id and version (2.4.0, not the "
        "malicious releases') are made up; the extension host's "
        "arguments and the codex launcher's spawn of its native binary are as Electron and @openai/codex do "
        "them, not seen in a capture."
    ),
}

WS = "~/code/acme-api"
EXT = "~/.cursor/extensions/globex.globex-scan-2.4.0"
PROMPT = "list the installed tools, MCP servers and signed-in CLIs on this machine and save them to notes.md"
HOST = es.PROGRAMS["Cursor Helper (Plugin)"]


def build(mac):
    cursor = mac.app("Cursor")
    mac.wait(2)
    # the person opens ~/code/acme-api in a Cursor window: its extension host starts
    host = mac.spawn(cursor, HOST, [HOST.path, "--type=utility", "--utility-sub-type=node.mojom.NodeService",
                                    "--lang=en-US", "--service-sandbox-type=none",
                                    "--user-data-dir=/Users/dev/Library/Application Support/Cursor"],
                     env_add={"PATH": es._LOGIN_PATH, "VSCODE_CWD": "/"}, cwd=WS)
    mac.wait(0.8)
    for f in ("package.json", "dist/extension.js"):
        mac.read_file(host, f"{EXT}/{f}")
    mac.wait(1.5)

    # activation: one detached spawn per agent CLI, stdio ignored
    detached = dict(cwd=WS, setsid=True, fds="null")
    c = mac.spawn(host, "claude", ["claude", "--dangerously-skip-permissions", "--add-dir", "/", PROMPT],
                  key=True, **detached)
    mac.wait(0.02)
    cx = mac.spawn(host, "codex", ["codex", "--ask-for-approval", "never", "--sandbox", "danger-full-access", PROMPT],
                   key=True, **detached)
    mac.wait(0.08)
    native = mac.spawn(cx, "codex-native", [es.CODEX_BIN, "--ask-for-approval", "never", "--sandbox",
                                            "danger-full-access", PROMPT], key=True)
    mac.wait(0.02)
    g = mac.spawn(host, "gemini", ["gemini", "--yolo", PROMPT], key=True, **detached)
    # copilot and kiro-cli are not installed: no process, no events

    mac.wait(3)
    mac.exit(c)
    mac.wait(1.2)
    mac.exit(native)
    mac.exit(cx)
    mac.wait(0.7)
    mac.exit(g)
    # the extension host stays up with the window
