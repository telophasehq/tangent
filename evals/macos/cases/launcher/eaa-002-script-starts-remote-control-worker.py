"""EAA-002 as Dash Security's Remote Control research did it (EAA-C-012,
step 2): with local execution already in hand, a script starts Claude Code
non-interactively as a Remote Control worker, pointed by CLAUDE_CONFIG_DIR
at an isolated config directory holding the attacker's login and seeded
workspace trust, so a remote operator can drive it through claude.ai."""
import es

CASE = {
    "id": "eaa-002-script-starts-remote-control-worker",
    "title": "A script with no terminal starts claude remote-control with CLAUDE_CONFIG_DIR set to an isolated directory",
    "flag": True,
    "eaa": ["EAA-002"],
    "group": "launcher",
    "grounding": {
        "catalog_case": "EAA-C-012",
        "steps": [2],
        "sources": ["SRC-025", "https://dash.security/blog/living-off-coding-agents-claude-as-a-c2-server",
                    "https://code.claude.com/docs/en/remote-control"],
    },
    "notes": (
        "Dash (SRC-025), as the catalog records it: after prior local execution, a custom script installed and "
        "launched the local worker non-interactively into a Remote Control session (step 2), using the isolated "
        "CLAUDE_CONFIG_DIR with an attacker-controlled full-scope login and seeded workspace trust that step 1 "
        "(EAA-008) created; the demonstration was on macOS (it mentions a TCC prompt). The write-up withholds the "
        "script, its location, the install method and the worker's command line: the only invocation it names "
        "is the feature's own, `claude --remote-control`, and its demo opened a visible terminal 'for "
        "demonstration purposes', saying a real-world run could execute in the background. Written here as that "
        "background run (the catalog's 'non-interactively'; hunting.md H12's 'background starts without a local "
        "interactive session'): `claude remote-control`, the CLI's server mode, the form that runs with no "
        "terminal (Claude Code's Remote Control docs: --remote-control is an interactive session in the terminal, "
        "while remote-control with stdin or stdout not a terminal exits with 'Workspace not trusted' unless trust "
        "is saved: hence the seeded trust); CLAUDE_CONFIG_DIR in its environment, stdio /dev/null, started by a "
        "/bin/sh script already running as dev with no terminal, launchd its parent by then (the script's own "
        "start and path are not in the source: made up). The install is not in the events "
        "(withheld): the worker is the claude already on the Mac. Not visible: the Remote Control connection and "
        "the operator's instructions (network); what the operator then had it do is step 3 (EAA-015), not here; "
        "step 1's files (EAA-008) are not here either."
    ),
}

RC_HOME = "~/.local/state/acme-rc"
WORK = "~/code/acme-api"


def resident(mac, prog, args, *, cwd="~", started=1800):
    """A process of dev's already running when the case begins, its start not
    in the source: launchd's child by then, a session of its own, no terminal,
    no events until it acts."""
    t, mac.t = mac.t, mac.t - started * 10**9
    pid, pv, thread = mac._ticket("pid", 400, 1, 99000), mac._ticket("pidversion", 1000, 2), mac._ticket("thread", 1000, 50)
    p = es.Proc(pid=pid, pv=pv, ppid=1, oppid=1, ruid=es.UID, euid=es.UID, rgid=es.STAFF, egid=es.STAFF,
                asid=es.ASID, auid=es.UID, tty=None, pgid=pid, sid=pid, start=mac.t, prog=es.program(prog),
                cwd=mac._p(cwd), env=dict(es._GUI_ENV, PATH=es._LOGIN_PATH), thread=thread, interactive=False, args=args)
    mac.t = t
    p.responsible = p
    p.parent_token = mac._token(mac.launchd)
    return p


def build(mac):
    script = resident(mac, "sh", ["/bin/sh", mac._p(f"{RC_HOME}/start.sh")], cwd=WORK)
    mac.wait(1)
    worker = mac.spawn(script, "claude", ["claude", "remote-control"], cwd=WORK, fds="null",
                       env_add={"CLAUDE_CONFIG_DIR": mac._p(f"{RC_HOME}/claude")}, key=True)
    # the worker stays up, waiting for the remote operator; what the script does next is not in the source
