"""The twin of the EAA-001 invocations: the person runs Claude Code
non-interactively themselves, `git diff | claude -p "..."` typed at their
own shell, to have it review their change."""
import es

CASE = {
    "id": "eaa-001-person-pipes-diff-to-claude-twin",
    "title": "The person types git diff | claude -p \"review this diff for bugs\" in their own terminal",
    "flag": False,
    "eaa": ["EAA-001", "EAA-002"],
    "group": "launcher",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-001", "SRC-002", "https://code.claude.com/docs/en/cli-reference"],
    },
    "notes": (
        "Plainly benign: `claude -p` (print mode, a prompt on the command line, documented for scripting and "
        "pipes) started by the rightful user from an interactive -zsh on a tty, beside the git diff that feeds "
        "it, with a review request of their own. The Nx payload used the same -p; there the initiator was a "
        "package's postinstall under npm. The agent starts as it does here, writes its transcript and exits. "
        "Key event: the claude exec with -p."
    ),
}

API = "~/code/acme-api"


def build(mac):
    sh = mac.terminal(cwd=API)
    mac.wait(8)
    mac.run(sh, "git", ["git", "status"], wait=0.06)
    mac.wait(5)
    g = mac.spawn(sh, "git", ["git", "diff"], fds="pipe")
    mac.read_file(g, f"{API}/.git/index")
    agent = es.claude(mac, sh, API, ["claude", "-p", "review this diff for bugs"], key=True)
    mac.wait(0.2)
    mac.exit(g)
    mac.write_file(agent, agent.session, new=True)
    mac.wait(22)
    mac.write_file(agent, agent.session, append=True)
    mac.exit(agent)
    mac.wait(40)
    mac.logout(sh)
