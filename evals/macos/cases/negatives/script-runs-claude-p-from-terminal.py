"""The person runs their own release-notes script at their terminal; the
script pipes `git log` into `claude -p`. A prompt on the command line, from a
program, but on the person's tty and under their shell."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "script-runs-claude-p-from-terminal",
    "title": "The person's release-notes script, run at their terminal, pipes git log into claude -p",
    "flag": False,
    "eaa": ["EAA-001", "EAA-002"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": [neg.HUNTING, "https://code.claude.com/docs/en/headless"]},
    "notes": (
        "Plainly benign: print mode (-p) is the documented non-interactive use, and the catalog's H1 and H2 note that "
        "shells, task runners and wrappers legitimately launch agents and that non-interactive execution is normal. "
        "Unlike an initiator an attacker controls, the script is the person's (scripts/release-notes.sh in their "
        "repository), run from their -zsh, and claude keeps the terminal's tty. The prompt is a plain request; the "
        "commit list arrives on stdin, which eslogger does not show, as is the notes file's content. Key event: "
        "claude's exec with -p, under bash, under -zsh."
    ),
}

SCRIPT = es.Program("release-notes.sh", f"{neg.WEB}/scripts/release-notes.sh", dyld=f"{neg.WEB}/scripts/release-notes.sh",
                    interp=("bash", []), size=412)


def build(mac):
    sh = mac.terminal(cwd=neg.WEB)
    mac.wait(6)
    mac.run(sh, "git", ["git", "tag", "--list", "v1.*"], wait=0.04)
    mac.wait(9)
    script = mac.spawn(sh, SCRIPT, ["./scripts/release-notes.sh", "v1.4.0"], cwd=neg.WEB)
    mac.run(script, "git", ["git", "log", "--oneline", "v1.3.0..HEAD"], wait=0.05)
    c = es.claude(mac, script, neg.WEB, ["claude", "-p", "Write release notes for v1.4.0 from these commits, grouped by area"],
                  key=True)
    es.claude_prompt(mac, c)
    mac.write_file(c, c.session, append=True)
    mac.atomic_write(c, "~/.claude.json")
    mac.wait(0.5)
    mac.exit(c)
    mac.write_file(script, f"{neg.WEB}/docs/release-notes-v1.4.0.md", new=True)
    mac.exit(script)
    mac.wait(20)
    mac.run(sh, "git", ["git", "diff", "--stat"], wait=0.04)
    mac.wait(30)
    mac.logout(sh)
