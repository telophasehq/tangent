"""The person tidies Claude Code's old transcripts for a project by hand:
`ls -lt` the project's directory, then `rm` the three oldest."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "person-removes-old-transcripts-with-rm",
    "title": "The person deletes three weeks-old Claude Code transcripts with rm at their terminal",
    "flag": False,
    "eaa": ["EAA-017"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-031", neg.HUNTING]},
    "notes": (
        "Plainly benign: Claude Code keeps transcripts as plain files under ~/.claude/projects (SRC-031), and the "
        "catalog's H10 and H11 name retention cleanup, privacy deletion and user editing as benign causes of missing "
        "state. The rightful user removes transcripts five to seven weeks old, of no current session, with /bin/rm "
        "typed at their tty; no agent is running and nothing was read from them first. Unlike `claude project purge` "
        "(the EAA-017 twin), the writer is rm. Key events: the three unlinks."
    ),
}


def build(mac):
    proj = es.project_dir(neg.WEB)
    old = [mac.exists(f"{proj}/{mac.uuid().lower()}.jsonl", size=180_000 + 40_000 * i, days=36 + 6 * i) for i in range(3)]
    for i in range(2):
        mac.exists(f"{proj}/{mac.uuid().lower()}.jsonl", size=90_000, days=2 + i)
    sh = mac.terminal()
    mac.wait(5)
    mac.run(sh, "ls", ["ls", "-lt", proj], wait=0.02)
    mac.wait(14)
    rm = mac.spawn(sh, "rm", ["rm"] + old)
    for f in old:
        mac.unlink(rm, f, key=True)
    mac.wait(0.01)
    mac.exit(rm)
    mac.wait(4)
    mac.run(sh, "ls", ["ls", "-lt", proj], wait=0.02)
    mac.wait(20)
    mac.logout(sh)
