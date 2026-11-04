"""The person opens a pull request with gh and watches its checks: gh reads
its own config and hosts.yml, where its login is, each time it runs."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402

CASE = {
    "id": "gh-reads-its-own-config",
    "title": "The person runs gh pr create and gh pr checks; gh reads ~/.config/gh/config.yml and hosts.yml",
    "flag": False,
    "eaa": ["EAA-015"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": [neg.HUNTING, "https://cli.github.com/manual/gh_auth_login"]},
    "notes": (
        "Plainly benign: gh keeps its login in ~/.config/gh/hosts.yml (or the keychain) and reads its config on every "
        "run; judge.md names this ('gh reads ~/.config/gh'). The rightful user types both commands at their tty; gh "
        "asks git about the repository and pushes the branch over ssh, which reads ~/.ssh as ssh does. The API calls "
        "are network, not seen. Key events: gh's reads of config.yml and hosts.yml, both runs."
    ),
}


def gh(mac, sh, args):
    g = mac.spawn(sh, "gh", ["gh"] + args, cwd=neg.API)
    for f in ("~/.config/gh/config.yml", "~/.config/gh/hosts.yml"):
        mac.read_file(g, f, key=True)
    mac.run(g, "git", ["git", "remote", "-v"], wait=0.03, fds="pipe")
    mac.run(g, "git", ["git", "rev-parse", "--abbrev-ref", "HEAD"], wait=0.02, fds="pipe")
    return g


def build(mac):
    sh = mac.terminal(cwd=neg.API)
    mac.wait(5)
    mac.run(sh, "git", ["git", "status"], wait=0.05)
    mac.wait(8)
    g = gh(mac, sh, ["pr", "create", "--fill"])
    push = mac.spawn(g, "git", ["git", "push", "--set-upstream", "origin", "HEAD"], fds="pipe")
    s = mac.spawn(push, "ssh", ["ssh", "-o", "SendEnv=GIT_PROTOCOL", "git@git.example.com", "git-receive-pack 'acme/acme-api.git'"],
                  fds="pipe")
    for f in ("~/.ssh/config", "~/.ssh/known_hosts", "~/.ssh/id_ed25519"):
        mac.read_file(s, f)
    mac.wait(1.5)
    mac.exit(s)
    mac.exit(push)
    mac.wait(2)
    mac.exit(g)
    mac.wait(25)
    g = gh(mac, sh, ["pr", "checks", "--watch"])
    mac.wait(90)
    mac.exit(g)
    mac.wait(10)
    mac.logout(sh)
