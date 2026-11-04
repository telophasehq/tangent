"""The person loads their key with `ssh-add --apple-use-keychain`; launchd
starts ssh-agent on the socket's first use; then the person connects to a new
build host and ssh records its host key."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "ssh-add-loads-key-into-agent",
    "title": "The person runs ssh-add, launchd starts ssh-agent with no tty, and ssh to a new host appends known_hosts",
    "flag": False,
    "eaa": ["EAA-015"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": [neg.HUNTING, "https://man.openbsd.org/ssh-add.1",
                                                                  "https://man.openbsd.org/ssh.1"]},
    "notes": (
        "Plainly benign: macOS runs ssh-agent as a launchd agent on demand (the SSH_AUTH_SOCK every login session "
        "has), and ssh-add reads the private key to load it; ssh reads its config and known hosts, and appends a new "
        "host's key to known_hosts once the person accepts it. Every program is Apple's, the person's at a tty or "
        "launchd's; judge.md names ssh reading ~/.ssh, and a program with no tty whose parent is launchd, as normal. "
        "build.example.net is made up. Key events: ssh-agent's exec under launchd (no tty), ssh-add's read of "
        "~/.ssh/id_ed25519, and ssh's write of ~/.ssh/known_hosts."
    ),
}


def build(mac):
    sh = mac.terminal()
    mac.wait(5)
    add = mac.spawn(sh, "ssh-add", ["ssh-add", "--apple-use-keychain", f"{es.HOME}/.ssh/id_ed25519"])
    mac.wait(0.05)
    _, started = neg.job(mac, "com.openssh.ssh-agent", "ssh-agent", ["/usr/bin/ssh-agent", "-l"])
    mac.key(started)
    mac.read_file(add, "~/.ssh/id_ed25519", key=True)
    mac.read_file(add, "~/.ssh/id_ed25519.pub")
    mac.wait(4)  # the passphrase, kept in the keychain
    mac.exit(add)
    mac.wait(10)
    s = mac.spawn(sh, "ssh", ["ssh", "deploy@build.example.net"])
    mac.read_file(s, "~/.ssh/config")
    mac.read_file(s, "~/.ssh/known_hosts")
    mac.wait(5)  # "Are you sure you want to continue connecting (yes/no)?"
    mac.write_file(s, "~/.ssh/known_hosts", append=True, key=True)
    mac.wait(120)
    mac.exit(s)
    mac.wait(8)
    mac.logout(sh)
