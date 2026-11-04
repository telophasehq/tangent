"""The person runs `brew services start` for a queue they develop against:
Homebrew writes the formula's LaunchAgent plist, bootstraps it with
launchctl, Background Task Management records the login item, and launchd
starts the service."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "brew-services-starts-launch-agent",
    "title": "The person runs brew services start; Homebrew writes a LaunchAgent, launchctl bootstraps it, BTM records it",
    "flag": False,
    "eaa": [],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": [
        "https://docs.brew.sh/Manpage#services-subcommand",
        "https://developer.apple.com/documentation/endpointsecurity/es_event_btm_launch_item_add_t"]},
    "notes": (
        "Plainly benign: the rightful user asks Homebrew, at their tty, to run a formula's service at login; `brew "
        "services start` writes ~/Library/LaunchAgents/homebrew.mxcl.<formula>.plist and bootstraps it, and macOS "
        "records the new login item (btm_launch_item_add). judge.md flags a new login item 'unless an installer the "
        "person plainly ran did it', which this is. No catalog technique: persistence is judge.md's own rule. The "
        "formula (acme/tap/acme-queue) is made up; the plist's content and brew's network are not seen; the "
        "btm_launch_item_add shape is from ESMessage.h, not a capture (es.py). Key events: ruby's write of the plist, "
        "launchctl's exec, the btm_launch_item_add, and the service's exec under launchd."
    ),
}

RUBY = "/opt/homebrew/Library/Homebrew/vendor/portable-ruby/current/bin/ruby"
QUEUE = es.Program("acme-queue", "/opt/homebrew/Cellar/acme-queue/0.9.2/bin/acme-queue", "acme-queue", cs=es.ADHOC,
                   dyld="/opt/homebrew/opt/acme-queue/bin/acme-queue")
PLIST = f"{es.HOME}/Library/LaunchAgents/homebrew.mxcl.acme-queue.plist"


def build(mac):
    sh = mac.terminal()
    mac.wait(6)
    b = mac.spawn(sh, "brew", ["brew", "services", "start", "acme/tap/acme-queue"])  # bash -pu brew, which execs ruby
    mac.exec(b, "ruby", [RUBY, "-W1", "--disable=gems,rubyopt", "/opt/homebrew/Library/Homebrew/brew.rb", "services", "start",
                         "acme/tap/acme-queue"])
    mac.read_file(b, "/opt/homebrew/Library/Taps/acme/homebrew-tap/Formula/acme-queue.rb")
    mac.wait(1.5)
    mac.write_file(b, PLIST, new=True, key=True)
    lc = mac.spawn(b, "launchctl", ["/bin/launchctl", "bootstrap", f"gui/{es.UID}", PLIST], fds="pipe", key=True)
    mac.btm_launch_item_add(f"file://{PLIST}", item_type=3, legacy=True, executable_path=QUEUE.dyld, instigator=lc, key=True)
    _, started = neg.job(mac, "homebrew.mxcl.acme-queue", QUEUE, [QUEUE.dyld, "--config", "/opt/homebrew/etc/acme-queue.conf"])
    mac.key(started)
    mac.wait(0.2)
    mac.exit(lc)
    mac.wait(0.5)
    mac.exit(b)
    mac.wait(15)
    mac.logout(sh)
