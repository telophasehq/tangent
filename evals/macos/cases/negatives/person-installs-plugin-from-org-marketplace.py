"""The person installs their company's lint plugin from the marketplace their
team uses, with Claude Code's own /plugin install: Claude Code copies the
plugin (with a hook) into its cache, records it, and enables it in settings."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "person-installs-plugin-from-org-marketplace",
    "title": "The person runs /plugin install acme-lint@acme-marketplace; Claude Code writes the plugin's cache, its record and settings.json",
    "flag": False,
    "eaa": ["EAA-009"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-017", "SRC-018", "SRC-019"]},
    "notes": (
        "Plainly benign: installing a plugin from a marketplace the person added is the documented path (SRC-017 to "
        "SRC-019); the rightful user types the command in a Claude Code session at their tty, from their company's "
        "marketplace, and Claude Code (the vendor's binary) is the writer of every file. The cache layout "
        "(cache/<marketplace>/<plugin>/<version>/) is this Mac's; the plugin's files and how settings.json and "
        "installed_plugins.json are written (plainly) are assumed; acme-lint is made up. The plugin's hook would run "
        "at the agent's steps, which is why its hooks.json is a key event. Key events: the hooks.json write, "
        "installed_plugins.json and ~/.claude/settings.json (enabledPlugins), all by claude."
    ),
}

CACHE = f"{es.HOME}/.claude/plugins/cache/acme-marketplace/acme-lint/1.2.0"


def build(mac):
    sh = mac.terminal(cwd=neg.API)
    mac.wait(5)
    c = es.claude(mac, sh, neg.API)
    mac.wait(18)
    mac.write_file(c, "~/.claude/history.jsonl", append=True)  # /plugin install acme-lint@acme-marketplace
    mac.read_file(c, "~/.claude/plugins/known_marketplaces.json")
    mac.read_file(c, f"{es.HOME}/.claude/plugins/marketplaces/acme-marketplace/.claude-plugin/marketplace.json")
    for d in ("", "/.claude-plugin", "/commands", "/hooks", "/scripts"):
        mac.create(c, CACHE + d)
    for f in (".claude-plugin/plugin.json", "commands/lint.md", "scripts/lint.sh", "README.md"):
        mac.write_file(c, f"{CACHE}/{f}", new=True)
    mac.write_file(c, f"{CACHE}/hooks/hooks.json", new=True, key=True)
    mac.write_file(c, "~/.claude/plugins/installed_plugins.json", key=True)
    mac.read_file(c, "~/.claude/settings.json")
    mac.write_file(c, "~/.claude/settings.json", key=True)
    mac.wait(25)
    es.claude_prompt(mac, c)
    neg.session_end(mac, c, sh)
