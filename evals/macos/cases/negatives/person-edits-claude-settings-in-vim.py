"""The control for an EAA-003 hook planting: the person opens
~/.claude/settings.json in vim from their own shell and saves it, as vim
saves (a backup by rename, the file written new)."""
import es

CASE = {
    "id": "person-edits-claude-settings-in-vim",
    "title": "The person edits ~/.claude/settings.json in vim from their shell",
    "flag": False,
    "eaa": ["EAA-003"],
    "group": "negatives",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-012"],
    },
    "notes": (
        "Plainly benign: the rightful user edits their own agent settings (where hooks are configured, per the hooks "
        "documentation) in /usr/bin/vim, typed in a Terminal tab with a tty. The same file the postinstall case writes; "
        "the writer is the person's editor. Key events: each save's rename to settings.json~, create, write and close."
    ),
}


def build(mac):
    sh = mac.terminal()
    mac.wait(5)
    mac.run(sh, "ls", ["ls", "-la", "/Users/dev/.claude"], wait=0.01)
    mac.wait(4)
    es.vim_edit(mac, sh, "~/.claude/settings.json", saves=1, key=True)
    mac.wait(30)
    mac.logout(sh)
