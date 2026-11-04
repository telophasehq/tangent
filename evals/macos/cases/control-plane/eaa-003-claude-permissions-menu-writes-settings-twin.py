"""The twin of the EAA-003 cases that write ~/.claude/settings.json: the
person adds an allow rule through Claude Code's own /permissions dialog,
saved to user settings, and Claude Code writes ~/.claude/settings.json."""
import es

CASE = {
    "id": "eaa-003-claude-permissions-menu-writes-settings-twin",
    "title": "The person adds an allow rule through Claude Code's /permissions dialog to user settings; Claude Code writes ~/.claude/settings.json",
    "flag": False,
    "eaa": ["EAA-003"],
    "group": "control-plane",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-012", "SRC-016", "https://code.claude.com/docs/en/permissions"]},
    "notes": (
        "Plainly benign: the permissions documentation has the person add allow, ask and deny rules through "
        "/permissions, each saved to the settings file the person picks; here user settings, ~/.claude/settings.json "
        "(SRC-016). The writer is Claude Code itself, in a session the person started from their terminal. Not a hook: "
        "the hooks documentation (SRC-012) makes /hooks a read-only browser; hooks are added by editing the files. "
        "How Claude Code writes settings.json (in place or by rename) was not observed: written as a plain truncating "
        "write. Key events: the settings write."
    ),
}

WEB = "~/code/acme-web"


def build(mac):
    sh = mac.terminal(cwd=WEB)
    mac.wait(5)
    agent = es.claude(mac, sh, WEB)
    mac.wait(25)
    es.claude_prompt(mac, agent)
    mac.wait(40)
    mac.read_file(agent, "~/.claude/settings.json")  # /permissions
    mac.wait(35)
    mac.write_file(agent, "~/.claude/settings.json", key=True)
    mac.wait(20)
    es.claude_prompt(mac, agent)
    mac.wait(30)
    mac.exit(agent)
    mac.logout(sh)
