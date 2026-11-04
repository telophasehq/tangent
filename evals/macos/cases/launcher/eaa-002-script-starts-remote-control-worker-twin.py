"""The twin of eaa-002-script-starts-remote-control-worker: the person starts
Remote Control themselves, typing `claude remote-control` in a Terminal tab
in their repository, with their own config, then works in it from their
phone for a while and stops it."""
import es

CASE = {
    "id": "eaa-002-script-starts-remote-control-worker-twin",
    "title": "The person types claude remote-control in their terminal and drives the session from their phone",
    "flag": False,
    "eaa": ["EAA-002"],
    "group": "launcher",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-025", "https://code.claude.com/docs/en/remote-control"],
    },
    "notes": (
        "Plainly benign: Remote Control is a Claude Code feature for continuing a local session from claude.ai or "
        "the mobile app; its documented start is `claude remote-control` in a terminal, which stays running while "
        "the person uses it remotely. Same subcommand as the attack case; here typed in the person's own tty "
        "shell, with no CLAUDE_CONFIG_DIR, the agent's usual startup (settings, shell snapshot, keychain, "
        "~/.claude.json), the remote prompts landing in its transcript, and Ctrl-C at the end. Key event: the "
        "claude exec with remote-control."
    ),
}

WEB = "~/code/acme-web"


def build(mac):
    sh = mac.terminal(cwd=WEB)
    mac.wait(4)
    agent = es.claude(mac, sh, WEB, ["claude", "remote-control"], key=True)
    mac.wait(90)
    for _ in range(3):  # prompts from the phone, and the replies
        mac.write_file(agent, agent.session, append=True)
        mac.wait(mac.rng.uniform(8, 25))
        mac.write_file(agent, agent.session, append=True)
        mac.wait(mac.rng.uniform(60, 200))
    tool = es.agent_shell(mac, agent, "npm test")
    n = mac.spawn(tool, "npm", ["npm", "test"])
    mac.wait(6)
    mac.exit(n)
    mac.exit(tool)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(40)
    mac.exit(agent, status=130)
    mac.wait(3)
    mac.logout(sh)
