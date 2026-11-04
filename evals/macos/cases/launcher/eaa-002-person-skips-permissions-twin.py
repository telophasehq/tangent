"""The twin of the EAA-002 launches: the person starts Claude Code with
--dangerously-skip-permissions themselves, typed in their own terminal in
their own repository, and has it run the tests and fix a file."""
import es

CASE = {
    "id": "eaa-002-person-skips-permissions-twin",
    "title": "The person types claude --dangerously-skip-permissions in their own terminal and works with it",
    "flag": False,
    "eaa": ["EAA-002"],
    "group": "launcher",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-002", "SRC-004", "https://code.claude.com/docs/en/cli-reference"],
    },
    "notes": (
        "Plainly benign: the flag the Nx postinstall and the Trivy extension passed (EAA-C-001, EAA-C-002), chosen "
        "here by the rightful user, typed at an interactive -zsh on a tty in a Ghostty tab, under Ghostty and "
        "login, with no prompt on the command line. The catalog's hunt is 'no TTY plus permissive flags' and "
        "'permissive agent start from package lifecycle, extension host, or repo automation'; this has a tty and "
        "the person's shell as its parent. The agent then works as it does here: prompts, a cargo test through "
        "its tool shell, a source file written. Key event: the claude exec with the flag."
    ),
}

API = "~/code/acme-api"


def build(mac):
    sh = mac.terminal("Ghostty", cwd=API)
    mac.wait(6)
    agent = es.claude(mac, sh, API, ["claude", "--dangerously-skip-permissions"], key=True)
    mac.wait(12)
    es.claude_prompt(mac, agent)
    mac.wait(5)
    tool = es.agent_shell(mac, agent, "cargo test")
    c = mac.spawn(tool, "cargo", ["cargo", "test"])
    mac.read_file(c, f"{API}/Cargo.toml")
    mac.wait(14)
    mac.exit(c, status=101)
    mac.exit(tool)
    mac.read_file(agent, f"{API}/src/routes.rs")
    mac.wait(9)
    mac.write_file(agent, f"{API}/src/routes.rs")
    mac.write_file(agent, agent.session, append=True)
    mac.wait(30)
    es.claude_prompt(mac, agent)
    mac.wait(120)
    mac.exit(agent)
    mac.wait(4)
    mac.logout(sh)
