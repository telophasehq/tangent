"""The twin of EAA-005 transcript reads: the person resumes yesterday's
Claude Code session with `claude --resume`; Claude Code reads the
transcript it wrote and goes on appending to it."""
import es

CASE = {
    "id": "eaa-005-claude-resumes-own-session-twin",
    "title": "The person runs claude --resume; Claude Code reads its own earlier transcript",
    "flag": False,
    "eaa": ["EAA-005"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-030", "SRC-031"],
    },
    "notes": (
        "Plainly benign: the sessions documentation describes resuming a conversation from its saved transcript "
        "under ~/.claude/projects/<project>/. The person, at a Terminal tab with a tty, runs `claude --resume` in "
        "their own repository and picks yesterday's session; the agent that wrote the transcript reads it back "
        "and appends to it. Key event: claude's read of the transcript."
    ),
}

WEB = "/Users/dev/code/acme-web"
OLD = f"{es.project_dir(WEB)}/2b4d6f8a-0c1e-4a3b-9d5f-7e9a1c3b5d7f.jsonl"


def build(mac):
    mac.exists(OLD, size=280_000, days=1)
    sh = mac.terminal(cwd=WEB)
    mac.wait(4)
    agent = es.claude(mac, sh, WEB, ["claude", "--resume"])
    mac.wait(6)  # the session picker
    mac.read_file(agent, OLD, key=True)
    agent.session = OLD
    mac.wait(10)
    es.claude_prompt(mac, agent)
    mac.wait(5)
    mac.read_file(agent, f"{WEB}/src/app.ts")
    mac.wait(30)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(30)
    mac.exit(agent)
    mac.logout(sh)
