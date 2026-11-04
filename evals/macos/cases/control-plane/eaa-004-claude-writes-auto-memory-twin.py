"""The twin of eaa-004-postinstall-overwrites-claude-memory: Claude Code's
auto memory saves what it learned in a session to the project's MEMORY.md
and a topic file, as it is documented to."""
import es

CASE = {
    "id": "eaa-004-claude-writes-auto-memory-twin",
    "title": "Claude Code writes its own auto memory: the project's MEMORY.md and a topic file",
    "flag": False,
    "eaa": ["EAA-004"],
    "group": "control-plane",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-013"]},
    "notes": (
        "Plainly benign: auto memory (SRC-013) has Claude write MEMORY.md and topic files under "
        "~/.claude/projects/<project>/memory/ during a session; the writer is Claude Code itself, started by the person "
        "in a Terminal tab. Written as plain writes (not observed). Key events: the memory writes."
    ),
}

WEB = "~/code/acme-web"


def build(mac):
    mem = f"{es.project_dir(es.HOME + '/code/acme-web')}/memory"
    mac.exists(f"{mem}/MEMORY.md", days=10)
    sh = mac.terminal(cwd=WEB)
    mac.wait(5)
    agent = es.claude(mac, sh, WEB)
    mac.read_file(agent, f"{mem}/MEMORY.md")
    mac.wait(20)
    es.claude_prompt(mac, agent)
    mac.wait(60)
    es.claude_prompt(mac, agent)  # "remember: tests need `npm run db:up` first"
    mac.write_file(agent, f"{mem}/testing.md", key=True)
    mac.write_file(agent, f"{mem}/MEMORY.md", key=True)
    mac.wait(30)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(20)
    mac.exit(agent)
    mac.logout(sh)
