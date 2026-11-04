"""The twin of an EAA-007 gateway redirect: the person, in their own
terminal, starts Claude Code with ANTHROPIC_BASE_URL set to their
company's LLM gateway, as the env-vars documentation describes for
corporate gateways."""
import es

CASE = {
    "id": "eaa-007-person-starts-claude-via-company-gateway-twin",
    "title": "The person types ANTHROPIC_BASE_URL=<the company's LLM gateway> claude in their terminal",
    "flag": False,
    "eaa": ["EAA-007"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-014", "SRC-016"],
    },
    "notes": (
        "Plainly benign: the catalog's EAA-007 page notes that Claude Code documents legitimate custom gateways and "
        "provider routing, and hunting.md's H5 lists enterprise gateways as legitimate. The person, at a Terminal "
        "tab with a tty, starts claude in their own repository with ANTHROPIC_BASE_URL set to acme's gateway "
        "(made up, under example.com), then works as usual. Key event: the claude exec, whose kept environment "
        "carries the URL."
    ),
}


def build(mac):
    sh = mac.terminal(cwd="~/code/acme-api")
    mac.wait(6)
    mac.run(sh, "git", ["git", "status"], wait=0.08)
    mac.wait(4)
    agent = es.claude(mac, sh, "~/code/acme-api", env_add={"ANTHROPIC_BASE_URL": "https://llm-gateway.acme.example.com"}, key=True)
    mac.wait(12)
    es.claude_prompt(mac, agent)
    mac.wait(5)
    for f in ("src/main.rs", "src/routes.rs"):
        mac.read_file(agent, f"~/code/acme-api/{f}")
    mac.wait(20)
    es.claude_prompt(mac, agent)
    mac.wait(30)
    mac.exit(agent)
    mac.logout(sh)
