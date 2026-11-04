"""The twin of eaa-016-c2-scout-enumerates-agent-stores: Claude Code itself,
through its own UI, lists the person's past sessions for this repository
(`claude --resume`): it opens its project directory, reads each transcript
there, and appends to the one the person picks."""
import es

CASE = {
    "id": "eaa-016-c2-scout-enumerates-agent-stores-twin",
    "title": "The person types claude --resume; Claude Code reads its own transcripts for the repository to list them",
    "flag": False,
    "eaa": ["EAA-016"],
    "group": "launcher",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-057", "https://code.claude.com/docs/en/cli-reference"],
    },
    "notes": (
        "Plainly benign: the agent enumerating and reading its own state for its own resume picker, started by "
        "the person at their shell. The session directory the scout case walks, here walked by the program that "
        "wrote it, and only its own vendor's. How --resume reads the transcripts is not in the capture: written "
        "as one opendir of the project directory and an open and close of each transcript; the directory open is "
        "assumed from opendir's open(2). Key events: the transcript reads."
    ),
}

WEB = "~/code/acme-web"
O_DIRECTORY = 0x100000


def opendir(mac, proc, path):
    """opendir(3) and closedir(3): an open of the directory for reading, and its close."""
    path = mac._p(path)
    ev = mac.open(proc, path, es.FREAD | es.O_NONBLOCK | O_DIRECTORY | es.O_CLOEXEC)
    ev["event"]["open"]["file"] = mac._dir(path)
    mac.wait(0.0002)
    cl = mac.close(proc, path)
    cl["event"]["close"]["target"] = mac._dir(path)
    return ev


def build(mac):
    proj = es.project_dir(mac._p(WEB))
    past = [mac.exists(f"{proj}/{mac.uuid().lower()}.jsonl", size=40000 + i * 9000, days=1 + i * 2) for i in range(4)]
    sh = mac.terminal(cwd=WEB)
    mac.wait(5)
    agent = es.claude(mac, sh, WEB, ["claude", "--resume"])
    mac.wait(0.3)
    opendir(mac, agent, proj)
    for f in past:
        mac.read_file(agent, f, key=True)
    mac.wait(9)  # the person picks the second one
    agent.session = past[1]
    mac.read_file(agent, agent.session)
    es.claude_prompt(mac, agent)
    mac.wait(70)
    mac.exit(agent)
    mac.wait(5)
    mac.logout(sh)
