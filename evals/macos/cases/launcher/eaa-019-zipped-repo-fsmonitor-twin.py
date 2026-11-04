"""The twin of the EAA-019 cases: the person turns on git's built-in file
system monitor in their own large repository (`git config core.fsmonitor
true`); later the `git status` Claude Code runs for them starts git's
fsmonitor daemon, a git child of a git child of the agent."""
import es

CASE = {
    "id": "eaa-019-zipped-repo-fsmonitor-twin",
    "title": "The person sets core.fsmonitor true; a git status Claude Code runs starts git fsmonitor--daemon",
    "flag": False,
    "eaa": ["EAA-019"],
    "group": "launcher",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-063", "https://git-scm.com/docs/git-fsmonitor--daemon"],
    },
    "notes": (
        "Plainly benign: core.fsmonitor=true is git's documented built-in monitor (git-fsmonitor--daemon, macOS "
        "supported; CLT git 2.50 ships it as a link to git), set by the rightful user with git config, which "
        "writes .git/config through config.lock renamed over it. The technique page's hunt ideas: 'A legitimate "
        "helper configuration alone is not evidence of abuse'; hunting.md H14: 'some Git commands intentionally "
        "invoke configured helpers'. The git status here is a tool call the person asked the agent for, through "
        "its tool shell; git, finding no daemon, runs `git fsmonitor--daemon start` (fsmonitor-ipc.c "
        "spawn_daemon), which starts `git fsmonitor--daemon run --detach --ipc-threads=8` in the background "
        "(start_bg_command, stdio closed; --detach frees a console only on Windows, so no new session) and "
        "returns; the daemon's socket is .git/fsmonitor--daemon.ipc. The spawn arguments are from git 2.50's "
        "source, not a capture. "
        "Key events: the git config rename over .git/config and the two fsmonitor--daemon execs."
    ),
}

API = "~/code/acme-api"
# git runs its own subcommands as `git` from its exec path, which CLT links to bin/git
_GIT = es.PROGRAMS["clt-git"]
EXEC_GIT = es.Program("exec-path-git", _GIT.path, _GIT.signing_id, _GIT.team_id, _GIT.cs,
                      dyld="/Library/Developer/CommandLineTools/usr/libexec/git-core/git", size=_GIT.size)


def build(mac):
    repo = mac._p(API)
    sh = mac.terminal(cwd=API)
    mac.wait(7)
    gc = mac.spawn(sh, "git", ["git", "config", "core.fsmonitor", "true"])
    mac.read_file(gc, f"{repo}/.git/config")
    mac.write_file(gc, f"{repo}/.git/config.lock", new=True)
    mac.rename(gc, f"{repo}/.git/config.lock", f"{repo}/.git/config", key=True)
    mac.exit(gc)
    mac.wait(20)

    agent = es.claude(mac, sh, API)
    mac.wait(10)
    es.claude_prompt(mac, agent)  # "what have I changed on this branch?"
    tool = es.agent_shell(mac, agent, "git status")
    g = mac.spawn(tool, "git", ["git", "status"])
    mac.read_file(g, f"{repo}/.git/config")
    start = mac.spawn(g, EXEC_GIT, ["git", "fsmonitor--daemon", "start"], fds="null", key=True)
    daemon = mac.spawn(start, EXEC_GIT, ["git", "fsmonitor--daemon", "run", "--detach", "--ipc-threads=8"],
                       fds="null", key=True)
    mac.create(daemon, f"{repo}/.git/fsmonitor--daemon.ipc")
    mac.wait(0.3)
    mac.exit(start)
    mac.read_file(g, f"{repo}/.git/index")
    mac.exit(g)
    mac.exit(tool)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(60)
    mac.exit(agent)
    mac.wait(4)
    mac.logout(sh)
    # the daemon keeps running, watching the repository
