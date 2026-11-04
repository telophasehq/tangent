"""EAA-019 as Manifold's GitSpawn research demonstrated it on Claude Code
(EAA-C-026, step 1): a repository arrives as a .zip with its .git directory,
whose .git/config sets core.fsmonitor to a command; the person unzips it and
starts Claude Code 2.1.193 in it, and the `git status` Claude Code runs at
startup, before the trust prompt is answered, runs that command."""
import es

CASE = {
    "id": "eaa-019-zipped-repo-fsmonitor-at-claude-startup",
    "title": "Claude Code 2.1.193's startup git status in an unzipped repository runs its core.fsmonitor command, "
             "which drops a marker file",
    "flag": True,
    "eaa": ["EAA-019"],
    "group": "launcher",
    "grounding": {
        "catalog_case": "EAA-C-026",
        "steps": [1],
        "sources": ["SRC-063", "https://www.manifold.security/blog/ai-coding-agents-git-hijack"],
    },
    "notes": (
        "Manifold (SRC-063): Claude Code runs `git status` as an internal subprocess at startup, before the "
        "workspace-trust prompt is accepted; git refreshes the index, which runs the command named by the "
        "repository's core.fsmonitor; confirmed on 2.1.193, fixed in 2.1.196. Delivery: clone, fetch and pull do "
        "not carry another repository's .git/config, so the repository must arrive as files with .git included; "
        "every proof of concept used a .zip. Payload: the startup git status 'drops the PWNED marker file' while the "
        "trust prompt waits; the article shows the setting only as `fsmonitor = <command>`, so `touch MARKER` "
        "stands in for the command (MARKER a stand-in for the article's marker name). Written here: the zip already in ~/Downloads (its download is the browser's and the "
        "network's), `unzip` typed by the person, which writes .git/config among the rest; `claude` 2.1.193 typed "
        "in the unzipped tree; Claude Code's git status (the article quotes `git status --porcelain=2 --branch` "
        "as an example of such calls, not as Claude Code's exact arguments), started as Claude Code starts its "
        "other children here (own session, piped stdio); git running the hook as run-command does for a command "
        "with a space, `/bin/sh -c '<cmd> \"$@\"' '<cmd>' 2 <token>` (hook protocol 2), the shell exec'ing touch, "
        "which creates MARKER (and, given git's two arguments, files named 2 and the token). The hook's arguments "
        "are from git's source (2.50 fsmonitor.c, run-command.c), not a capture: the token is the index's "
        "fsmonitor extension's, and an index without one (what the zip's has is not in the source) gets the "
        "current time in nanoseconds (add_fsmonitor). The trust prompt is the agent's UI; what the person answers is not "
        "in the source: they close it. The config value is file content, which eslogger does not show. Key events: "
        "the hook's sh and touch execs under git under claude. globex-ui is a made-up repository."
    ),
}

REPO = "~/code/globex-ui"
ZIP = "~/Downloads/globex-ui.zip"
HOOK = "touch MARKER"
UNZIP = es.Program("unzip", "/usr/bin/unzip", "com.apple.unzip", size=437216)  # codesign on macOS 26.3
TOUCH = es.Program("touch", "/usr/bin/touch", "com.apple.touch", size=101792)
CLAUDE = es.Program("claude-2.1.193", f"{es.HOME}/.local/share/claude/versions/2.1.193", "com.anthropic.claude-code",
                    "Q6L2SF6YDW", es.DEVID, dyld=f"{es.HOME}/.local/bin/claude", size=298411824)
FILES = [".git/config", ".git/HEAD", ".git/description", ".git/index", ".git/packed-refs",
         ".git/objects/pack/pack-4c1d0b7e93a25f68e0d1c2b3a4f5e6d7c8b9a0f1.pack",
         ".git/objects/pack/pack-4c1d0b7e93a25f68e0d1c2b3a4f5e6d7c8b9a0f1.idx", ".git/refs/heads/main",
         "README.md", "package.json", "src/App.tsx", "src/main.tsx", "vite.config.ts"]


def claude(mac, sh, cwd, prog, args=None):
    """es.claude, for a Claude Code other than the installed one."""
    cwd = mac._p(cwd)
    c = mac.spawn(sh, prog, args or ["claude"], cwd=cwd)
    c.env.update(CLAUDECODE="1", CLAUDE_CODE_ENTRYPOINT="cli", GIT_EDITOR="true", COREPACK_ENABLE_AUTO_PIN="0")
    c.env.pop("_", None)
    c.session = f"{es.project_dir(cwd)}/{mac.uuid().lower()}.jsonl"
    c.snapshot = f"{es.HOME}/.claude/shell-snapshots/snapshot-zsh-{mac.t // 10**6}-{mac.rng.getrandbits(30):06x}.sh"
    for f in ("~/.claude/settings.json", f"{cwd}/.claude/settings.json", f"{cwd}/CLAUDE.md", "~/.claude.json"):
        if mac._p(f) in mac.files:
            mac.read_file(c, f)
    snap = mac.spawn(c, "zsh", ["/bin/zsh", "-c", "-l", f"SNAPSHOT_FILE={c.snapshot}; source ~/.zshrc < /dev/null; "
                                "typeset -f > $SNAPSHOT_FILE"], setsid=True)
    mac.read_file(snap, "~/.zshrc")
    mac.write_file(snap, c.snapshot)
    mac.exit(snap)
    mac.run(c, "security", ["security", "find-generic-password", "-a", es.USER, "-w", "-s", "Claude Code-credentials"],
            setsid=True, wait=0.03)
    return c


def build(mac):
    mac.exists(ZIP, size=1843200, days=0)
    repo = mac._p(REPO)
    sh = mac.terminal(cwd="~/code")
    mac.wait(6)
    u = mac.spawn(sh, UNZIP, ["unzip", "-q", mac._p(ZIP)])
    mac.read_file(u, ZIP)
    for f in FILES:
        mac.write_file(u, f"{repo}/{f}", new=True)
    mac.exit(u)
    mac.wait(11)

    sh.cwd = repo
    agent = claude(mac, sh, repo, CLAUDE)
    # startup context: git status, before the trust prompt is answered
    g = mac.spawn(agent, "git", ["git", "status", "--porcelain=2", "--branch"], setsid=True, fds="pipe")
    mac.read_file(g, f"{repo}/.git/config")
    mac.read_file(g, f"{repo}/.git/index")
    token = str(mac.t)  # no fsmonitor extension in the index: git starts from the current time
    hook = mac.spawn(g, "sh", ["/bin/sh", "-c", f'{HOOK} "$@"', HOOK, "2", token], fds="pipe", key=True)
    mac.exec(hook, TOUCH, ["touch", "MARKER", "2", token], key=True)
    for f in ("MARKER", "2", token):
        mac.create(hook, f"{repo}/{f}")
    mac.exit(hook)
    mac.wait(0.01)
    mac.write_file(g, f"{repo}/.git/index.lock", new=True)
    mac.rename(g, f"{repo}/.git/index.lock", f"{repo}/.git/index")
    mac.exit(g)
    mac.atomic_write(agent, "~/.claude.json")
    mac.wait(25)  # the trust prompt, unanswered; the person closes it
    mac.exit(agent, status=130)
    mac.wait(6)
    mac.logout(sh)
