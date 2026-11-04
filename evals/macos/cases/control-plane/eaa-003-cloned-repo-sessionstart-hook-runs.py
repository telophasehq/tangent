"""EAA-003 as Check Point demonstrated it (EAA-C-015, step 1): the person
clones an untrusted repository whose committed .claude/settings.json holds a
SessionStart hook, runs `claude` in it and accepts the workspace trust
dialog; the hook's command runs at startup with no separate approval."""
import es

CASE = {
    "id": "eaa-003-cloned-repo-sessionstart-hook-runs",
    "title": "A cloned repository's .claude/settings.json SessionStart hook runs a command when Claude Code starts in it",
    "flag": True,
    "eaa": ["EAA-003"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-015",
        "steps": [1],
        "sources": ["SRC-023", "SRC-040", "SRC-012",
                    "https://research.checkpoint.com/2026/rce-and-api-token-exfiltration-through-claude-code-project-files-cve-2025-59536/"],
    },
    "notes": (
        "Check Point: the hook was in the project's .claude/settings.json (SessionStart, matcher startup); the victim "
        "cloned the project and ran claude in it; after general workspace trust the hook ran without the approval bash "
        "commands need. The proof of concept opened Calculator (`open -na Calculator`); the events use a stand-in, "
        "`open -a TextEdit`. Affected versions were before 1.0.87 (SRC-040); the version string here is the framework's. Not in "
        "the source and so chosen: an https clone, and the hook in a session of its own with piped stdio. The hooks "
        "documentation (SRC-012) has a command hook's string run by `sh -c` on macOS, with CLAUDE_PROJECT_DIR (the "
        "project root) in its environment. TextEdit itself is started by "
        "launchd, not as a child of open, and is not written. Steps 2 and 3 (MCP auto-approval, ANTHROPIC_BASE_URL) "
        "are EAA-006 and EAA-007, not here. globex-quickstart is made up."
    ),
}

URL = "https://git.example.com/globex/globex-quickstart.git"
REPO = "~/code/globex-quickstart"
CLT = "/Library/Developer/CommandLineTools/usr/bin/git"
REMOTE = es.Program("git-remote-https", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-http",
                    "com.apple.git-remote-http", "59GAB85EFG", es.PLATFORM_CLT,
                    dyld="/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-https")
OPEN = es.Program("open", "/usr/bin/open", "com.apple.open")


def checkout(mac, git, path, key=False):
    """git writes a working-tree file: created, opened exclusive, written, closed."""
    out = [mac.create(git, path), mac.open(git, path, es.FWRITE | es.O_CREAT | es.O_EXCL)]
    mac.wait(0.0003)
    out.append(mac.close(git, path, modified=True))
    return mac.key(out) if key else out


def clone(mac, sh, url, dest, files, keys=()):
    g = mac.spawn(sh, "git", ["git", "clone", url])
    mac.write_file(g, f"{dest}/.git/config.lock", new=True)
    mac.rename(g, f"{dest}/.git/config.lock", f"{dest}/.git/config")
    mac.write_file(g, f"{dest}/.git/HEAD", new=True)
    r = mac.spawn(g, REMOTE, ["git-remote-https", "origin", url], fds="pipe")
    mac.wait(0.7)
    pack = f"{dest}/.git/objects/pack/pack-{mac.rng.getrandbits(160):040x}"
    ip = mac.spawn(g, "clt-git", [CLT, "index-pack", "--stdin", "--fix-thin", "--check-self-contained-and-connected"], fds="pipe")
    mac.write_file(ip, pack + ".pack", new=True)
    mac.write_file(ip, pack + ".idx", new=True)
    mac.exit(ip)
    mac.exit(r)
    for f in files:
        checkout(mac, g, f"{dest}/{f}", key=f in keys)
    mac.write_file(g, f"{dest}/.git/index.lock", new=True)
    mac.rename(g, f"{dest}/.git/index.lock", f"{dest}/.git/index")
    mac.wait(0.05)
    mac.exit(g)


def build(mac):
    sh = mac.terminal(cwd="~/code")
    mac.wait(5)
    clone(mac, sh, URL, REPO, ["README.md", "package.json", "src/index.js", ".claude/settings.json"],
          keys={".claude/settings.json"})
    mac.wait(12)
    sh.cwd = mac._p(REPO)  # cd globex-quickstart
    agent = es.claude(mac, sh, REPO)
    mac.wait(6)
    mac.atomic_write(agent, "~/.claude.json")  # the trust dialog accepted

    hook = mac.spawn(agent, "sh", ["/bin/sh", "-c", "open -a TextEdit"], cwd=REPO, setsid=True, fds="pipe",
                     env_add={"CLAUDE_PROJECT_DIR": mac._p(REPO)}, key=True)
    mac.exec(hook, OPEN, ["open", "-a", "TextEdit"], key=True)
    mac.wait(0.4)
    mac.exit(hook)

    mac.wait(20)
    es.claude_prompt(mac, agent)
    mac.wait(30)
    mac.exit(agent)
    mac.logout(sh)
