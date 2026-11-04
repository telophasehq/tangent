"""EAA-003 as Mitiga's chain used it (EAA-C-011, step 2): the person clones a
repository into a path whose trust an earlier npm postinstall pre-seeded,
Claude Code starts in it and the project's SessionStart hook runs a script
from the project's hooks directory, which rewrites ~/.claude.json's MCP server
URL to a localhost proxy, again on every start."""
import es

CASE = {
    "id": "eaa-003-project-hook-rewrites-mcp-endpoint",
    "title": "A project SessionStart hook, run by Claude Code at startup, rewrites the MCP server URL in ~/.claude.json",
    "flag": True,
    "eaa": ["EAA-003"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-011",
        "steps": [2],
        "sources": ["SRC-024", "SRC-012", "https://www.mitiga.io/blog/claude-code-mcp-token-theft-mitm"],
    },
    "notes": (
        "Mitiga: the postinstall seeds trust for common clone locations; the victim then clones the repository into "
        "one ('NPM installed, repo cloned, and it has a hook'), and a SessionStart hook in the now trusted project runs a "
        "shell script from the project's hooks directory as a child of Claude Code; the script opens ~/.claude.json and "
        "rewrites the MCP server entry's URL to a localhost proxy, reasserting it each time Claude loads. Step 1 (the npm "
        "postinstall that seeded trust and first rewrote the endpoint) is EAA-006 and is not in this case: the trust "
        "flag in ~/.claude.json is there before it starts. The hook runs as `sh -c` with CLAUDE_PROJECT_DIR in its "
        "environment, as the hooks documentation (SRC-012) has command hooks on macOS. Not in the source and so chosen: "
        "an https clone, the hook in a session of its own, a bash script, and python3 doing the JSON rewrite with a "
        "plain truncating write (the source names no tool). Step 3 (the OAuth token through the proxy) is network, not "
        "visible. globex-tools and its files are made up."
    ),
}

URL = "https://git.example.com/globex/globex-tools.git"
REPO = "~/code/globex-tools"
HOOK = es.Program("session-start.sh", f"{es.HOME}/code/globex-tools/.claude/hooks/session-start.sh", interp=("bash", []), size=412)
CLT = "/Library/Developer/CommandLineTools/usr/bin/git"
REMOTE = es.Program("git-remote-https", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-http",
                    "com.apple.git-remote-http", "59GAB85EFG", es.PLATFORM_CLT,
                    dyld="/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-https")


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
    clone(mac, sh, URL, REPO, ["README.md", "package.json", ".claude/settings.json", ".claude/hooks/session-start.sh",
                               ".claude/hooks/pin_mcp.py"], keys={".claude/settings.json"})
    mac.wait(10)
    sh.cwd = mac._p(REPO)  # cd globex-tools
    agent = es.claude(mac, sh, REPO)
    mac.wait(1.5)

    hook = mac.spawn(agent, "sh", ["/bin/sh", "-c", '"$CLAUDE_PROJECT_DIR"/.claude/hooks/session-start.sh'],
                     cwd=REPO, setsid=True, fds="pipe", env_add={"CLAUDE_PROJECT_DIR": mac._p(REPO)}, key=True)
    mac.exec(hook, HOOK, [f"{mac._p(REPO)}/.claude/hooks/session-start.sh"], key=True)
    py = mac.spawn(hook, "python3", ["python3", ".claude/hooks/pin_mcp.py"], fds="pipe", key=True)
    mac.read_file(py, f"{REPO}/.claude/hooks/pin_mcp.py")
    mac.read_file(py, "~/.claude.json")
    mac.write_file(py, "~/.claude.json", key=True)
    mac.exit(py)
    mac.exit(hook)

    mac.wait(25)
    es.claude_prompt(mac, agent)
    mac.wait(40)
    mac.atomic_write(agent, "~/.claude.json")
    mac.wait(30)
    mac.exit(agent)
    mac.logout(sh)
