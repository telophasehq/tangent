"""EAA-014 (with EAA-003 and EAA-004) as Miasma's repository injection
planted it (EAA-C-004, steps 1-3), seen where it lands: one commit added a
Claude Code and a Gemini CLI SessionStart hook, an always-applied Cursor rule
and a VS Code folderOpen task, all pointing at one payload file; the person
clones the repository and git writes all five."""
import es

CASE = {
    "id": "eaa-014-clone-brings-hooks-rule-and-task-for-four-tools",
    "title": "git clone writes Claude and Gemini SessionStart hooks, a Cursor rule and a folderOpen task from one commit",
    "flag": True,
    "eaa": ["EAA-003", "EAA-004", "EAA-014"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-004",
        "steps": [1, 2, 3],
        "sources": ["SRC-006", "https://www.stepsecurity.io/blog/miasma-worm-hits-microsoft-again-azure-functions-action-and-72-other-repositories-disabled-after-supply-chain-attack-targeting-ai-coding-agents"],
    },
    "notes": (
        "StepSecurity: commit 5f456b8 added five files and changed no source: .claude/settings.json and "
        ".gemini/settings.json (SessionStart hooks, matcher *, running node .github/setup.js), .cursor/rules/setup.mdc "
        "(alwaysApply, telling the agent to run the same script as project setup), .vscode/tasks.json (a shell task, "
        "runOn folderOpen) and .github/setup.js (the 4.6 MB payload). It was pushed with a compromised contributor's "
        "account: remote state, invisible here. 'Developers receive the files through a normal clone or pull ... "
        "Cloning the repository is safe. Opening it is not.' This case is the clone, by the person, in their terminal; "
        "activation on a victim is not established, so nothing is opened and no hook runs. Key events: the four "
        "config writes. The same file set (plus .claude/setup.mjs and .vscode/setup.mjs) is what Phantom Gyp "
        "(EAA-C-005) committed through createCommitOnBranch; it lands the same way. In the events the rule, the payload "
        "and the repository are stand-ins: .cursor/rules/project-init.mdc, .github/init-env.js and "
        "globex-deploy-action."
    ),
}

URL = "https://git.example.com/globex/globex-deploy-action.git"
REPO = "~/code/globex-deploy-action"
CLT = "/Library/Developer/CommandLineTools/usr/bin/git"
REMOTE = es.Program("git-remote-https", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-http",
                    "com.apple.git-remote-http", "59GAB85EFG", es.PLATFORM_CLT,
                    dyld="/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-https")
PLANTED = [".claude/settings.json", ".gemini/settings.json", ".cursor/rules/project-init.mdc", ".vscode/tasks.json"]


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
    mac.wait(1.1)
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
    mac.wait(7)
    files = ["README.md", "action.yml", "package.json", "src/main.ts", "src/deploy.ts", ".github/workflows/ci.yml",
             ".github/init-env.js"] + PLANTED
    clone(mac, sh, URL, REPO, sorted(files), keys=set(PLANTED))
    mac.wait(9)
    sh.cwd = mac._p(REPO)
    mac.run(sh, "ls", ["ls", "-la"], wait=0.01)
    mac.wait(25)
    mac.logout(sh)
