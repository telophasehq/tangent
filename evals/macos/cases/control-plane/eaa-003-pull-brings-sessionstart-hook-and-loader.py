"""EAA-003 as Mini Shai-Hulud planted it (EAA-C-003, step 1), seen where it
lands: the payload committed a project SessionStart hook, a VS Code
folderOpen task and the loader and payload they run into branches of
repositories it could write with stolen credentials; the person pulls one of
those branches and git writes them into the working tree."""
import es

CASE = {
    "id": "eaa-003-pull-brings-sessionstart-hook-and-loader",
    "title": "git pull writes a .claude/settings.json SessionStart hook, a folderOpen task and their loader into acme-api",
    "flag": True,
    "eaa": ["EAA-003"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-003",
        "steps": [1],
        "sources": ["SRC-005", "https://www.stepsecurity.io/blog/a-mini-shai-hulud-has-appeared"],
    },
    "notes": (
        "StepSecurity: the payload committed, through the GitHub API with stolen tokens, to up to 50 branches: "
        ".claude/settings.json (a SessionStart hook running node .vscode/setup.mjs), .vscode/tasks.json (runOn "
        "folderOpen, node .claude/setup.mjs), .claude/setup.mjs and .vscode/setup.mjs (the Bun loader) and "
        ".claude/execution.js (the 11.6 MB payload), with the message 'chore: update dependencies'. The commit is "
        "remote state, invisible here; on the infected host the install hook and its API calls show no control-plane "
        "write either. What a Mac sees is the arrival: the person pulls the branch and git (a `git merge` child of "
        "`git pull`) writes the files. The catalog does not confirm the hook fired on a victim, so no hook run is "
        "written. Key events: the two config writes; the loader and payload files are written too but are not "
        "config and not keyed. Their names in the events are stand-ins for the report's IOCs: .claude/prepare.mjs, "
        ".vscode/prepare.mjs and .claude/runtime.js. Branch and repository are made up."
    ),
}

REPO = "~/code/acme-api"
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


def pull(mac, sh, repo, files, keys=()):
    """git pull: a `git fetch` child (its transport a git-remote-https), then a `git merge` child that writes the tree."""
    g = mac.spawn(sh, "git", ["git", "pull"])
    f = mac.spawn(g, "clt-git", ["git", "fetch", "--update-head-ok"])
    r = mac.spawn(f, REMOTE, ["git-remote-https", "origin", "https://git.example.com/acme/acme-api.git"], fds="pipe")
    mac.wait(0.6)
    mac.exit(r)
    mac.write_file(f, f"{repo}/.git/FETCH_HEAD")
    mac.exit(f)
    m = mac.spawn(g, "clt-git", ["git", "merge", "FETCH_HEAD"])
    mac.write_file(m, f"{repo}/.git/index.lock", new=True)
    mac.write_file(m, f"{repo}/.git/ORIG_HEAD")
    for p in files:
        full = mac._p(f"{repo}/{p}")
        if full in mac.files:
            mac.unlink(m, full)
        checkout(mac, m, full, key=p in keys)
    mac.rename(m, f"{repo}/.git/index.lock", f"{repo}/.git/index")
    mac.exit(m)
    mac.exit(g)


def build(mac):
    mac.exists(f"{REPO}/.git/FETCH_HEAD")
    mac.exists(f"{REPO}/.git/ORIG_HEAD")
    sh = mac.terminal(cwd=REPO)
    mac.wait(6)
    mac.run(sh, "git", ["git", "status"], wait=0.08)
    mac.wait(3)
    pull(mac, sh, REPO, [".claude/settings.json", ".claude/prepare.mjs", ".claude/runtime.js",
                         ".vscode/tasks.json", ".vscode/prepare.mjs"],
         keys={".claude/settings.json", ".vscode/tasks.json"})
    mac.wait(8)
    mac.run(sh, "git", ["git", "log", "--oneline", "-3"], wait=0.05)
    mac.wait(20)
    mac.logout(sh)
