"""The twin of eaa-003-pull-brings-sessionstart-hook-and-loader: the person
pulls main and git writes a teammate's change to the project's committed
.claude/settings.json (its permission allowlist), with the source change
that went with it."""
import es

CASE = {
    "id": "eaa-003-pull-brings-team-claude-settings-twin",
    "title": "git pull writes a teammate's change to acme-web's committed .claude/settings.json",
    "flag": False,
    "eaa": ["EAA-003"],
    "group": "control-plane",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-016"]},
    "notes": (
        "Plainly benign: project settings in .claude/settings.json are meant to be committed and shared (SRC-016); a "
        "teammate allowed `npm run lint` and the person pulls it. Same writer, same kind of file as the Mini Shai-Hulud "
        "pull; what differs is the company (one config file, a source file, no loader or task) and, invisibly, the "
        "content. Key events: the settings write."
    ),
}

REPO = "~/code/acme-web"
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
    r = mac.spawn(f, REMOTE, ["git-remote-https", "origin", "https://git.example.com/acme/acme-web.git"], fds="pipe")
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
    for f in (".claude/settings.json", ".git/FETCH_HEAD", ".git/ORIG_HEAD", "package.json"):
        mac.exists(f"{REPO}/{f}", days=15)
    sh = mac.terminal(cwd=REPO)
    mac.wait(6)
    pull(mac, sh, REPO, [".claude/settings.json", "package.json", "src/app.ts"], keys={".claude/settings.json"})
    mac.wait(10)
    mac.run(sh, "git", ["git", "log", "--oneline", "-3"], wait=0.05)
    mac.wait(20)
    mac.logout(sh)
