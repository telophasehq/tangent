"""The twin of eaa-014-clone-brings-hooks-rule-and-task-for-four-tools: the
person clones their company's monorepo, which keeps instruction files for
the several assistants its engineers use, and git writes them with the rest
of the tree."""
import es

CASE = {
    "id": "eaa-014-clone-brings-team-agent-docs-twin",
    "title": "git clone of acme's monorepo writes its CLAUDE.md, AGENTS.md, GEMINI.md, Cursor rule and Copilot instructions",
    "flag": False,
    "eaa": ["EAA-014"],
    "group": "control-plane",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-013"]},
    "notes": (
        "Plainly benign: a team commits instruction files for each assistant it uses (CLAUDE.md per SRC-013, and the "
        "others' equivalents); the person clones the repository in their terminal. One process writes several vendors' "
        "agent files, as in the Miasma clone, but they are instructions committed by the team, with no hooks, no task "
        "and no payload. Key events: the five instruction writes."
    ),
}

URL = "https://git.example.com/acme/acme-platform.git"
REPO = "~/code/acme-platform"
CLT = "/Library/Developer/CommandLineTools/usr/bin/git"
REMOTE = es.Program("git-remote-https", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-http",
                    "com.apple.git-remote-http", "59GAB85EFG", es.PLATFORM_CLT,
                    dyld="/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-https")
DOCS = ["CLAUDE.md", "AGENTS.md", "GEMINI.md", ".cursor/rules/style.mdc", ".github/copilot-instructions.md"]


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
    mac.wait(2.4)
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
    mac.wait(6)
    files = ["README.md", "package.json", "pnpm-workspace.yaml", "apps/web/package.json", "apps/web/src/main.ts",
             "services/billing/Cargo.toml", "services/billing/src/main.rs", ".github/workflows/ci.yml"] + DOCS
    clone(mac, sh, URL, REPO, sorted(files), keys=set(DOCS))
    mac.wait(8)
    sh.cwd = mac._p(REPO)
    mac.run(sh, "ls", ["ls"], wait=0.01)
    mac.wait(25)
    mac.logout(sh)
