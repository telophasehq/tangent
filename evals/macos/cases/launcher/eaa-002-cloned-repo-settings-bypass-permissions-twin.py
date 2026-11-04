"""The twin of eaa-002-cloned-repo-settings-bypass-permissions: the person
clones a team repository whose committed .claude/settings.json is the
team's shared project settings, as Claude Code's documentation has teams do,
and starts the current Claude Code in it."""
import es

CASE = {
    "id": "eaa-002-cloned-repo-settings-bypass-permissions-twin",
    "title": "git clone writes a team repository's committed .claude/settings.json; the current Claude Code starts in it",
    "flag": False,
    "eaa": ["EAA-002"],
    "group": "launcher",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-011", "https://code.claude.com/docs/en/settings"],
    },
    "notes": (
        "Plainly benign: project settings (.claude/settings.json) are meant to be committed and shared, so a clone "
        "writing one is ordinary. The same writer (git's checkout) and the same file as the attack case; the "
        "differences are the setting inside it, which eslogger cannot show, and the agent, the current 2.1.318, "
        "which shows the trust dialog whatever the file says. Key events: git's create, write and close of the "
        "settings file. acme-infra is a made-up repository."
    ),
}

REPO = "~/code/acme-infra"
URL = "https://example.com/acme/acme-infra.git"
REMOTE = es.Program("git-remote-https", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-http",
                    "com.apple.git-remote-http", "59GAB85EFG", es.PLATFORM_CLT,
                    dyld="/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-https", size=4681600)
HOOKS = ["applypatch-msg", "commit-msg", "fsmonitor-watchman", "post-update", "pre-applypatch", "pre-commit",
         "pre-merge-commit", "pre-push", "pre-rebase", "pre-receive", "prepare-commit-msg", "push-to-checkout",
         "sendemail-validate", "update"]


def clone(mac, sh, url, repo, files, *, key_paths=()):
    """`git clone url` from `sh` in ~/code, as CLT git writes a clone. Returns the events of `key_paths`."""
    repo = mac._p(repo)
    keys = []
    g = mac.spawn(sh, "git", ["git", "clone", url], cwd="~/code")

    def config():  # git config's lock file, renamed over (or to) .git/config
        mac.write_file(g, f"{repo}/.git/config.lock", new=True)
        mac.rename(g, f"{repo}/.git/config.lock", f"{repo}/.git/config")

    config()
    for f in ("HEAD", "description", "info/exclude"):
        mac.write_file(g, f"{repo}/.git/{f}", new=True)
    for h in HOOKS:  # the template's sample hooks
        mac.write_file(g, f"{repo}/.git/hooks/{h}.sample", new=True)
    r = mac.spawn(g, REMOTE, ["git-remote-https", "origin", url], fds="pipe")
    mac.wait(2.1)
    pack = f"{repo}/.git/objects/pack/tmp_pack_{mac.rng.getrandbits(24):06x}"
    mac.write_file(r, pack, new=True)
    mac.exit(r)
    mac.rename(g, pack, f"{repo}/.git/objects/pack/pack-{mac.rng.getrandbits(160):040x}.pack")
    config()  # remote.origin
    config()  # branch.main
    mac.write_file(g, f"{repo}/.git/packed-refs", new=True)
    for f in files:  # checkout
        ev = mac.write_file(g, f"{repo}/{f}", new=True)
        if f in key_paths:
            keys += ev
    mac.write_file(g, f"{repo}/.git/index.lock", new=True)
    mac.rename(g, f"{repo}/.git/index.lock", f"{repo}/.git/index")
    mac.wait(0.05)
    mac.exit(g)
    return keys


def build(mac):
    sh = mac.terminal("Ghostty", cwd="~/code")
    mac.wait(5)
    keys = clone(mac, sh, URL, REPO, ["README.md", "CLAUDE.md", "terraform/main.tf", "terraform/variables.tf",
                                      ".claude/settings.json"], key_paths=(".claude/settings.json",))
    mac.key(keys)
    mac.wait(12)
    sh.cwd = mac._p(REPO)
    agent = es.claude(mac, sh, REPO)
    mac.wait(25)
    es.claude_prompt(mac, agent)
    mac.wait(80)
    mac.exit(agent)
    mac.wait(8)
    mac.logout(sh)
