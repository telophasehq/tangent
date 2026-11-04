"""EAA-002 as Anthropic's advisory GHSA-mmgp-wc2j-qcv7 describes it
(EAA-C-016, step 1): the person clones a repository whose committed
.claude/settings.json sets permissions.defaultMode to bypassPermissions, and
opens it in a Claude Code older than 2.1.53, which read that mode before
deciding to show the workspace trust dialog, so the dialog never showed and
the session started in bypass mode."""
import es

CASE = {
    "id": "eaa-002-cloned-repo-settings-bypass-permissions",
    "title": "git clone writes a repository's committed .claude/settings.json, then Claude Code 2.1.50 starts in it",
    "flag": True,
    "eaa": ["EAA-002"],
    "group": "launcher",
    "grounding": {
        "catalog_case": "EAA-C-016",
        "steps": [1],
        "sources": ["SRC-011", "https://github.com/anthropics/claude-code/security/advisories/GHSA-mmgp-wc2j-qcv7"],
    },
    "notes": (
        "Advisory (SRC-011): Claude Code resolved the permission mode from settings files, the repo-controlled "
        ".claude/settings.json included, before deciding whether to show the workspace trust dialog; a malicious "
        "repository's committed settings with permissions.defaultMode bypassPermissions skipped the dialog on "
        "first open. Affected: earlier than 2.1.53, so dev here runs 2.1.50 (manual updates, which the advisory "
        "names). What eslogger shows: git's checkout writing the committed .claude/settings.json in the fresh clone, "
        "and the affected claude (its version in its path) starting there, the start that enters bypass mode (the "
        "key events: those writes and that exec); its read of the file is a settings read, which the plugin drops "
        "by design. The setting itself is file content and the skipped dialog is the agent's UI: neither is in any "
        "event. The advisory establishes the permission-mode bypass, not a downstream action, so the case ends "
        "with the person's first prompt and the agent's reply; no tool use is invented. git clone is as CLT git "
        "2.50 does it (config, template hooks, git-remote-https, pack, checkout), the fetch itself is network. "
        "globex-tools is a made-up repository."
    ),
}

REPO = "~/code/globex-tools"
URL = "https://example.com/globex/globex-tools.git"
CLAUDE = es.Program("claude-2.1.50", f"{es.HOME}/.local/share/claude/versions/2.1.50", "com.anthropic.claude-code",
                    "Q6L2SF6YDW", es.DEVID, dyld=f"{es.HOME}/.local/bin/claude", size=211467312)
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
    mac.wait(1.4)
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


def claude(mac, sh, cwd, prog, args=None, *, key=False):
    """es.claude, for a Claude Code other than the installed one."""
    cwd = mac._p(cwd)
    c = mac.spawn(sh, prog, args or ["claude"], cwd=cwd, key=key)
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
    mac.atomic_write(c, "~/.claude.json")
    return c


def build(mac):
    sh = mac.terminal(cwd="~/code")
    mac.wait(6)
    keys = clone(mac, sh, URL, REPO, ["README.md", "package.json", "src/index.ts", "src/cli.ts", ".claude/settings.json"],
                 key_paths=(".claude/settings.json",))
    mac.key(keys)
    mac.wait(9)
    sh.cwd = mac._p(REPO)
    agent = claude(mac, sh, REPO, CLAUDE, key=True)  # reads the repository's .claude/settings.json; no trust dialog
    mac.wait(20)
    es.claude_prompt(mac, agent)
    mac.wait(60)
    mac.exit(agent)
    mac.wait(10)
    mac.logout(sh)
