"""EAA-019 as Manifold's GitSpawn research found it in goose (EAA-C-026,
step 1): a repository delivered as a .zip with a .git/config whose
core.fsmonitor names a command; `goose review` in it runs `git diff` (with
only core.quotePath=off added) before contacting the model, and git runs
the command."""
import es

CASE = {
    "id": "eaa-019-zipped-repo-fsmonitor-at-goose-review",
    "title": "goose 1.41.0's review runs git -c core.quotePath=off diff in an unzipped repository, which runs its "
             "core.fsmonitor command",
    "flag": True,
    "eaa": ["EAA-019"],
    "group": "launcher",
    "grounding": {
        "catalog_case": "EAA-C-026",
        "steps": [1],
        "sources": ["SRC-063", "https://www.manifold.security/blog/ai-coding-agents-git-hijack"],
    },
    "notes": (
        "Manifold (SRC-063): goose runs `git diff` with core.quotePath=off added and nothing else stripped, during "
        "`goose review`, before the model is contacted; the trigger is the repository's core.fsmonitor (git diff "
        "refreshes the index); goose 1.41.0 affected, fixed in 1.44.0. Delivery as for every proof of concept: a "
        ".zip with .git included (clone, fetch and pull do not carry .git/config). The article describes the "
        "payload only for Claude Code (a PWNED marker file): the Claude Code case's stand-in, `touch MARKER`, "
        "stands in here. The exact git diff "
        "arguments beyond the -c are not given: written as `git -c core.quotePath=off diff`. goose is a single "
        "binary installed in ~/.local/bin; its signature is not known here (ad hoc assumed, as a Rust build "
        "links), and its version is not in its path. The hook runs as git's run-command runs one with a space, "
        "`/bin/sh -c '<cmd> \"$@\"' '<cmd>' 2 <token>` (git's source, not a capture; the token is the current time "
        "in nanoseconds, as git gives an index with no fsmonitor extension). What goose does after the "
        "diff (the model call) is network. Key events: the hook's sh and touch execs under git under goose. "
        "globex-ledger is a made-up repository."
    ),
}

REPO = "~/code/globex-ledger"
ZIP = "~/Downloads/globex-ledger.zip"
HOOK = "touch MARKER"
UNZIP = es.Program("unzip", "/usr/bin/unzip", "com.apple.unzip", size=437216)  # codesign on macOS 26.3
TOUCH = es.Program("touch", "/usr/bin/touch", "com.apple.touch", size=101792)
GOOSE = es.dropped("~/.local/bin/goose")
FILES = [".git/config", ".git/HEAD", ".git/description", ".git/index", ".git/packed-refs",
         ".git/objects/pack/pack-9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d.pack",
         ".git/objects/pack/pack-9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d.idx", ".git/refs/heads/main",
         "README.md", "Cargo.toml", "Cargo.lock", "src/main.rs", "src/ledger.rs"]


def build(mac):
    mac.exists(ZIP, size=921600, days=0)
    repo = mac._p(REPO)
    sh = mac.terminal("iTerm", cwd="~/code")
    mac.wait(5)
    u = mac.spawn(sh, UNZIP, ["unzip", "-q", mac._p(ZIP)])
    mac.read_file(u, ZIP)
    for f in FILES:
        mac.write_file(u, f"{repo}/{f}", new=True)
    mac.exit(u)
    mac.wait(14)

    sh.cwd = repo
    goose = mac.spawn(sh, GOOSE, ["goose", "review"])
    mac.wait(0.4)
    g = mac.spawn(goose, "git", ["git", "-c", "core.quotePath=off", "diff"], fds="pipe")
    mac.read_file(g, f"{repo}/.git/config")
    mac.read_file(g, f"{repo}/.git/index")
    token = str(mac.t)  # no fsmonitor extension in the index: git starts from the current time
    hook = mac.spawn(g, "sh", ["/bin/sh", "-c", f'{HOOK} "$@"', HOOK, "2", token], fds="pipe", key=True)
    mac.exec(hook, TOUCH, ["touch", "MARKER", "2", token], key=True)
    for f in ("MARKER", "2", token):
        mac.create(hook, f"{repo}/{f}")
    mac.exit(hook)
    mac.wait(0.02)
    mac.exit(g)
    mac.wait(35)  # the review, from the model
    mac.exit(goose)
    mac.wait(10)
    mac.logout(sh)
