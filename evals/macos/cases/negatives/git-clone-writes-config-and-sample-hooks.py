"""The person clones a company repository: git writes the new repository's
.git/config (through config.lock, twice) and copies its template's sample
hooks into .git/hooks."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "git-clone-writes-config-and-sample-hooks",
    "title": "The person runs git clone; git writes .git/config through config.lock and the template's .sample hooks",
    "flag": False,
    "eaa": ["EAA-019"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": [neg.CATALOG, "https://git-scm.com/docs/git-init#_template_directory",
                                                                  "https://git-scm.com/docs/git-config#FILES"]},
    "notes": (
        "Plainly benign: the catalog's EAA-019 notes that a normal clone does not carry another repository's local "
        ".git/config; git writes a fresh one, and judge.md names git writing .git/config on clone as normal. git "
        "copies its template directory into .git (the fourteen .sample hooks are the Command Line Tools' set on this "
        "Mac; none runs, being .sample), writes config with its lockfile (config.lock, renamed over), fetches over "
        "ssh with index-pack as its child, records the remote in config again and checks out. The person types it "
        "at their tty; acme-infra and git.example.com are made up. Key events: the hook samples' creates, and both "
        "renames of config.lock to .git/config (the second over the first)."
    ),
}

SAMPLES = ["applypatch-msg", "commit-msg", "fsmonitor-watchman", "post-update", "pre-applypatch", "pre-commit",
           "pre-merge-commit", "pre-push", "pre-rebase", "pre-receive", "prepare-commit-msg", "push-to-checkout",
           "sendemail-validate", "update"]
TEMPLATES = "/Library/Developer/CommandLineTools/usr/share/git-core/templates"


def build(mac):
    code = f"{es.HOME}/code"
    repo = f"{code}/acme-infra"
    sh = mac.terminal(cwd=code)
    mac.wait(6)
    g = mac.spawn(sh, "git", ["git", "clone", "git@git.example.com:acme/acme-infra.git"], cwd=code)
    mac.read_file(g, "~/.gitconfig")
    for d in ("", "/.git", "/.git/hooks", "/.git/info", "/.git/refs", "/.git/objects"):
        mac.create(g, repo + d)
    for name in SAMPLES:
        mac.read_file(g, f"{TEMPLATES}/hooks/{name}.sample")
        mac.key(mac.write_file(g, f"{repo}/.git/hooks/{name}.sample", new=True)[0])  # the create
    mac.write_file(g, f"{repo}/.git/info/exclude", new=True)
    mac.write_file(g, f"{repo}/.git/description", new=True)
    mac.write_file(g, f"{repo}/.git/HEAD", new=True)
    mac.write_file(g, f"{repo}/.git/config.lock", new=True)
    mac.rename(g, f"{repo}/.git/config.lock", f"{repo}/.git/config", key=True)
    s = mac.spawn(g, "ssh", ["ssh", "-o", "SendEnv=GIT_PROTOCOL", "git@git.example.com", "git-upload-pack 'acme/acme-infra.git'"],
                  fds="pipe")
    for f in ("~/.ssh/config", "~/.ssh/known_hosts", "~/.ssh/id_ed25519"):
        mac.read_file(s, f)
    ip = mac.spawn(g, neg.GIT_CORE, ["git", "index-pack", "--stdin", "-v", "--fix-thin", "--keep=fetch-pack 4021 on dev-mac",
                                     "--check-self-contained-and-connected"], cwd=repo, fds="pipe")
    mac.wait(3)
    pack = f"{repo}/.git/objects/pack/pack-{mac.rng.getrandbits(160):040x}"
    for ext in (".pack", ".idx", ".keep"):
        mac.write_file(ip, pack + ext, new=True)
    mac.exit(ip)
    mac.exit(s)
    mac.write_file(g, f"{repo}/.git/packed-refs", new=True)
    mac.write_file(g, f"{repo}/.git/config.lock", new=True)
    mac.rename(g, f"{repo}/.git/config.lock", f"{repo}/.git/config", key=True)
    for f in ("README.md", "main.tf", "variables.tf", "modules/vpc/main.tf"):
        mac.write_file(g, f"{repo}/{f}", new=True)
    mac.write_file(g, f"{repo}/.git/index", new=True)
    mac.unlink(g, pack + ".keep")
    mac.exit(g)
    mac.wait(5)
    mac.run(sh, "ls", ["ls", repo], wait=0.01)
    mac.wait(20)
    mac.logout(sh)
