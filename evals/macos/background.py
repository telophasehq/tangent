"""The ordinary working day of dev's Mac, around the cases: synthetic, at about
the rates of a real 10-minute capture of a developer's Mac with agents at work.

    build(mac, start, hours, flush=None)   # gen.py calls it once per work day, mac.owner None

A day runs from `start` for `hours` (09:00-18:00Z as gen.py lays it). The
person arrives a few minutes in, steps out for lunch and a meeting or two
(Day), and leaves before the end. What happens is six tracks, each built for
the whole day in turn from its start, the clock rewound between them, so their
events interleave as on a real Mac once gen.py sorts the stream:

- `system`: launchd's jobs, with no tty (xpcproxy, then mdworker_shared, which
  Spotlight starts as files change; ssh-agent on first use), and cfprefsd
  saving preferences.
- `editor`: Cursor, open all day: its extension host runs git all the time for
  the source-control view, and a Rust language server that runs `cargo check`
  on save; the person saves files from it, now and then CLAUDE.md or AGENTS.md.
- `person`: Terminal tabs in ~/code/acme-api and ~/code/acme-web: git (fetch,
  pull and push over ssh), cargo, npm (most days a dev server all day, which
  reads the project's .env files), gh, aws (sso login most mornings), kubectl
  (whose exec plugin runs aws), docker, ssh, vim, uv in ~/code/globex-sdk, and
  now and then an npm install whose install scripts build a native module and
  write inside node_modules; one day in ten an alias added to ~/.zshrc in vim.
- `claude_web`, `claude_api`: Claude Code in acme-web (Terminal; with a local
  MCP server) and acme-api (Ghostty), sessions of one to two hours. At start:
  its settings read, the shell snapshot, the keychain and ~/.claude.json saved
  (es.claude), git for its context and, at the day's first start, the
  transcripts older than cleanupPeriodDays (30) unlinked. Per turn:
  history.jsonl appended, the transcript at the prompt, now and then as the
  tools run and at the reply (the capture's closes are a few a turn, not one
  a message), tool calls through its tool shell (git, rg, sed, npm test,
  cargo test, a commit, rarely a push or an install), file-history copies
  before edits, ~/.claude.json saved as a tmp file renamed over. Now and then
  a memory file or the project's settings.local.json. The first acme-web
  session of a day refreshes a plugin marketplace (one day in three, a second
  refresh): some 550 files written into a .staging directory, the live one
  renamed to .backup, staging renamed live, the backup unlinked, in seconds.
- `codex`: Codex (bun's global install) in ~/code/globex-sdk from late
  morning: its config and auth.json read, its log and the session's rollout
  file held open, history.jsonl appended, commands through `zsh -lc` (uv run
  pytest, git, rg).

Rates. The capture had, in 10 minutes, 291 execs, 240 forks, 242 exits, 33,951
closes, 1,759 creates, 1,104 unlinks and 311 renames (and a 30-second capture
2,789 opens); the plugin kept 2,553 records, 2,202 of them one marketplace
refresh: about 350 in 10 minutes without it. Every exec and every event on
the plugin's paths is generated as it happens. Over the 45 work hours of
gen.py's default with seed 1 the background has, per work hour: 1,396 execs
(1,800 in the busy afternoon hours, the capture's 1,746), and 2,211 records
the plugin keeps, 1,826 of them not opens (the capture's 2,100, which had no
opens), with a refresh of some 2,260 records most mornings. What the plugin
drops is sampled: forks and exits one in THIN (except an app's, daemon's or
launchd's forks, which tell the plugin's tree what those are: an exec's own
record names its parent, so a fork the tree can do without is one by an
exec'd process), about a tenth; creates, renames and unlinks off the
plugin's paths with chance DROPPED, a 20th to a 30th of the capture's; reads
and writes off them with chance SKIM, a 400th to a 700th (a tenth would be
50,000 events an hour). The stream is some 12 MB a work hour (1 MB gzipped).

Assumed, not seen: the git commands Claude Code runs for its context, the
local MCP server, Codex's files (held open or appended, from its docs) and
`zsh -lc` (es.agent_shell), the extension host's arguments and its git
commands (VS Code's source-control view), and the marketplace's layout. The
npm packages, the language server, the marketplace and its plugins are made
up; hosts are example.com and example.net."""
import random
import zlib

import es

S = 10**9
HOME = es.HOME
WEB, API, SDK = f"{HOME}/code/acme-web", f"{HOME}/code/acme-api", f"{HOME}/code/globex-sdk"

THIN = 10       # forks and exits: one in THIN of those the plugin's tree can do without (thinned)
DROPPED = 0.1   # creates, renames and unlinks off the plugin's paths
SKIM = 0.025    # reads and writes off the plugin's paths

P = es.Program
NM = f"{WEB}/node_modules"
BUNDLE = P("acme-bundle", f"{NM}/acme-bundler/bin/acme-bundle.js", dyld=f"{NM}/.bin/acme-bundle", interp=("env", ["node"]))
BUNDLE_CORE = es.dropped(f"{NM}/@acme/bundler-darwin-arm64/bin/acme-bundler-core")
TESTER = P("globex-test", f"{NM}/globex-test/bin/globex-test.js", dyld=f"{NM}/.bin/globex-test", interp=("env", ["node"]))
LINT = P("acme-lint", f"{NM}/acme-lint/bin/acme-lint.js", dyld=f"{NM}/.bin/acme-lint", interp=("env", ["node"]))
RUST_LSP = es.dropped(f"{HOME}/.cursor/extensions/acme.rust-lsp-2.3.1-darwin-arm64/server/acme-rust-lsp")
COMPOSE = P("docker-compose", "/Applications/Docker.app/Contents/Resources/cli-plugins/docker-compose", "com.docker.compose",
            "9BNSXJN65R", es.DEVID_TOOL)  # identifier and team from memory, not checked
_PY = es.PROGRAMS["python"]
VENV_PY = P("venv-python", _PY.path, _PY.signing_id, cs=_PY.cs, dyld=f"{SDK}/.venv/bin/python3", then="Python.app")
RUSTFMT = es.dropped(f"{es.TOOLCHAIN}/rustfmt")
CLIPPY = es.dropped(f"{es.TOOLCHAIN}/clippy-driver")
RUSTDOC = es.dropped(f"{es.TOOLCHAIN}/rustdoc")
TESTBINS = [es.dropped(f"{API}/target/debug/deps/{n}") for n in
            ("acme_api-5f0c2a9e41b7d3c8", "routes-0d8e6b3a9c1f4e72", "acme_core-b3a1d9e07c2f6a45")]
BUILD_SCRIPT = es.dropped(f"{API}/target/debug/build/acme-db-9c1f2e4b7a3d5e60/build-script-build")
DOCS_MCP = f"{HOME}/code/acme-tools/docs-mcp/dist/server.js"
MARKET = f"{HOME}/.claude/plugins/marketplaces/acme-marketplace"
MDWORKER = es.PROGRAMS["mdworker_shared"].path
SSO_CACHE = "~/.aws/sso/cache/5e3a1f0c9b2d4e6f7a8b9c0d1e2f3a4b5c6d7e8f.json"

SRC = {
    WEB: ["src/app.ts", "src/index.ts", "src/routes/session.ts", "src/routes/billing.ts", "src/lib/http.ts", "src/lib/cache.ts",
          "src/components/Header.tsx", "src/components/Cart.tsx", "test/session.test.ts", "test/billing.test.ts",
          "package.json", "tsconfig.json", "README.md"],
    API: ["src/main.rs", "src/lib.rs", "src/routes.rs", "src/db.rs", "src/auth.rs", "src/error.rs", "tests/routes.rs",
          "Cargo.toml", "README.md"],
    SDK: ["src/globex_sdk/__init__.py", "src/globex_sdk/client.py", "src/globex_sdk/models.py", "src/globex_sdk/retry.py",
          "tests/test_client.py", "tests/test_retry.py", "pyproject.toml", "README.md"],
}
REMOTE = {WEB: "acme/acme-web.git", API: "acme/acme-api.git", SDK: "globex/globex-sdk.git"}
MESSAGES = ["handle empty bodies", "retry on 502", "fix the cart total rounding", "add session expiry test",
            "tidy error types", "bump the client timeout", "log the request id", "split the billing routes"]


def build(mac, start, hours, flush=None):
    """A work day from `start`, `hours` long. `flush`, when given, is called
    after each track (gen.py writes the events out and clears mac.log)."""
    t0 = es.ns(start)
    seed = mac.rng.getrandbits(64)
    day = Day(random.Random(f"{seed}:day"), t0, t0 + int(hours * 3600) * S)
    rng = mac.rng
    mac.thin = lambda kind, proc: thinned(mac, kind, proc)
    for track in (system, editor, person, claude_web, claude_api, codex):
        mac.rng = random.Random(f"{seed}:{track.__name__}")
        mac.at(t0)
        track(mac, day)
        if flush:
            flush()
    mac.rng, mac.thin = rng, None


def thinned(mac, kind, proc):
    """Whether a fork or an exit goes unrecorded: all but one in THIN, except a
    fork by a process with no exec of its own (an app, a daemon, launchd),
    which is how the plugin's tree learns what that process is."""
    if kind == "fork" and (proc is mac.launchd or any(proc is p for p in mac._running.values())):
        return False
    return zlib.crc32(es.iso(mac.t).encode()) % THIN != 0


class Day:
    """When the person is at the desk: from `arrive` to `leave`, less lunch and meetings."""

    def __init__(self, r, t0, end):
        self.t0, self.end = t0, end
        span = (end - t0) // S
        self.arrive = t0 + min(span // 4, r.randint(3, 25) * 60) * S
        self.leave = end - min(span // 8, r.randint(5, 35) * 60) * S
        self.away = []
        if span >= 5 * 3600:
            lunch = t0 + r.randint(195, 240) * 60 * S
            self.away.append((lunch, lunch + r.randint(35, 60) * 60 * S))
        for _ in range(r.randint(0, 2) if span >= 3 * 3600 else 0):
            m = t0 + r.randint(60, span // 60 - 60) * 60 * S
            self.away.append((m, m + r.choice((15, 25, 30, 45)) * 60 * S))
        self.away.sort()

    def back(self, t):
        """`t`, or when the person is next at the desk; None once they have left."""
        t = max(t, self.arrive)
        for a, b in self.away:
            if a <= t < b:
                t = b
        return t if t < self.leave else None


def every(mac, day, gap, act, start=None, stop=None):
    """act(), then `gap()` seconds, again and again while the person is at the
    desk, from `start` (default: now, or their arrival) until `stop` (their leaving)."""
    stop = min(stop or day.leave, day.leave)
    mac.at(max(mac.t, start or day.arrive))
    while True:
        t = day.back(mac.t)
        if t is None or t >= stop:
            return
        if t > mac.t:
            mac.at(t)
        act()
        mac.wait(gap(), jitter=0)


# ---- small things

def maybe(mac, p):
    return mac.rng.random() < p


def pick(mac, weighted):
    """A choice from [(weight, x, ...), ...]: the tuple, less its weight when it had two parts."""
    x = mac.rng.choices(weighted, [w[0] for w in weighted])[0]
    return x[1] if len(x) == 2 else x


def skim(mac, proc, *paths):
    """Reads the plugin drops (sources, manifests, caches), sampled (SKIM)."""
    for p in paths:
        if mac.rng.random() < SKIM:
            mac.read_file(proc, p)


def scribble(mac, proc, path, *, new=None):
    """A write the plugin drops (a build output, a cache, a source file saved),
    sampled: DROPPED when it creates the file, SKIM when it rewrites one."""
    path = mac._p(path)
    new = path not in mac.files if new is None else new
    if mac.rng.random() < (DROPPED if new else SKIM):
        mac.write_file(proc, path, new=new)


def h32(mac):
    return f"{mac.rng.getrandbits(32):08x}"


def files(mac, repo, n):
    return [f"{repo}/{f}" for f in mac.rng.sample(SRC[repo], min(n, len(SRC[repo])))]


def job(mac, label, prog, args, *, user=True):
    """launchd starts a job: it forks, the child execs xpcproxy, which execs the
    program; no tty, a session of its own, as dev for a user agent."""
    c = mac.fork(mac.launchd)
    if user:
        c.ruid = c.euid = es.UID
        c.rgid = c.egid = es.STAFF
        c.asid, c.auid = es.ASID, es.UID
    c.sid = c.pgid = c.pid
    c.tty, c.cwd, c.responsible = None, "/", c
    c.env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": HOME if user else "/var/root", "USER": es.USER if user else "root",
             "LOGNAME": es.USER if user else "root", "SHELL": "/bin/zsh", "TMPDIR": es._GUI_ENV["TMPDIR"],
             "XPC_SERVICE_NAME": label, "XPC_FLAGS": "0x0"}
    mac.exec(c, "xpcproxy", ["xpcproxy", label])
    mac.exec(c, prog, args)
    return c


# ---- what the person, the agents and the editor run (each runs under `sh`, in `repo`, and exits)

def git(mac, sh, repo, args, *, net=False, writes=False):
    g = mac.spawn(sh, "git", ["git"] + args, cwd=repo)
    skim(mac, g, f"{HOME}/.gitconfig", f"{repo}/.git/config", f"{repo}/.git/HEAD", f"{repo}/.git/index")
    if net:
        pack = "git-receive-pack" if args[0] == "push" else "git-upload-pack"
        ssh(mac, g, ["ssh", "-o", "SendEnv=GIT_PROTOCOL", "git@git.example.com", f"{pack} '{REMOTE[repo]}'"], fds="pipe",
            wait=mac.rng.uniform(0.6, 3))
    if writes:
        for _ in range(mac.rng.randint(1, 4)):
            h = f"{mac.rng.getrandbits(160):040x}"
            scribble(mac, g, f"{repo}/.git/objects/{h[:2]}/{h[2:]}", new=True)
        if maybe(mac, DROPPED):
            mac.write_file(g, f"{repo}/.git/index.lock", new=True)
            mac.rename(g, f"{repo}/.git/index.lock", f"{repo}/.git/index")
    mac.wait(mac.rng.uniform(0.01, 0.15))
    mac.exit(g)


def ssh(mac, parent, args, *, wait=1.0, **kw):
    """ssh reads its config, known hosts and key: the plugin keeps the reads (credentials, by ssh)."""
    s = mac.spawn(parent, "ssh", args, **kw)
    for f in ("~/.ssh/config", "~/.ssh/known_hosts", "~/.ssh/id_ed25519"):
        mac.read_file(s, f)
    mac.wait(wait)
    mac.exit(s)


def one(prog, *args, reads=0):
    """A command that is one program: ls, rg, sed."""
    def run(mac, sh, repo):
        p = mac.spawn(sh, prog, [prog] + list(args), cwd=repo)
        skim(mac, p, *files(mac, repo, reads))
        mac.wait(mac.rng.uniform(0.005, 0.2))
        mac.exit(p)
    return run


def npm_env(repo, script):
    return {"npm_lifecycle_event": script, "npm_command": "test" if script == "test" else "run-script",
            "npm_package_json": f"{repo}/package.json", "npm_package_name": repo.rsplit("/", 1)[1],
            "npm_config_user_agent": "npm/11.6.0 node/v26.0.0 darwin arm64 workspaces/false", "INIT_CWD": repo,
            "PATH": f"{repo}/node_modules/.bin:" + es._LOGIN_PATH}


def npm(script):
    """npm test / npm run build / lint: npm reads ~/.npmrc (kept: a credential,
    by npm), runs the script as `sh -c` (which execs it), and the script's node
    runs its own children: the bundler's native core, test workers."""
    def run(mac, sh, repo):
        n = mac.spawn(sh, "npm", ["npm", "test"] if script == "test" else ["npm", "run", script], cwd=repo)
        mac.read_file(n, "~/.npmrc")
        skim(mac, n, f"{repo}/package.json", f"{repo}/package-lock.json")
        line = {"test": "globex-test run", "build": "acme-bundle build", "lint": "acme-lint src"}[script]
        s = mac.spawn(n, "sh", ["sh", "-c", line], cwd=repo, fds="pipe", env_add=npm_env(repo, script))
        if script == "test":
            mac.exec(s, TESTER, ["globex-test", "run"])
            for _ in range(mac.rng.randint(2, 4)):
                w = mac.spawn(s, "node", [es.NODE, f"{NM}/globex-test/dist/worker.js"], fds="pipe")
                skim(mac, w, *files(mac, repo, 4))
                mac.wait(mac.rng.uniform(0.5, 4))
                mac.exit(w)
        elif script == "build":
            mac.exec(s, BUNDLE, ["acme-bundle", "build"])
            core = mac.spawn(s, BUNDLE_CORE, [BUNDLE_CORE.path, "--service=1.4.2", "--ping"], fds="pipe")
            skim(mac, core, *files(mac, repo, 6))
            mac.wait(mac.rng.uniform(1, 6))
            for f in ("dist/index.js", "dist/index.js.map", "dist/app.css"):
                scribble(mac, core, f"{repo}/{f}")
            mac.exit(core)
        else:
            mac.exec(s, LINT, ["acme-lint", "src"])
            skim(mac, s, *files(mac, repo, 6))
            mac.wait(mac.rng.uniform(1, 5))
        mac.exit(s)
        mac.exit(n)
    return run


def npm_install(mac, sh, repo):
    """npm install with two ordinary install scripts: acme-native builds its
    addon (node-gyp: python's gyp, make, c++, ld) inside node_modules, and
    acme-icons' postinstall writes its own dist/."""
    n = mac.spawn(sh, "npm", ["npm", "install"], cwd=repo)
    mac.read_file(n, "~/.npmrc")
    mac.wait(mac.rng.uniform(2, 8))
    for pkg, f in (("acme-native", "binding.gyp"), ("acme-native", "src/addon.cc"), ("acme-icons", "scripts/build.js")):
        scribble(mac, n, f"{NM}/{pkg}/{f}", new=True)
    native = f"{NM}/acme-native"
    s = es.npm_lifecycle(mac, n, native, "node-gyp rebuild", event="install", version="3.2.0")
    mac.exec(s, "node-gyp", ["node-gyp", "rebuild"])
    gyp = mac.spawn(s, "python3", ["python3", "/opt/homebrew/lib/node_modules/npm/node_modules/node-gyp/gyp/gyp_main.py",
                                   "binding.gyp", "-f", "make"], fds="pipe")
    mac.wait(mac.rng.uniform(0.3, 1))
    scribble(mac, gyp, f"{native}/build/Makefile", new=True)
    mac.exit(gyp)
    mk = mac.spawn(s, "make", ["make", "BUILDTYPE=Release", "-C", "build"], fds="pipe")
    for src in ("addon", "codec"):
        cc = mac.spawn(mk, "c++", ["c++", "-o", f"Release/obj.target/acme_native/src/{src}.o", f"../src/{src}.cc", "-O3"], fds="pipe")
        mac.wait(mac.rng.uniform(0.5, 3))
        scribble(mac, cc, f"{native}/build/Release/obj.target/acme_native/src/{src}.o", new=True)
        mac.exit(cc)
    link = mac.spawn(mk, "c++", ["c++", "-bundle", "-o", "Release/acme_native.node"], fds="pipe")
    ld = mac.spawn(link, "ld", ["/Library/Developer/CommandLineTools/usr/bin/ld", "-bundle", "-o", "Release/acme_native.node"], fds="pipe")
    scribble(mac, ld, f"{native}/build/Release/acme_native.node", new=True)
    mac.exit(ld)
    mac.exit(link)
    mac.exit(mk)
    mac.exit(s)
    s = es.npm_lifecycle(mac, n, f"{NM}/acme-icons", "node scripts/build.js", version="5.0.1")
    mac.exec(s, "node", ["node", "scripts/build.js"])
    for f in ("dist/index.js", "dist/icons.json"):
        scribble(mac, s, f"{NM}/acme-icons/{f}", new=True)
    mac.exit(s)
    scribble(mac, n, f"{NM}/.package-lock.json")
    scribble(mac, n, f"{repo}/package-lock.json")
    mac.exit(n)


def cargo(sub):
    """cargo build/test/check/clippy/fmt through rustup: rustc (or clippy) for
    what changed, now and then a build script and a dependency rebuild; test
    runs the test binaries and rustdoc."""
    def run(mac, sh, repo):
        c = mac.spawn(sh, "cargo", ["cargo", sub] + (["--message-format=json"] if sh.prog is RUST_LSP else []), cwd=repo)
        skim(mac, c, f"{repo}/Cargo.toml", f"{repo}/Cargo.lock", f"{HOME}/.cargo/config.toml")
        if sub == "fmt":
            mac.run(c, RUSTFMT, [RUSTFMT.path, "--edition", "2021", "src/main.rs"], wait=0.3, fds="pipe")
            mac.exit(c)
            return
        crates = ["acme_api"] if maybe(mac, 0.6) else ["acme_core", "acme_api"]
        if maybe(mac, 0.06):  # a dependency changed
            crates = [f"dep{i}" for i in range(mac.rng.randint(5, 14))] + crates
            mac.run(c, BUILD_SCRIPT, [BUILD_SCRIPT.path], wait=0.4, fds="pipe")
        compiler = CLIPPY if sub == "clippy" else es.PROGRAMS["rustc"]
        for crate in crates:
            r = mac.spawn(c, compiler, [compiler.path, "--crate-name", crate, "--edition=2021", "src/lib.rs",
                                        "--error-format=json", "--crate-type", "lib"], fds="pipe")
            skim(mac, r, *files(mac, repo, 3))
            mac.wait(mac.rng.uniform(0.3, 6))
            inc = f"{repo}/target/debug/incremental/{crate}-{h32(mac)}"
            for _ in range(mac.rng.randint(4, 16)):
                scribble(mac, r, f"{inc}/s-{h32(mac)}-working/{h32(mac)}.o", new=True)
            scribble(mac, r, f"{repo}/target/debug/deps/lib{crate}-{h32(mac)}.rmeta", new=True)
            if maybe(mac, DROPPED * 3):
                mac.rename(r, f"{inc}/s-{h32(mac)}-working", f"{inc}/s-{h32(mac)}-{h32(mac)}")
            for _ in range(mac.rng.randint(2, 10)):
                if maybe(mac, DROPPED):
                    mac.unlink(r, f"{inc}/s-{h32(mac)}-{h32(mac)}/{h32(mac)}.o")
            mac.exit(r)
        if sub == "test":
            for t in TESTBINS:
                mac.run(c, t, [t.path], wait=mac.rng.uniform(0.2, 3), fds="pipe")
            mac.run(c, RUSTDOC, [RUSTDOC.path, "--edition=2021", "--crate-name", "acme_api", "--test", "src/lib.rs"],
                    wait=mac.rng.uniform(1, 4), fds="pipe")
        mac.exit(c)
    return run


def uv(*args):
    """uv run (pytest) or uv sync in globex-sdk: the venv's python, which execs Python.app."""
    def run(mac, sh, repo):
        u = mac.spawn(sh, "uv", ["uv"] + list(args), cwd=repo)
        skim(mac, u, f"{repo}/pyproject.toml", f"{repo}/uv.lock")
        if args[0] == "run":
            p = mac.spawn(u, VENV_PY, [f"{repo}/.venv/bin/python3", "-m"] + list(args[1:]), fds="pipe")
            skim(mac, p, *files(mac, repo, 5))
            mac.wait(mac.rng.uniform(1, 6))
            scribble(mac, p, f"{repo}/.pytest_cache/v/cache/lastfailed")
            mac.exit(p)
        else:
            mac.wait(mac.rng.uniform(0.5, 3))
        mac.exit(u)
    return run


def gh(*args):
    """gh reads its config and hosts.yml (kept: credentials, by gh), asks git
    which repository it is in, and talks to the API."""
    def run(mac, sh, repo):
        g = mac.spawn(sh, "gh", ["gh"] + list(args), cwd=repo)
        mac.read_file(g, "~/.config/gh/config.yml")
        mac.read_file(g, "~/.config/gh/hosts.yml")
        git(mac, g, repo, ["remote", "-v"])
        if args[:2] == ("pr", "create"):
            git(mac, g, repo, ["rev-parse", "--abbrev-ref", "HEAD"])
        mac.wait(mac.rng.uniform(0.4, 2.5))
        mac.exit(g)
    return run


def aws(*args):
    """aws (Homebrew's: python, which execs Python.app) reads its config, the
    shared credentials file and the SSO token cache (kept: credentials, by aws)."""
    def run(mac, sh, repo):
        a = mac.spawn(sh, "aws", ["aws"] + list(args), cwd=repo, fds="pipe" if sh.prog.name == "kubectl" else None)
        for f in ("~/.aws/config", "~/.aws/credentials", SSO_CACHE):
            mac.read_file(a, f)
        mac.wait(mac.rng.uniform(0.5, 2.5))
        if maybe(mac, 0.3):
            mac.write_file(a, "~/.aws/cli/cache/0c4f2a8e6b1d3f5a7c9e0b2d4f6a8c1e3b5d7f9a.json")
        mac.exit(a)
    return run


def aws_sso_login(mac, sh, repo):
    """aws sso login: the device page opened in the browser, the person
    approves it, and aws writes the SSO token cache (kept: a credential, by aws)."""
    a = mac.spawn(sh, "aws", ["aws", "sso", "login", "--profile", "acme-dev"], cwd=repo)
    mac.read_file(a, "~/.aws/config")
    mac.run(a, "open", ["open", "https://device.sso.example.com/?user_code=QWRT-ZXCV"], wait=0.2)
    mac.wait(mac.rng.uniform(12, 40))
    mac.write_file(a, SSO_CACHE)
    mac.write_file(a, "~/.aws/sso/cache/botocore-client-id-us-west-2.json")
    mac.exit(a)


def kubectl(*args):
    """kubectl reads ~/.kube/config (kept, by kubectl) and runs its exec
    credential plugin, `aws eks get-token`."""
    def run(mac, sh, repo):
        k = mac.spawn(sh, "kubectl", ["kubectl"] + list(args), cwd=repo)
        mac.read_file(k, "~/.kube/config")
        aws("eks", "get-token", "--cluster-name", "acme-dev", "--output", "json")(mac, k, repo)
        scribble(mac, k, f"{HOME}/.kube/cache/discovery/acme-dev.example.com_443/servergroups.json")
        mac.wait(mac.rng.uniform(0.3, 2))
        mac.exit(k)
    return run


def docker(*args):
    """docker reads ~/.docker/config.json (kept, by docker); compose is a CLI plugin it execs."""
    def run(mac, sh, repo):
        d = mac.spawn(sh, "docker", ["docker"] + list(args), cwd=repo)
        mac.read_file(d, "~/.docker/config.json")
        if args[0] == "compose":
            c = mac.spawn(d, COMPOSE, [COMPOSE.path, "compose"] + list(args[1:]), fds="pipe")
            mac.read_file(c, "~/.docker/config.json")
            mac.wait(mac.rng.uniform(0.5, 4))
            mac.exit(c)
        mac.wait(mac.rng.uniform(0.1, 0.8))
        mac.exit(d)
    return run


def vim_src(mac, sh, repo):
    es.vim_edit(mac, sh, mac.rng.choice(files(mac, repo, 3)), saves=mac.rng.randint(1, 2))


def commit(message):
    def run(mac, sh, repo):
        git(mac, sh, repo, ["add", "-A"], writes=True)
        git(mac, sh, repo, ["commit", "-m", message], writes=True)
    return run


# (weight, command as typed, what runs) for the agents' tool shells, per repository
AGENT = {
    WEB: [(14, "git status", lambda m, s, r: git(m, s, r, ["status"])),
          (10, "git diff", lambda m, s, r: git(m, s, r, ["diff"])),
          (4, "git diff --stat", lambda m, s, r: git(m, s, r, ["diff", "--stat"])),
          (4, "git log --oneline -10", lambda m, s, r: git(m, s, r, ["log", "--oneline", "-10"])),
          (6, "ls src src/routes", one("ls", "src", "src/routes")),
          (10, "rg -n useSession src", one("rg", "-n", "useSession", "src", reads=6)),
          (8, "sed -n 1,120p src/app.ts", one("sed", "-n", "1,120p", "src/app.ts", reads=1)),
          (12, "npm test", npm("test")),
          (6, "npm run build", npm("build")),
          (6, "npm run lint", npm("lint")),
          (3, "git add -A && git commit -m \"fix the cart total rounding\"", commit("fix the cart total rounding")),
          (0.4, "git push", lambda m, s, r: git(m, s, r, ["push"], net=True)),
          (0.3, "npm install", npm_install)],
    API: [(14, "git status", lambda m, s, r: git(m, s, r, ["status"])),
          (10, "git diff", lambda m, s, r: git(m, s, r, ["diff"])),
          (10, "cargo check", cargo("check")),
          (12, "cargo test", cargo("test")),
          (6, "cargo build", cargo("build")),
          (5, "cargo clippy", cargo("clippy")),
          (3, "cargo fmt", cargo("fmt")),
          (10, "rg -n RetryPolicy src tests", one("rg", "-n", "RetryPolicy", "src", "tests", reads=5)),
          (8, "sed -n 40,160p src/routes.rs", one("sed", "-n", "40,160p", "src/routes.rs", reads=1)),
          (5, "ls src tests", one("ls", "src", "tests")),
          (3, "git add -A && git commit -m \"tidy error types\"", commit("tidy error types")),
          (0.4, "git push", lambda m, s, r: git(m, s, r, ["push"], net=True))],
    SDK: [(12, "git status", lambda m, s, r: git(m, s, r, ["status"])),
          (8, "git diff", lambda m, s, r: git(m, s, r, ["diff"])),
          (12, "uv run pytest -q", uv("run", "pytest", "-q")),
          (6, "uv run pytest -q tests/test_retry.py", uv("run", "pytest", "-q", "tests/test_retry.py")),
          (10, "rg -n backoff src tests", one("rg", "-n", "backoff", "src", "tests", reads=5)),
          (8, "sed -n 1,120p src/globex_sdk/client.py", one("sed", "-n", "1,120p", "src/globex_sdk/client.py", reads=1)),
          (5, "ls src/globex_sdk tests", one("ls", "src/globex_sdk", "tests")),
          (2, "git add -A && git commit -m \"bump the client timeout\"", commit("bump the client timeout"))],
}

# (weight, repository, what runs) for the person's own commands, at a tab in that repository
PERSON = [
    (12, API, lambda m, s, r: git(m, s, r, m.rng.choice([["status"], ["diff"], ["log", "--oneline", "-10"], ["branch"]]))),
    (12, WEB, lambda m, s, r: git(m, s, r, m.rng.choice([["status"], ["diff"], ["log", "--oneline", "-10"], ["stash", "list"]]))),
    (4, API, lambda m, s, r: git(m, s, r, ["commit", "-am", m.rng.choice(MESSAGES)], writes=True)),
    (4, WEB, lambda m, s, r: git(m, s, r, ["commit", "-am", m.rng.choice(MESSAGES)], writes=True)),
    (2, API, lambda m, s, r: git(m, s, r, ["push"], net=True)),
    (2, WEB, lambda m, s, r: git(m, s, r, ["push"], net=True)),
    (2, WEB, lambda m, s, r: git(m, s, r, ["pull", "--rebase"], net=True, writes=True)),
    (2, API, lambda m, s, r: git(m, s, r, ["switch", "-c", f"dev/{m.rng.choice(['retry', 'cart', 'expiry', 'errors'])}-{m.rng.randint(2, 99)}"])),
    (5, API, one("ls", "-la")),
    (4, WEB, one("rg", "-n", "TODO", "src", reads=6)),
    (3, API, one("cat", "Cargo.toml", reads=1)),
    (5, API, cargo("build")),
    (4, API, cargo("test")),
    (2, API, cargo("clippy")),
    (4, WEB, npm("test")),
    (2, WEB, npm("build")),
    (2, WEB, npm("lint")),
    (2, WEB, gh("pr", "status")),
    (2, API, gh("pr", "checks")),
    (1, WEB, gh("pr", "view", "--web")),
    (0.6, API, gh("pr", "create", "--fill")),
    (2, API, aws("sts", "get-caller-identity", "--profile", "acme-dev")),
    (1.5, API, aws("s3", "ls", "s3://acme-dev-artifacts/", "--profile", "acme-dev")),
    (1.5, API, aws("logs", "tail", "/acme/api", "--since", "10m", "--profile", "acme-dev")),
    (2, API, kubectl("get", "pods", "-n", "acme-dev")),
    (1.5, API, kubectl("logs", "deploy/acme-api", "-n", "acme-dev", "--tail=100")),
    (1.5, API, docker("ps")),
    (1, API, docker("compose", "up", "-d")),
    (0.5, API, lambda m, s, r: ssh(m, s, ["ssh", "deploy@build.example.net"], wait=m.rng.uniform(30, 240))),
    (2, API, vim_src),
    (2, SDK, uv("run", "pytest", "-q")),
    (0.15, WEB, npm_install),
    (0.3, SDK, uv("sync")),
]


# ---- the tracks

def system(mac, day):
    """launchd's jobs and the preferences daemon, while the Mac is in use."""
    prefs = mac.daemon("cfprefsd", user=True)
    mac.at(day.arrive + mac.rng.randint(120, 1200) * S)
    job(mac, "com.openssh.ssh-agent", "ssh-agent", ["/usr/bin/ssh-agent", "-l"])  # on first use; it runs on

    def spotlight():
        w = job(mac, f"com.apple.mdworker.shared.{mac.rng.getrandbits(32):08X}-0000-0000-0000-000000000000",
                "mdworker_shared", [MDWORKER, "-s", "mdworker", "-c", "MDSImporterWorker", "-m", "com.apple.mdworker.shared"])
        skim(mac, w, *files(mac, mac.rng.choice((WEB, API, SDK)), 8))
        mac.wait(mac.rng.uniform(2, 40))
        mac.exit(w)
        for _ in range(mac.rng.randint(0, 4)):  # apps' preferences saved as they change: a tmp file renamed over
            if maybe(mac, DROPPED * 8):
                name = mac.rng.choice(["com.apple.Terminal", "com.apple.finder", "com.apple.dock", "com.todesktop.230313mzl4w4u92",
                                       "com.mitchellh.ghostty", "com.apple.spotlight", "com.googlecode.iterm2"])
                plist = f"~/Library/Preferences/{name}.plist"
                mac.atomic_write(prefs, plist, tmp=f"{plist}.{h32(mac)[:6]}")

    every(mac, day, lambda: mac.rng.uniform(25, 90), spotlight)


def editor(mac, day):
    """Cursor, open since login: the extension host, its git and a language server."""
    cursor = mac.app("Cursor")
    helper = es.PROGRAMS["Cursor Helper (Plugin)"]
    mac.at(day.arrive + mac.rng.randint(60, 600) * S)
    host = mac.spawn(cursor, helper, [helper.path, "--type=utility", "--utility-sub-type=node.mojom.NodeService", "--lang=en-US",
                                      "--service-sandbox-type=none"], env_add={"VSCODE_CWD": "/", "VSCODE_PID": str(cursor.pid)})
    lsp = mac.spawn(host, RUST_LSP, [RUST_LSP.path], cwd=API, fds="pipe")
    edits = {f"{WEB}/CLAUDE.md": maybe(mac, 0.3) and mac.rng.randint(day.arrive, day.leave),
             f"{API}/AGENTS.md": maybe(mac, 0.2) and mac.rng.randint(day.arrive, day.leave)}

    def refresh():
        repo = mac.rng.choice((WEB, API))
        for args in (["status", "-z", "-uall"], ["rev-parse", "--show-toplevel"])[: mac.rng.choice((1, 1, 2))]:
            git(mac, host, repo, args)
        if maybe(mac, 0.15):
            git(mac, host, repo, ["for-each-ref", "--sort", "-committerdate", "--format", "%(refname)%00%(objectname)"])
        if maybe(mac, 0.12):  # the person saves a file
            scribble(mac, cursor, mac.rng.choice(files(mac, repo, 3)))
            if repo == API and maybe(mac, 0.7):
                cargo("check")(mac, lsp, API)
        for f, when in edits.items():
            if when and mac.t >= when:  # the person edits the agents' instructions in Cursor
                mac.write_file(cursor, f)
                edits[f] = False

    every(mac, day, lambda: mac.rng.uniform(15, 45), refresh)
    mac.at(max(mac.t, day.leave))
    mac.exit(lsp)
    mac.exit(host)


def person(mac, day):
    """Terminal tabs, from arrival until leaving."""
    mac.at(day.arrive)
    tabs = {API: mac.terminal("Terminal", cwd=API), WEB: mac.terminal("Terminal", cwd=WEB)}
    tabs[SDK] = tabs[API]  # cd ../globex-sdk in the api tab
    mac.wait(mac.rng.uniform(5, 30))
    git(mac, tabs[API], API, ["pull", "--rebase"], net=True, writes=True)
    git(mac, tabs[WEB], WEB, ["pull", "--rebase"], net=True, writes=True)
    if maybe(mac, 0.7):
        aws_sso_login(mac, tabs[API], API)
    dev = None
    if maybe(mac, 0.75):  # the dev server, all day, in a tab of its own; it loads the project's .env files
        devtab = mac.terminal("Terminal", cwd=WEB)
        n = mac.spawn(devtab, "npm", ["npm", "run", "dev"], cwd=WEB)
        mac.read_file(n, "~/.npmrc")
        s = mac.spawn(n, "sh", ["sh", "-c", "acme-bundle dev"], cwd=WEB, fds="pipe", env_add=npm_env(WEB, "dev"))
        mac.exec(s, BUNDLE, ["acme-bundle", "dev"])
        for f in (".env", ".env.local"):
            mac.read_file(s, f"{WEB}/{f}")
        core = mac.spawn(s, BUNDLE_CORE, [BUNDLE_CORE.path, "--service=1.4.2", "--ping"], fds="pipe")
        dev = (devtab, n, s, core)
    zshrc = maybe(mac, 0.1) and mac.rng.randint(day.arrive, day.leave)

    def command():
        nonlocal zshrc
        repo, run = pick(mac, [(w, (r, f)) for w, r, f in PERSON])
        run(mac, tabs[repo], repo)
        if zshrc and mac.t >= zshrc:  # an alias, in vim
            es.vim_edit(mac, tabs[API], "~/.zshrc")
            zshrc = False

    every(mac, day, lambda: mac.rng.uniform(25, 200), command)
    mac.at(max(mac.t, day.leave))
    if dev:
        devtab, n, s, core = dev
        mac.exit(core)
        mac.exit(s)
        mac.exit(n)
        mac.logout(devtab)
    mac.logout(tabs[WEB])
    mac.wait(2)
    mac.logout(tabs[API])


def claude_web(mac, day):
    claude_sessions(mac, day, WEB, "Terminal", mcp=True, refreshes=1 + maybe(mac, 0.35))


def claude_api(mac, day):
    claude_sessions(mac, day, API, "Ghostty")


def claude_sessions(mac, day, repo, app, *, mcp=False, refreshes=0):
    """Claude Code sessions in `repo`, one after another, while the person is at the desk."""
    mac.at(day.arrive + mac.rng.randint(4, 40) * 60 * S)
    first = True
    while True:
        t = day.back(mac.t)
        if t is None or t >= day.leave - 10 * 60 * S:
            return
        mac.at(t)
        sh = mac.terminal(app, cwd=repo)
        mac.wait(mac.rng.uniform(2, 10))
        c = es.claude(mac, sh, repo)
        if first:  # sessions older than cleanupPeriodDays (30) are deleted at startup: those that aged out overnight
            for _ in range(mac.rng.randint(0, 3)):
                old = mac.exists(f"{es.project_dir(repo)}/{mac.uuid().lower()}.jsonl", days=mac.rng.randint(31, 33))
                mac.unlink(c, old)
            first = False
        for args in (["rev-parse", "--abbrev-ref", "HEAD"], ["status", "--short"], ["log", "--oneline", "-n", "5"]):
            mac.run(c, "git", ["git"] + args, cwd=repo, setsid=True, wait=0.03)
        server = mcp and mac.spawn(c, "node", ["node", DOCS_MCP], cwd=repo, fds="pipe", setsid=True)
        if refreshes:
            refresh_marketplace(mac, c)
            refreshes -= 1
        ends = mac.t + mac.rng.randint(40, 130) * 60 * S
        state = {"memory": maybe(mac, 0.3), "allow": maybe(mac, 0.2)}
        every(mac, day, lambda: mac.rng.uniform(8, 80), lambda: claude_turn(mac, c, repo, state), stop=ends)
        mac.write_file(c, c.session, append=True)
        if server:
            mac.exit(server)
        mac.exit(c)
        mac.wait(mac.rng.uniform(1, 30))
        mac.logout(sh)
        mac.wait(mac.rng.uniform(2, 20) * 60)


def claude_turn(mac, c, repo, state):
    """A prompt, the tool calls it takes, the reply. The transcript is appended
    at the prompt, now and then as the tools run, and at the reply: not on
    every message (the capture's closes on transcripts are a few a turn)."""
    es.claude_prompt(mac, c, reply=False)
    mac.wait(mac.rng.uniform(1.5, 6))
    for _ in range(pick(mac, [(2, 1), (3, 2), (3, 3), (4, 5), (3, 8), (2, 12), (1, 18)])):
        kind = pick(mac, [(50, "bash"), (25, "read"), (15, "edit"), (10, "search")])
        if kind == "bash":
            text, run = pick(mac, [(w, (t, f)) for w, t, f in AGENT[repo]])
            tool = es.agent_shell(mac, c, text)
            run(mac, tool, repo)
            cwdfile = tool.args[2].rsplit(">| ", 1)[1]
            if maybe(mac, DROPPED):  # `pwd -P >| <file>`, which Claude Code reads and removes
                mac.write_file(tool, cwdfile, new=True)
            mac.exit(tool)
            if cwdfile in mac.files:
                mac.read_file(c, cwdfile)
                mac.unlink(c, cwdfile)
        elif kind == "edit":
            sid = c.session.rsplit("/", 1)[1][:-len(".jsonl")]
            scribble(mac, c, f"{HOME}/.claude/file-history/{sid}/{h32(mac)}{h32(mac)}@v{mac.rng.randint(1, 4)}", new=True)
            scribble(mac, c, mac.rng.choice(files(mac, repo, 3)))
        else:
            skim(mac, c, *files(mac, repo, 2 if kind == "read" else 8))
        if maybe(mac, 0.25):
            mac.write_file(c, c.session, append=True)
        mac.wait(mac.rng.uniform(1.5, 8))  # the model's next step
    mac.write_file(c, c.session, append=True)  # the reply
    if maybe(mac, 0.7):
        mac.atomic_write(c, "~/.claude.json")
    if state["memory"] and maybe(mac, 0.1):  # it saves what it learned to its memory
        mac.write_file(c, f"{es.project_dir(repo)}/memory/MEMORY.md")
        state["memory"] = False
    if state["allow"] and maybe(mac, 0.1):  # the person chose "don't ask again" for a command
        mac.write_file(c, f"{repo}/.claude/settings.local.json")
        state["allow"] = False


def refresh_marketplace(mac, c):
    """A plugin marketplace refreshed by Claude Code: written into .staging, the
    live copy renamed to .backup, staging renamed live, the backup unlinked."""
    dirs, fs = MARKET_TREE
    staging, backup = MARKET + ".staging", MARKET + ".backup"
    mac.create(c, staging)
    for d in dirs:
        mac.create(c, f"{staging}/{d}")
    for f in fs:
        mac.write_file(c, f"{staging}/{f}", new=True)
    mac.rename(c, MARKET, backup)
    mac.rename(c, staging, MARKET)
    for f in fs:
        mac.unlink(c, f"{backup}/{f}")
    for d in reversed(dirs):
        mac.unlink(c, f"{backup}/{d}")
    mac.unlink(c, backup)
    mac.atomic_write(c, "~/.claude/plugins/known_marketplaces.json")


def _market_tree():
    words = ["review", "lint", "deploy", "docs", "tests", "release", "triage", "schema", "migrate", "perf", "a11y", "i18n",
             "changelog", "commits", "prs", "oncall", "infra", "deps", "style", "bench"]
    dirs, fs = [".claude-plugin", "plugins"], [".claude-plugin/marketplace.json", "README.md", "LICENSE"]
    for i, name in enumerate(f"{co}-{w}" for co in ("acme", "globex") for w in words):
        b = f"plugins/{name}"
        dirs += [b, f"{b}/.claude-plugin", f"{b}/commands"]
        fs += [f"{b}/.claude-plugin/plugin.json", f"{b}/README.md", f"{b}/LICENSE"]
        fs += [f"{b}/commands/{name}-{k}.md" for k in range(2 + i % 5)]
        if i % 2 == 0:
            dirs.append(f"{b}/agents")
            fs += [f"{b}/agents/{name}-agent-{k}.md" for k in range(1 + i % 3)]
        if i % 3 != 2:
            dirs += [f"{b}/skills", f"{b}/skills/{name}"]
            fs += [f"{b}/skills/{name}/SKILL.md", f"{b}/skills/{name}/reference.md"]
            if i % 2:
                dirs.append(f"{b}/skills/{name}/examples")
                fs += [f"{b}/skills/{name}/examples/example-{k}.md" for k in range(3)]
        if i % 4 == 0:
            dirs += [f"{b}/hooks", f"{b}/scripts"]
            fs += [f"{b}/hooks/hooks.json", f"{b}/scripts/format.sh", f"{b}/scripts/check.sh"]
    return dirs, fs


MARKET_TREE = _market_tree()


def codex(mac, day):
    """Codex in globex-sdk, from late morning."""
    start = day.t0 + mac.rng.randint(120, 240) * 60 * S
    if start >= day.leave - 30 * 60 * S:
        return
    mac.at(start)
    while True:
        t = day.back(mac.t)
        if t is None or t >= day.leave - 15 * 60 * S:
            return
        mac.at(t)
        sh = mac.terminal("iTerm", cwd=SDK)
        mac.wait(mac.rng.uniform(2, 8))
        front = mac.spawn(sh, "codex", ["codex"], cwd=SDK)  # env, then node
        cx = mac.spawn(front, "codex-native", [es.CODEX_BIN], cwd=SDK)
        mac.read_file(cx, "~/.codex/config.toml")
        mac.read_file(cx, "~/.codex/auth.json")
        log = "~/.codex/log/codex-tui.log"
        mac.open(cx, log, es.FWRITE | es.O_APPEND | es.O_CREAT | es.O_CLOEXEC)
        d = es.iso(mac.t)
        rollout = f"~/.codex/sessions/{d[:4]}/{d[5:7]}/{d[8:10]}/rollout-{d[:19].replace(':', '-')}-{mac.uuid().lower()}.jsonl"
        mac.create(cx, rollout)
        mac.open(cx, rollout, es.FWRITE | es.O_APPEND | es.O_CREAT | es.O_CLOEXEC)

        def turn():
            mac.write_file(cx, "~/.codex/history.jsonl", append=True)
            for _ in range(pick(mac, [(2, 1), (3, 2), (3, 3), (3, 5), (2, 8)])):
                mac.wait(mac.rng.uniform(2, 10))
                text, run = pick(mac, [(w, (t, f)) for w, t, f in AGENT[SDK]])
                tool = es.agent_shell(mac, cx, text, cwd=SDK)
                skim(mac, tool, "/etc/zprofile", "~/.zprofile")
                mac.run(tool, "path_helper", ["/usr/libexec/path_helper", "-s"], wait=0.003)
                run(mac, tool, SDK)
                mac.exit(tool)
            if maybe(mac, 0.4):
                scribble(mac, cx, mac.rng.choice(files(mac, SDK, 3)))

        every(mac, day, lambda: mac.rng.uniform(20, 150), turn, stop=mac.t + mac.rng.randint(45, 150) * 60 * S)
        mac.close(cx, rollout, modified=True)
        mac.close(cx, log, modified=True)
        mac.exit(cx)
        mac.exit(front)
        mac.wait(mac.rng.uniform(1, 20))
        mac.logout(sh)
        mac.wait(mac.rng.uniform(5, 40) * 60)
