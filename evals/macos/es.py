"""Builders for the JSON lines `eslogger` writes (message version 10, schema 1,
macOS 26), for the macos pack's evals. Every value is synthetic: the user is
`dev` (/Users/dev, uid 501); the shapes are those of a real capture
(shapecheck.py compares them).

    mac = Mac(seed=1)
    mac.at("2026-10-06T10:00:00Z")
    sh = mac.terminal()                                   # Terminal tab: login, -zsh on a tty
    npm = mac.spawn(sh, "npm", ["npm", "install"], cwd="~/code/acme-web")
    mac.write_file(npm, "~/code/acme-web/node_modules/.package-lock.json")
    mac.exit(npm)

A `Mac` keeps a clock (every event a few microseconds to milliseconds after
the last; `wait` for longer), the processes it has started and the files that
exist, and records every event it builds in `mac.log` with the case it
belongs to (`mac.owner`, set by gen.py) and whether it is a key event.
Paths may start with `~/`. Signing identities and code-signing flags are as
codesign(1) and csops(2) reported them on a macOS 26.3 Mac, unless a comment
says otherwise. Not verified against a capture, and so marked where they are
built: the `open` action's flags value, `image_cpusubtype`, and the shapes of
`btm_launch_item_add` and `tcc_modify` (written from ESMessage.h in the macOS
26.4 SDK; neither was in the capture)."""

import functools
import hashlib
import random
from datetime import datetime, timezone

USER, UID, STAFF = "dev", 501, 20
HOME = "/Users/dev"
ASID = 100008  # the GUI login's audit session
NOBODY_AUID = 4294967295  # AU_DEFAUDITID: processes started before any login

# open(2)'s flags as the kernel holds them (`fflag`): O_RDONLY is FREAD, O_WRONLY FWRITE
FREAD, FWRITE, O_NONBLOCK, O_APPEND = 0x1, 0x2, 0x4, 0x8
O_NOFOLLOW, O_CREAT, O_TRUNC, O_EXCL, O_CLOEXEC = 0x100, 0x200, 0x400, 0x800, 0x1000000

EVENT_TYPE = {"exec": 9, "open": 10, "fork": 11, "close": 12, "create": 13, "exit": 15,
              "rename": 25, "unlink": 32, "btm_launch_item_add": 124, "tcc_modify": 147}

# (codesigning_flags, cs_validation_category) by kind of signature, from csops(2)
PLATFORM = (0x26010B01, 1)         # /bin/zsh, /bin/sh, /bin/bash
PLATFORM_LV = (0x26011B11, 1)      # vim, curl, tail
PLATFORM_KILL = (0x26015B11, 1)    # launchd, security
PLATFORM_DAEMON = (0x26017B01, 1)  # tccd, backgroundtaskmanagementd
PLATFORM_CLT = (0x26006201, 1)     # Command Line Tools' git
DEVID = (0x22010301, 6)            # Claude Code: Developer ID, hardened runtime
DEVID_APP = (0x22011311, 6)        # Ghostty, Cursor
DEVID_TOOL = (0x22010201, 6)       # bun
ADHOC = (0x22000201, 10)           # Homebrew's node and python: linker-signed, ad hoc
UNSIGNED = (0x0, 10)               # assumed; arm64 code cannot run unsigned (Rosetta's x86_64 can)

CPU_ARM64 = 16777228
CPU_SUB_ARM64E = -2147483646  # CPU_SUBTYPE_ARM64E | CPU_SUBTYPE_PTRAUTH_ABI as an int32: not verified
CPU_SUB_ARM64 = 0

SHELLS = ("zsh", "sh", "bash")


@functools.lru_cache(maxsize=None)  # the same few thousand paths, hashed for every event that names them
def _hex(s, n):
    return hashlib.sha256(s.encode()).hexdigest()[:n]


def _h(s):
    return int(_hex(s, 12), 16)


class Program:
    """A program as eslogger names it: `path` is the file the kernel ran (a
    symlink resolved), `dyld` the path it was asked to run when that differs
    (/opt/homebrew/bin/node). A script has `interp` (program, its shebang
    arguments): exec'ing it runs the interpreter with `script` set. `then` is
    a program this one execs in place with the same arguments (the
    xcode-select shim's real tool, rustup's toolchain cargo)."""

    def __init__(self, name, path, signing_id="", team_id=None, cs=PLATFORM, platform=None,
                 dyld=None, then=None, interp=None, size=None, arm64e=None, setuid=False):
        self.name, self.path, self.signing_id, self.team_id = name, path, signing_id, team_id
        self.cs = cs
        self.platform = cs[1] == 1 if platform is None else platform
        self.dyld, self.then, self.interp, self.setuid = dyld, then, interp, setuid
        self.size = size or 40000 + _h(path) % 4000000
        self.arm64e = self.platform if arm64e is None else arm64e
        self.cdhash = "0" * 40 if not signing_id else _hex("cdhash:" + path, 40)


def _adhoc(stem):
    """A linker-signed identifier: the name and a hash of the image"""
    return f"{stem}-{_hex(stem, 16)}"


NODE = "/opt/homebrew/Cellar/node/26.0.0/bin/node"
PY = "/opt/homebrew/Cellar/python@3.14/3.14.4/Frameworks/Python.framework/Versions/3.14"
CLAUDE = f"{HOME}/.local/share/claude/versions/2.1.318"
CODEX_JS = f"{HOME}/.bun/install/global/node_modules/@openai/codex/bin/codex.js"
CODEX_BIN = f"{HOME}/.bun/install/global/node_modules/@openai/codex-darwin-arm64/vendor/aarch64-apple-darwin/codex/codex"
TOOLCHAIN = f"{HOME}/.rustup/toolchains/stable-aarch64-apple-darwin/bin"
CURSOR_AGENT = f"{HOME}/.local/share/cursor-agent/versions/2026.10.22-6f1b2c4"

_P = [
    # macOS
    Program("launchd", "/sbin/launchd", "com.apple.xpc.launchd", cs=PLATFORM_KILL),
    Program("Terminal", "/System/Applications/Utilities/Terminal.app/Contents/MacOS/Terminal", "com.apple.Terminal", cs=PLATFORM_DAEMON, size=2224528),
    Program("login", "/usr/bin/login", "com.apple.login", size=139312, setuid=True),
    Program("path_helper", "/usr/libexec/path_helper", "com.apple.path_helper"),
    Program("zsh", "/bin/zsh", "com.apple.zsh", size=1361200),
    Program("sh", "/bin/sh", "com.apple.sh", size=101232),
    Program("bash", "/bin/bash", "com.apple.bash", size=1310224),
    Program("env", "/usr/bin/env", "com.apple.env", size=102368),
    Program("git", "/usr/bin/git", "com.apple.dt.xcode_select.tool-shim-public", then="clt-git", size=118880),
    Program("clt-git", "/Library/Developer/CommandLineTools/usr/bin/git", "com.apple.git", "59GAB85EFG", PLATFORM_CLT, size=7604272),
    Program("python3", "/usr/bin/python3", "com.apple.dt.xcode_select.tool-shim-public", then="clt-python3", size=118880),
    Program("clt-python3", "/Library/Developer/CommandLineTools/usr/bin/python3", "com.apple.python3", "59GAB85EFG", PLATFORM_CLT),
    Program("curl", "/usr/bin/curl", "com.apple.curl", cs=PLATFORM_LV, size=552624),
    Program("security", "/usr/bin/security", "com.apple.security", cs=PLATFORM_KILL, size=662480),
    Program("launchctl", "/bin/launchctl", "com.apple.xpc.launchctl", size=364448),
    Program("ssh", "/usr/bin/ssh", "com.apple.ssh"),
    Program("tar", "/usr/bin/bsdtar", "com.apple.bsdtar", dyld="/usr/bin/tar", size=274784),  # /usr/bin/tar is a link to bsdtar
    Program("ls", "/bin/ls", "com.apple.ls"),
    Program("cat", "/bin/cat", "com.apple.cat"),
    Program("osascript", "/usr/bin/osascript", "com.apple.osascript"),
    Program("vim", "/usr/bin/vim", "com.apple.vim", cs=PLATFORM_LV, size=5684032),
    Program("tccd", "/System/Library/PrivateFrameworks/TCC.framework/Support/tccd", "com.apple.tccd", cs=PLATFORM_DAEMON),
    Program("backgroundtaskmanagementd", "/System/Library/PrivateFrameworks/BackgroundTaskManagement.framework/Resources/backgroundtaskmanagementd",
            "com.apple.backgroundtaskmanagementd", cs=PLATFORM_DAEMON),
    Program("cfprefsd", "/usr/sbin/cfprefsd", "com.apple.cfprefsd", cs=PLATFORM_DAEMON),
    # terminals and editors (third-party ones signed with their Developer ID)
    Program("iTerm", "/Applications/iTerm.app/Contents/MacOS/iTerm2", "com.googlecode.iterm2", "H7V7XYVQ7D", DEVID_APP),  # team from memory, not checked
    Program("Ghostty", "/Applications/Ghostty.app/Contents/MacOS/ghostty", "com.mitchellh.ghostty", "24VZTF6M5V", DEVID_APP, size=41979664),
    Program("Cursor", "/Applications/Cursor.app/Contents/MacOS/Cursor", "com.todesktop.230313mzl4w4u92", "VDXQ22DGB9", DEVID_APP, size=102336),
    Program("Cursor Helper (Plugin)", "/Applications/Cursor.app/Contents/Frameworks/Cursor Helper (Plugin).app/Contents/MacOS/Cursor Helper (Plugin)",
            "com.github.Electron.helper", "VDXQ22DGB9", DEVID_APP),
    Program("VS Code", "/Applications/Visual Studio Code.app/Contents/MacOS/Electron", "com.microsoft.VSCode", "UBF8T346G9", DEVID_APP),  # not installed here: from memory
    Program("Code Helper (Plugin)", "/Applications/Visual Studio Code.app/Contents/Frameworks/Code Helper (Plugin).app/Contents/MacOS/Code Helper (Plugin)",
            "com.github.Electron.helper", "UBF8T346G9", DEVID_APP),
    Program("Zed", "/Applications/Zed.app/Contents/MacOS/zed", "dev.zed.Zed", "MQ55VZLNZQ", DEVID_APP),  # not installed here: from memory
    # runtimes and package managers
    Program("node", NODE, _adhoc("node"), cs=ADHOC, dyld="/opt/homebrew/bin/node", size=68384),
    Program("npm", "/opt/homebrew/lib/node_modules/npm/bin/npm-cli.js", dyld="/opt/homebrew/bin/npm", interp=("env", ["node"]), size=54),
    Program("npx", "/opt/homebrew/lib/node_modules/npm/bin/npx-cli.js", dyld="/opt/homebrew/bin/npx", interp=("env", ["node"]), size=2922),
    Program("bun", f"{HOME}/.bun/bin/bun", "bun", "7FRXF46ZSN", DEVID_TOOL, size=63072928),
    Program("uv", f"{HOME}/.local/bin/uv", _adhoc("uv"), cs=ADHOC, size=47778352),
    Program("python", f"{PY}/bin/python3.14", _adhoc("python3"), cs=ADHOC, dyld="/opt/homebrew/bin/python3", then="Python.app"),
    Program("Python.app", f"{PY}/Resources/Python.app/Contents/MacOS/Python", "org.python.python", cs=ADHOC),
    Program("pip", "/opt/homebrew/Cellar/python@3.14/3.14.4/bin/pip3", dyld="/opt/homebrew/bin/pip3",
            interp=("python", []), size=230),  # its shebang names .../3.14.4/bin/python3.14, a symlink to `python`
    Program("cargo", f"{HOME}/.cargo/bin/rustup", _adhoc("rustup_init"), cs=ADHOC, dyld=f"{HOME}/.cargo/bin/cargo",
            then="toolchain-cargo", size=11321792),
    Program("toolchain-cargo", f"{TOOLCHAIN}/cargo", _adhoc("cargo"), cs=ADHOC, size=32182376),
    Program("rustc", f"{TOOLCHAIN}/rustc", _adhoc("rustc_main"), cs=ADHOC, size=413480),
    # AI agents
    Program("claude", CLAUDE, "com.anthropic.claude-code", "Q6L2SF6YDW", DEVID, dyld=f"{HOME}/.local/bin/claude", size=319598464),
    Program("codex", CODEX_JS, dyld=f"{HOME}/.bun/bin/codex", interp=("env", ["node"]), size=5741),  # node then spawns codex-native
    Program("codex-native", CODEX_BIN, "codex", "2DC432GLL2", DEVID),
    Program("Codex", "/Applications/Codex.app/Contents/MacOS/Codex", "com.openai.codex", "2DC432GLL2", DEVID_APP),
    # not installed here; from the vendors' install docs: an npm package with a node shebang, and a
    # shell script that runs a bundled node (whose signature is not known: ad hoc assumed)
    Program("gemini", "/opt/homebrew/lib/node_modules/@google/gemini-cli/dist/index.js", dyld="/opt/homebrew/bin/gemini",
            interp=("env", ["node"])),
    Program("cursor-agent", f"{CURSOR_AGENT}/cursor-agent", dyld=f"{HOME}/.local/bin/cursor-agent", interp=("env", ["bash"])),
    Program("cursor-agent-node", f"{CURSOR_AGENT}/node", _adhoc("node"), cs=ADHOC),
    # more of what this Mac runs, for background.py and the cases. Identifiers, teams and install
    # paths as codesign(1) and ls showed them on a macOS 26.3 Mac, unless a comment says otherwise;
    # code-signing flags are those of the like programs above, not read with csops.
    Program("xpcproxy", "/usr/libexec/xpcproxy", "com.apple.xpc.proxy", cs=PLATFORM_DAEMON),
    Program("mdworker_shared", "/System/Library/Frameworks/CoreServices.framework/Versions/A/Frameworks/Metadata.framework"
            "/Versions/A/Support/mdworker_shared", "com.apple.mdworker_shared", cs=PLATFORM_DAEMON),
    Program("ssh-agent", "/usr/bin/ssh-agent", "com.apple.ssh-agent"),
    Program("ssh-add", "/usr/bin/ssh-add", "com.apple.ssh-add"),
    Program("open", "/usr/bin/open", "com.apple.open"),
    Program("rm", "/bin/rm", "com.apple.rm"),
    Program("mkdir", "/bin/mkdir", "com.apple.mkdir"),
    Program("find", "/usr/bin/find", "com.apple.find"),
    Program("grep", "/usr/bin/grep", "com.apple.egrep"),
    Program("sed", "/usr/bin/sed", "com.apple.sed"),
    Program("head", "/usr/bin/head", "com.apple.head"),
    Program("make", "/usr/bin/make", "com.apple.dt.xcode_select.tool-shim-public", then="clt-make", size=118880),
    Program("clt-make", "/Library/Developer/CommandLineTools/usr/bin/make", "com.apple.gnumake", "59GAB85EFG", PLATFORM_CLT),
    Program("c++", "/usr/bin/c++", "com.apple.dt.xcode_select.tool-shim-public", then="clt-clang", size=118880),
    Program("clt-clang", "/Library/Developer/CommandLineTools/usr/bin/clang", "com.apple.clang", "59GAB85EFG", PLATFORM_CLT),
    Program("ld", "/Library/Developer/CommandLineTools/usr/bin/ld", "com.apple.ld", "59GAB85EFG", PLATFORM_CLT),
    # Homebrew's builds are linker-signed ad hoc (Go's linker names every binary a.out)
    Program("gh", "/opt/homebrew/Cellar/gh/2.90.0/bin/gh", "a.out", cs=ADHOC, dyld="/opt/homebrew/bin/gh"),
    # awscli is a Python script; its shebang names the formula's own Python (here es's `python`)
    Program("aws", "/opt/homebrew/Cellar/awscli/2.34.34/bin/aws", dyld="/opt/homebrew/bin/aws", interp=("python", [])),
    Program("brew", "/opt/homebrew/bin/brew", interp=("bash", ["-pu"]), size=9158),  # brew.sh then execs ruby
    Program("ruby", "/opt/homebrew/Library/Homebrew/vendor/portable-ruby/4.0.9/bin/ruby", "ruby", cs=ADHOC),
    Program("node-gyp", "/opt/homebrew/lib/node_modules/npm/node_modules/node-gyp/bin/node-gyp.js", interp=("env", ["node"])),
    # not installed on that Mac: the usual Homebrew and Docker Desktop paths; docker's identifier and team from memory
    Program("rg", "/opt/homebrew/Cellar/ripgrep/15.1.0/bin/rg", _adhoc("rg"), cs=ADHOC, dyld="/opt/homebrew/bin/rg"),
    Program("kubectl", "/opt/homebrew/Cellar/kubernetes-cli/1.34.2/bin/kubectl", "a.out", cs=ADHOC, dyld="/opt/homebrew/bin/kubectl"),
    Program("docker", "/Applications/Docker.app/Contents/Resources/bin/docker", "com.docker.docker", "9BNSXJN65R", DEVID_TOOL,
            dyld="/usr/local/bin/docker"),
]
PROGRAMS = {p.name: p for p in _P}
_BY_PATH = {p.path: p for p in _P}


def program(p):
    """A Program, by name (PROGRAMS) or as given."""
    if isinstance(p, Program):
        return p
    if p in PROGRAMS:
        return PROGRAMS[p]
    raise KeyError(f"no program {p!r}: name one of PROGRAMS, or make one with dropped() or Program()")


def dropped(path, name=None, signed="adhoc"):
    """A program that is not installed the usual way (a binary a script wrote):
    linker-signed ad hoc as every arm64 binary must be (`signing_id` its name
    and a hash), or `signed=None` for no signature at all."""
    name = name or path.rsplit("/", 1)[-1]
    path = path.replace("~/", HOME + "/", 1) if path.startswith("~/") else path
    if signed == "adhoc":
        return Program(name, path, _adhoc(name), cs=ADHOC)
    return Program(name, path, "", cs=UNSIGNED, platform=False)


def ns(t):
    """ISO time, datetime or nanoseconds since the epoch, as nanoseconds."""
    if isinstance(t, int):
        return t
    if isinstance(t, str):
        t = datetime.fromisoformat(t.replace("Z", "+00:00"))
    return int(t.timestamp()) * 10**9 + t.microsecond * 1000


_SECONDS = {}  # iso()'s whole seconds, formatted once: every event has a dozen times, most in the same few seconds
_STAT_TIMES = {}  # a file's birth and modification times, as iso(): the same for every event that names it


def iso(t):
    """eslogger's time: UTC, nanoseconds."""
    s, n = divmod(t, 10**9)
    p = _SECONDS.get(s)
    if p is None:
        p = _SECONDS[s] = datetime.fromtimestamp(s, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{p}.{n:09d}Z"


def _stat_time(t):
    s = _STAT_TIMES.get(t)
    if s is None:
        s = _STAT_TIMES[t] = iso(t)
    return s


def project_dir(cwd):
    """Where Claude Code keeps a working directory's transcripts and memory."""
    return f"{HOME}/.claude/projects/" + "".join(c if c.isalnum() else "-" for c in cwd)


class Proc:
    """A process as eslogger describes it."""

    def __init__(self, **kw):
        self.__dict__.update(kw)

    def __repr__(self):
        return f"<{self.pid} {self.prog.name}>"


# Files that exist on dev's Mac before anything here runs. A case may add its
# own with mac.exists(); every case and the background start from this set.
BASELINE = [
    "~/.zshrc", "~/.zprofile", "~/.gitconfig", "~/.npmrc", "~/.aws/config", "~/.aws/credentials",
    "~/.ssh/config", "~/.ssh/id_ed25519", "~/.ssh/known_hosts", "~/.config/gh/hosts.yml",
    "~/Library/Keychains/login.keychain-db",
    "~/.claude.json", "~/.claude/settings.json", "~/.claude/history.jsonl",
    "~/.codex/config.toml", "~/.codex/auth.json", "~/.codex/history.jsonl", "~/.gemini/settings.json",
    "~/code/acme-web/package.json", "~/code/acme-web/package-lock.json", "~/code/acme-web/CLAUDE.md",
    "~/code/acme-web/src/index.ts", "~/code/acme-web/src/app.ts", "~/code/acme-web/.git/config",
    "~/code/acme-api/Cargo.toml", "~/code/acme-api/Cargo.lock", "~/code/acme-api/AGENTS.md",
    "~/code/acme-api/src/main.rs", "~/code/acme-api/src/lib.rs", "~/code/acme-api/src/routes.rs",
    "~/code/acme-api/.git/config", "~/code/globex-sdk/pyproject.toml", "~/code/globex-sdk/.git/config",
]


class Mac:
    def __init__(self, seed=0, boot="2026-10-05T07:12:09Z"):
        self.seed = seed
        self.rng = random.Random(f"mac:{seed}")
        self.boot = ns(boot)
        self.t = self.boot
        self.log = []  # [event, owner, key]
        self._index = {}  # id(event) -> its row in log
        self.owner = None  # the case being built (gen.py); None for the background
        self._used = {"time": set(), "pid": set(), "pidversion": set(), "thread": set(), "ino": set()}
        self._ttys = 0
        self._baseline = {}
        self.files = {}
        for p in BASELINE:
            self.exists(p)
        self._baseline = dict(self.files)
        self._running = {}
        self.thin = None  # thin(kind, proc) -> True: this fork or exit is not recorded (background.py samples them)
        self._used["pid"].add(1)
        self.launchd = self.daemon("launchd", pid=1)

    # ---- time

    def at(self, when):
        """Set the clock (an ISO time, datetime or ns)."""
        self.t = ns(when)
        return self

    def wait(self, seconds, jitter=0.25):
        """Let `seconds` pass, give or take `jitter` of it."""
        self.t += int(seconds * (1 + self.rng.uniform(-jitter, jitter)) * 10**9)
        return self

    @property
    def now(self):
        return iso(self.t)

    def _stamp(self, gap=None):
        self.t += gap if gap is not None else self.rng.randint(40_000, 2_500_000)
        while self.t in self._used["time"]:
            self.t += 1
        self._used["time"].add(self.t)
        return self.t

    def _ticket(self, kind, base, per_sec, wrap=None):
        """A pid, pidversion or thread id: growing with the clock, never reused."""
        n = base + (self.t - self.boot) * per_sec // 10**9
        if wrap:
            n = base + (n - base) % wrap
        tries = 0
        while n in self._used[kind]:
            n = n + 1 if not wrap or n + 1 < base + wrap else base
            tries += 1
            if wrap and tries > wrap:
                raise RuntimeError(f"every {kind} from {base} to {base + wrap - 1} is taken: a stream this long would reuse one")
        self._used[kind].add(n)
        return n

    # ---- marking

    def key(self, ev):
        """Mark an event (or a list of them) as one a person should see."""
        for e in ev if isinstance(ev, list) else [ev]:
            self.log[self._index[id(e)]][2] = True
        return ev

    @property
    def last(self):
        return self.log[-1][0]

    # ---- files

    def _p(self, path):
        return HOME + path[1:] if path.startswith("~/") or path == "~" else path

    def exists(self, path, size=None, days=None):
        """Declare that a file exists (before this case's events)."""
        path = self._p(path)
        h = _h(path)
        age = (days if days is not None else 3 + h % 300) * 86400 * 10**9
        birth = self.boot - age
        self.files[path] = {"ino": self._ino(path), "size": size if size is not None else 200 + h % 30000,
                            "birth": birth, "mtime": birth + (h % (age // 10**9 or 1)) * 10**9}
        return path

    def reset_files(self):
        """Back to BASELINE: gen.py calls this before each case and the background."""
        self.files = {p: dict(m) for p, m in self._baseline.items()}

    def _ino(self, path):
        n = 1_000_000 + _h("ino:" + path) % 60_000_000
        while n in self._used["ino"]:
            n += 1
        self._used["ino"].add(n)
        return n

    def _stat(self, path, kind, prog=None):
        h = _h(path)
        system = path.startswith(("/System/", "/bin/", "/sbin/", "/usr/")) and not path.startswith("/usr/local/")
        uid, gid = (0, 0) if system or path.startswith(("/Library/", "/private/var/")) else (UID, STAFF)
        if path.startswith(("/opt/homebrew/", "/Applications/")):
            gid = 80
        flags, dev, rdev, blksize = (524320 if system else 0), 16777234, 0, 4096
        if kind == "tty":
            n = int(path[-3:]) if path[-3:].isdigit() else 0
            ino, size, mode, uid, gid, dev, rdev, blksize = 700 + n, 0, 0o20620, UID, 4, 852713282, 268435456 + n, 65536
            birth = mtime = self.boot
        elif kind == "dir":
            ino = (1152921500311000000 if system else 300000) + h % 2_000_000
            size, mode = 64 + 32 * (h % 40), 0o40755
            birth = self.boot - (60 + h % 400) * 86400 * 10**9
            mtime = max(birth, self.t - (h % 86400) * 10**9)
        elif kind == "exe":
            p = prog or _BY_PATH.get(path)  # the process's own program: a case's Program is not in _BY_PATH
            ino = (1152921500311000000 if system else 2_000_000) + h % 2_000_000
            size, mode = (p.size if p else 40000 + h % 4000000), (0o100555 if "/Cellar/" in path else 0o100755)
            birth = mtime = self.boot - (20 + h % 90) * 86400 * 10**9
            meta = self.files.get(path)
            if meta:  # a program a case wrote
                ino, size, birth, mtime = meta["ino"], meta["size"], meta["birth"], meta["mtime"]
        else:
            meta = self.files.get(path)
            if meta is None:  # read, so it exists: system files and the like
                meta = {"ino": (1152921500311000000 + h % 2_000_000) if system else self._ino(path),
                        "size": 100 + h % 200000, "birth": self.boot - (30 + h % 300) * 86400 * 10**9}
                meta["mtime"] = meta["birth"]
                self.files[path] = meta
            ino, size, birth, mtime = meta["ino"], meta["size"], meta["birth"], meta["mtime"]
            mode = 0o100600 if "/.ssh/" in path or "/.aws/" in path or path.endswith(".keychain-db") else 0o100644
        b, m = _stat_time(birth), _stat_time(mtime)
        return {"st_atimespec": iso(max(mtime, self.t - 3600 * 10**9)), "st_birthtimespec": b,
                "st_blksize": blksize, "st_blocks": -(-size // 4096) * 8, "st_ctimespec": m,
                "st_dev": dev, "st_flags": flags, "st_gen": 0, "st_gid": gid, "st_ino": ino, "st_mode": mode,
                "st_mtimespec": m, "st_nlink": 1, "st_rdev": rdev, "st_size": size, "st_uid": uid}

    def _file(self, path, kind="file", prog=None):
        return {"path": path, "path_truncated": False, "stat": self._stat(path, kind, prog)}

    def _dir(self, path):
        return self._file(path.rstrip("/") or "/", "dir")

    # ---- processes

    def _token(self, p):
        return {"asid": p.asid, "auid": p.auid, "egid": p.egid, "euid": p.euid, "pid": p.pid,
                "pidversion": p.pv, "rgid": p.rgid, "ruid": p.ruid}

    def _block(self, p):
        prog = p.prog
        return {
            "audit_token": self._token(p),
            "cdhash": prog.cdhash,
            "codesigning_flags": prog.cs[0],
            "cs_validation_category": prog.cs[1],
            "executable": self._file(prog.path, "exe", prog),
            "group_id": p.pgid,
            "is_es_client": False,
            "is_platform_binary": prog.platform,
            "original_ppid": p.oppid,
            "parent_audit_token": p.parent_token,
            "ppid": p.ppid,
            "responsible_audit_token": self._token(p.responsible),
            "session_id": p.sid,
            "signing_id": prog.signing_id,
            "start_time": iso(p.start - p.start % 1000),  # a timeval: microseconds
            "team_id": prog.team_id,
            "tty": self._file(p.tty, "tty") if p.tty else None,
        }

    def daemon(self, name, pid=None, user=False):
        """A process running since boot (launchd's child), with no events of
        its own until it acts: tccd, backgroundtaskmanagementd, cfprefsd. The
        same one each time it is asked for; `user=True` for the user's own
        instance (the user's tccd)."""
        k = ("daemon", name, user)
        if k in self._running:
            return self._running[k]
        t, self.t = self.t, self.boot + 2 * 10**9 + _h(name) % (40 * 10**9)
        pid = pid or self._ticket("pid", 90, 1, 99000)
        uid = UID if user else 0
        p = Proc(pid=pid, pv=self._ticket("pidversion", 1000, 2), ppid=0 if pid == 1 else 1, oppid=0 if pid == 1 else 1,
                 ruid=uid, euid=uid, rgid=STAFF if user else 0, egid=STAFF if user else 0, asid=ASID if user else 100000,
                 auid=UID if user else NOBODY_AUID, tty=None, pgid=pid, sid=pid, start=self.t, prog=program(name),
                 cwd="/", env={}, thread=self._ticket("thread", 1000, 50), interactive=False, args=[name])
        p.responsible = p
        p.parent_token = self._token(self.launchd) if pid != 1 else self._token(p)
        self.t = t
        self._running[k] = p
        return p

    def app(self, name):
        """A GUI app the person has had open since they logged in (launchd's
        child, responsible for itself, no tty). The same one each time."""
        k = ("app", name)
        if k in self._running:
            return self._running[k]
        t, self.t = self.t, self.boot + 90 * 10**9 + _h(name) % (60 * 10**9)
        pid = self._ticket("pid", 400, 1, 99000)
        p = Proc(pid=pid, pv=self._ticket("pidversion", 1000, 2), ppid=1, oppid=1, ruid=UID, euid=UID, rgid=STAFF,
                 egid=STAFF, asid=ASID, auid=UID, tty=None, pgid=pid, sid=pid, start=self.t, prog=program(name),
                 cwd="/", env=dict(_GUI_ENV), thread=self._ticket("thread", 1000, 50), interactive=False, args=[program(name).path])
        p.responsible = p
        p.parent_token = self._token(self.launchd)
        self.t = t
        self._running[k] = p
        return p

    def _event(self, proc, kind, body, *, block=None, key=False, result=None):
        ev = {
            "action": {"result": result or {"result": {"auth": 0}, "result_type": 0}},
            "action_type": 1,  # notify
            "event": {kind: body},
            "event_type": EVENT_TYPE[kind],
            "global_seq_num": 0,  # numbered in time order by number()
            "mach_time": (self.t - self.boot) * 3 // 125,  # 24 MHz ticks since boot
            "process": block or self._block(proc),
            "schema_version": 1,
            "seq_num": 0,
            "thread": {"thread_id": proc.thread},
            "time": iso(self.t),
            "version": 10,
        }
        self._index[id(ev)] = len(self.log)
        self.log.append([ev, self.owner, bool(key)])
        return ev

    def fork(self, parent, *, key=False):
        """`parent` forks: the child runs the parent's program until it execs."""
        self._stamp()
        pid = self._ticket("pid", 400, 1, 99000)
        c = Proc(**{k: v for k, v in parent.__dict__.items()})
        c.env = dict(parent.env)
        c.pid, c.pv, c.ppid, c.oppid, c.start = pid, self._ticket("pidversion", 1000, 2), parent.pid, parent.pid, self.t
        c.parent_token = self._token(parent)
        c.thread = self._ticket("thread", 1000, 50)
        c.interactive = False
        if parent.interactive:  # a job of an interactive shell gets its own process group
            c.pgid = pid
        if key or not (self.thin and self.thin("fork", parent)):
            self._event(parent, "fork", {"child": self._block(c)}, key=key)
        return c

    def exec(self, proc, prog, args=None, *, env=None, env_add=None, cwd=None, fds=None, key=False):
        """`proc` runs `prog` in place (execve), with `args` (argv, argv[0]
        included). A script runs its interpreter (`/usr/bin/env node` is two
        execs: env, then node); a shim runs its real tool. `env` replaces the
        environment, `env_add` sets names in it; a shell parent also sets `_`.
        Returns the last exec event; `key` marks it."""
        prog = program(prog)
        invoked = prog.dyld or prog.path
        args = list(args) if args else [invoked.rsplit("/", 1)[-1]]
        if cwd:
            proc.cwd = self._p(cwd)
        e = dict(proc.env) if env is None else dict(env)
        if proc.prog.name in SHELLS and env is None:  # what a shell exports to its commands
            e["PWD"], e["_"] = proc.cwd, invoked
        if env_add:
            e.update(env_add)
        steps = []  # (program, argv, dyld_exec_path, script)
        if prog.interp:
            ip, iargs = program(prog.interp[0]), prog.interp[1]
            shebang = "/opt/homebrew/Cellar/python@3.14/3.14.4/bin/python3.14" if ip.name == "python" else ip.path
            steps.append((ip, [shebang] + iargs + [invoked] + args[1:], shebang, prog.path))
            if ip.name == "env":  # env finds the interpreter on PATH and execs it
                real = program(iargs[0])
                steps.append((real, [iargs[0], invoked] + args[1:], real.dyld or real.path, None))
        else:
            steps.append((prog, args, invoked, None))
        while steps[-1][0].then:
            nxt = program(steps[-1][0].then)
            steps.append((nxt, steps[-1][1], nxt.path, None))
        ev = None
        for i, (p, argv, dyld, script) in enumerate(steps):
            self._stamp()
            before = self._block(proc)
            proc.prog, proc.pv, proc.args, proc.env = p, self._ticket("pidversion", 1000, 2), argv, e
            if p.setuid:  # setuid root: the effective uid is root from this exec on
                proc.euid = 0
            body = {
                "args": argv,
                "cwd": self._dir(proc.cwd),
                "dyld_exec_path": dyld,
                "env": [f"{k}={v}" for k, v in e.items()],
                "fds": self._fds(proc, fds),
                "image_cpusubtype": CPU_SUB_ARM64E if p.arm64e else CPU_SUB_ARM64,
                "image_cputype": CPU_ARM64,
                "last_fd": 2,
                "script": self._file(script) if script else None,
                "target": self._block(proc),
            }
            ev = self._event(proc, "exec", body, block=before, key=key and i == len(steps) - 1)
        return ev

    def _fds(self, proc, fds):
        """stdin, stdout, stderr: `tty`, `null` or a `file` (vnodes, fdtype 1), or `pipe`."""
        fds = fds or ("tty" if proc.tty else "null")
        if fds == "pipe":
            base = self._ticket("thread", 5_000_000, 50)
            return [{"fd": i, "fdtype": 6, "pipe": {"pipe_id": base + i}} for i in range(3)]
        return [{"fd": i, "fdtype": 1} for i in range(3)]

    def spawn(self, parent, prog, args=None, *, env=None, env_add=None, cwd=None, fds=None, setsid=False, key=False):
        """fork + exec: `parent` starts `prog`. `setsid` starts it in a new
        session with no terminal (an agent's tool shell, a detached child).
        Returns the child; `key` marks its (last) exec."""
        c = self.fork(parent)
        if setsid:
            c.sid = c.pgid = c.pid
            c.tty = None
        self.exec(c, prog, args, env=env, env_add=env_add, cwd=cwd, fds=fds, key=key)
        return c

    def exit(self, proc, status=0, *, key=False):
        self._stamp()
        if not key and self.thin and self.thin("exit", proc):
            return None
        return self._event(proc, "exit", {"stat": status << 8}, key=key)

    def run(self, parent, prog, args=None, *, wait=0.05, status=0, key=False, **kw):
        """spawn, let it run `wait` seconds, exit: a command that does nothing
        a file event would show (git status, ls). Returns the child."""
        c = self.spawn(parent, prog, args, key=key, **kw)
        self.wait(wait)
        self.exit(c, status)
        return c

    # ---- a terminal

    def terminal(self, app="Terminal", cwd=None):
        """A new tab or window of `app` (Terminal, iTerm, Ghostty, running since
        login): the app forks, the child takes a new tty and session and execs
        login, which forks the person's login shell, -zsh, as dev. Returns the
        shell, interactive, in `cwd` (default ~)."""
        a = self.app(app)
        self._ttys += 1
        tty = f"/dev/ttys{self._ttys % 100:03d}"
        c = self.fork(a)
        c.sid = c.pgid = c.pid
        c.tty = tty
        c.env = dict(_GUI_ENV, **_TERM_ENV.get(app, {}), TERM_SESSION_ID=self.uuid())
        login_args = {"Terminal": ["login", "-pf", USER]}.get(app, ["/usr/bin/login", "-flp", USER, "/bin/zsh"])
        self.exec(c, "login", login_args)  # login is setuid root
        self.wait(0.01)
        sh = self.fork(c)
        sh.ruid, sh.euid, sh.rgid, sh.egid = UID, UID, STAFF, STAFF  # login's child drops to dev before the exec
        env = dict(c.env, HOME=HOME, SHELL="/bin/zsh", USER=USER, LOGNAME=USER, PATH=_LOGIN_PATH, SHLVL="1",
                   PWD=self._p(cwd or "~"), OLDPWD=HOME, **_BREW_ENV)
        self.exec(sh, "zsh", ["-zsh"], env=env, cwd=HOME)
        sh.interactive = True
        for f in ("/etc/zprofile", "~/.zprofile", "/etc/zshrc", "~/.zshrc"):
            self.read_file(sh, f)
        self.run(sh, "path_helper", ["/usr/libexec/path_helper", "-s"], wait=0.003)
        sh.cwd = self._p(cwd or "~")
        sh.login = c
        return sh

    def logout(self, sh):
        """The tab closes: its shell exits, then login."""
        self.exit(sh)
        self.wait(0.002)
        self.exit(sh.login)

    def uuid(self):
        h = f"{self.rng.getrandbits(128):032x}"
        return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-{'89ab'[int(h[16], 16) % 4]}{h[17:20]}-{h[20:]}".upper()

    # ---- file events

    def open(self, proc, path, flags=FREAD, *, key=False):
        """open(2): `flags` as the kernel holds them (FREAD, FWRITE, O_*)."""
        self._stamp()
        path = self._p(path)
        # the action's flags for a notify open: not verified (assumed all-allowed)
        return self._event(proc, "open", {"fflag": flags, "file": self._file(path)}, key=key,
                           result={"result": {"flags": 4294967295}, "result_type": 1})

    def close(self, proc, path, modified=False, *, key=False):
        self._stamp()
        path = self._p(path)
        if modified and path in self.files:
            m = self.files[path]
            m["mtime"], m["size"] = self.t, m["size"] + self.rng.randint(1, 600)
        return self._event(proc, "close", {"modified": modified, "target": self._file(path),
                                           "was_mapped_writable": False}, key=key)

    def create(self, proc, path, *, key=False):
        """A new file. A notify create names the file as it now exists
        (`existing_file`); the capture has no other kind."""
        self._stamp()
        path = self._p(path)
        self.files[path] = {"ino": self._ino(path + f"@{self.t}"), "size": 0, "birth": self.t, "mtime": self.t}
        return self._event(proc, "create", {"acl": None, "destination": {"existing_file": self._file(path)},
                                            "destination_type": 0}, key=key)

    def rename(self, proc, src, dst, *, key=False):
        """rename(2): over an existing file (`existing_file`) or to a new name (`new_path`)."""
        self._stamp()
        src, dst = self._p(src), self._p(dst)
        body = {"source": self._file(src)}
        if dst in self.files:
            body.update(destination={"existing_file": self._file(dst)}, destination_type=0)
        else:
            d, name = dst.rsplit("/", 1)
            body.update(destination={"new_path": {"dir": self._dir(d), "filename": name}}, destination_type=1)
        self.files[dst] = self.files.pop(src, None) or {"ino": self._ino(dst), "size": 100, "birth": self.t, "mtime": self.t}
        return self._event(proc, "rename", body, key=key)

    def unlink(self, proc, path, *, key=False):
        self._stamp()
        path = self._p(path)
        ev = self._event(proc, "unlink", {"parent_dir": self._dir(path.rsplit("/", 1)[0]), "target": self._file(path)}, key=key)
        self.files.pop(path, None)
        return ev

    # ---- the common shapes

    def write_file(self, proc, path, *, append=False, new=None, key=False):
        """A write as most programs make it: create (when the file is new),
        open for writing (truncating, or `append`), close modified. `key`
        marks all three. Returns the events."""
        path = self._p(path)
        new = path not in self.files if new is None else new
        out = [self.create(proc, path)] if new else []
        flags = FWRITE | O_CREAT | O_CLOEXEC | (O_APPEND if append else O_TRUNC)
        out.append(self.open(proc, path, flags))
        self.wait(0.0005)
        out.append(self.close(proc, path, modified=True))
        return self.key(out) if key else out

    def read_file(self, proc, path, *, key=False):
        """open for reading, close unmodified; `key` marks the open."""
        out = [self.open(proc, path, FREAD | O_CLOEXEC, key=key)]
        out.append(self.close(proc, path))
        return out

    def atomic_write(self, proc, path, *, tmp=None, key=False):
        """Write a tmp file beside `path` and rename it over: `key` marks the
        rename (the tmp file's own events are marked only when asked, with
        mac.key). Returns the events."""
        path = self._p(path)
        tmp = self._p(tmp) if tmp else f"{path}.tmp.{proc.pid}.{self.rng.getrandbits(32):08x}"
        out = self.write_file(proc, tmp, new=True)
        out.append(self.rename(proc, tmp, path, key=key))
        return out

    # ---- rare events, kept whole by the plugin; shapes from ESMessage.h, not from a capture

    def btm_launch_item_add(self, item_url, *, item_type=3, legacy=True, managed=False, uid=UID, app_url=None,
                            executable_path="", instigator=None, app=None, key=False):
        """Background Task Management saw a new login item; reported by
        backgroundtaskmanagementd. item_type: 0 user item, 1 app, 2 login
        item, 3 agent, 4 daemon. `instigator`, `app`: Procs or None."""
        self._stamp()
        d = self.daemon("backgroundtaskmanagementd")
        body = {"app": self._block(app) if app else None, "app_token": self._token(app) if app else None,
                "executable_path": executable_path,
                "instigator": self._block(instigator) if instigator else None,
                "instigator_token": self._token(instigator) if instigator else None,
                "item": {"app_url": app_url, "item_type": item_type, "item_url": item_url, "legacy": legacy,
                         "managed": managed, "uid": uid}}
        return self._event(d, "btm_launch_item_add", body, key=key)

    def tcc_modify(self, service, identity, *, identity_type=0, update_type=1, right=2, reason=3, instigator=None,
                   responsible=None, system=False, key=False):
        """A privacy permission changed (macOS 15.4+), reported by tccd (the
        user's, or root's for `system` services such as Full Disk Access).
        identity_type: 0 bundle id, 1 path; update_type: 1 create, 2 modify,
        3 delete; right: 0 denied, 2 allowed; reason: 2 user consent, 3 user
        set, 4 system set."""
        self._stamp()
        d = self.daemon("tccd", user=not system)
        inst = instigator or d
        body = {"identity": identity, "identity_type": identity_type,
                "instigator": self._block(inst), "instigator_token": self._token(inst),
                "reason": reason, "responsible": self._block(responsible) if responsible else None,
                "responsible_token": self._token(responsible) if responsible else None,
                "right": right, "service": service, "update_type": update_type}
        return self._event(d, "tcc_modify", body, key=key)


# ---- what programs do

def vim_edit(mac, shell, path, *, saves=1, key=False):
    """The person edits `path` in /usr/bin/vim from `shell`, as vim's defaults
    write a file: read it, keep a swap file beside it, and on each :w test the
    directory with a `4913` file, rename the file to a `~` backup, write it
    new, unlink the backup. `key` marks each save's rename, create, open and
    close. Returns the vim process (exited)."""
    path = mac._p(path)
    d, name = path.rsplit("/", 1)
    vim = mac.spawn(shell, "vim", ["vim", path])
    mac.read_file(vim, path)
    swap = f"{d}/.{name}.swp"
    mac.create(vim, swap)
    mac.open(vim, swap, FREAD | FWRITE | O_CREAT | O_EXCL | O_NOFOLLOW)
    for _ in range(saves):
        mac.wait(mac.rng.uniform(20, 240))
        mac.write_file(vim, f"{d}/4913", new=True)
        mac.unlink(vim, f"{d}/4913")
        out = [mac.rename(vim, path, path + "~"), mac.create(vim, path),
               mac.open(vim, path, FWRITE | O_CREAT | O_TRUNC)]
        mac.wait(0.002)
        out.append(mac.close(vim, path, modified=True))
        mac.unlink(vim, path + "~")
        if key:
            mac.key(out)
    mac.wait(mac.rng.uniform(2, 30))
    mac.close(vim, swap, modified=True)
    mac.unlink(vim, swap)
    mac.exit(vim)
    return vim


def claude(mac, shell, cwd, args=None, *, env_add=None, key=False):
    """The person starts Claude Code from `shell` in `cwd`: it reads its
    settings, snapshots the shell (a zsh child writes
    ~/.claude/shell-snapshots/), reads its credential from the keychain
    (security), and saves ~/.claude.json (tmp + rename). Returns the claude
    process, with `.session` (its transcript) and `.snapshot`."""
    cwd = mac._p(cwd)
    c = mac.spawn(shell, "claude", args or ["claude"], cwd=cwd, env_add=env_add, key=key)
    c.env.update(CLAUDECODE="1", CLAUDE_CODE_ENTRYPOINT="cli", GIT_EDITOR="true", COREPACK_ENABLE_AUTO_PIN="0")
    c.env.pop("_", None)
    c.session = f"{project_dir(cwd)}/{mac.uuid().lower()}.jsonl"
    c.snapshot = f"{HOME}/.claude/shell-snapshots/snapshot-zsh-{mac.t // 10**6}-{mac.rng.getrandbits(30):06x}.sh"
    for f in ("~/.claude/settings.json", f"{cwd}/.claude/settings.json", f"{cwd}/CLAUDE.md", "~/.claude.json"):
        if mac._p(f) in mac.files:
            mac.read_file(c, f)
    snap = mac.spawn(c, "zsh", ["/bin/zsh", "-c", "-l", f"SNAPSHOT_FILE={c.snapshot}; source ~/.zshrc < /dev/null; typeset -f > $SNAPSHOT_FILE"],
                     setsid=True)
    mac.read_file(snap, "~/.zshrc")
    mac.write_file(snap, c.snapshot)
    mac.exit(snap)
    mac.run(c, "security", ["security", "find-generic-password", "-a", USER, "-w", "-s", "Claude Code-credentials"],
            setsid=True, wait=0.03)
    mac.atomic_write(c, "~/.claude.json")
    return c


def claude_prompt(mac, c, *, reply=True):
    """The person sends Claude a prompt: history.jsonl and the transcript
    appended; `reply` appends the model's answer after a pause."""
    mac.write_file(c, "~/.claude/history.jsonl", append=True)
    mac.write_file(c, c.session, append=True)
    if reply:
        mac.wait(mac.rng.uniform(2, 15))
        mac.write_file(c, c.session, append=True)


def agent_shell(mac, agent, command, *, cwd=None):
    """The shell an agent runs a command in, in a session of its own, no
    terminal, stdio not a tty: Claude Code's `zsh -c "source <snapshot> ...
    eval '<command>'"` (as seen on this Mac), Codex's `zsh -lc` and Gemini's
    `bash -c` (from their docs, not seen). Returns the shell, which forks
    whatever the command runs; exit it when done."""
    name = agent.prog.name
    if name == "claude":
        cwdfile = f"/var/folders/7x/k2m9q4z51b3c0000gn/T/claude-{mac.rng.getrandbits(16):04x}-cwd"
        line = (f"source {agent.snapshot} 2>/dev/null || true && setopt NO_EXTENDED_GLOB NO_BARE_GLOB_QUAL 2>/dev/null"
                f" || true && eval '{command}' < /dev/null && pwd -P >| {cwdfile}")
        args, prog = ["/bin/zsh", "-c", line], "zsh"
    elif name in ("codex-native", "Codex"):
        args, prog = ["/bin/zsh", "-lc", command], "zsh"
    elif name == "node":  # gemini (cursor-agent's node is "cursor-agent-node", so it takes the zsh -c below; its shell is not documented)
        args, prog = ["bash", "-c", command], "bash"
    else:
        args, prog = ["/bin/zsh", "-c", command], "zsh"
    sh = mac.spawn(agent, prog, args, cwd=cwd, setsid=True, fds="null" if name == "claude" else "pipe")
    if name == "claude":
        mac.read_file(sh, agent.snapshot)
    return sh


def npm_lifecycle(mac, npm, pkg, command, *, event="postinstall", version="1.0.0"):
    """npm runs a package's lifecycle script as `sh -c <command>` in the
    package's directory, stdio piped, with npm's variables set (npm 11).
    Returns the sh process: exec what the command runs in it (sh -c with one
    command execs it in place), or spawn from it."""
    pkg = mac._p(pkg)
    project = npm.cwd
    name = pkg.rsplit("/node_modules/", 1)[-1]
    env = {
        "npm_lifecycle_event": event, "npm_lifecycle_script": command, "npm_package_name": name,
        "npm_package_version": version, "npm_package_json": f"{pkg}/package.json", "npm_command": "install",
        "npm_config_user_agent": "npm/11.6.0 node/v26.0.0 darwin arm64 workspaces/false",
        "npm_node_execpath": NODE, "npm_execpath": "/opt/homebrew/lib/node_modules/npm/bin/npm-cli.js",
        "NODE": NODE, "INIT_CWD": project, "COLOR": "0", "EDITOR": "vi",
        "PATH": f"{pkg}/node_modules/.bin:{project}/node_modules/.bin:/opt/homebrew/lib/node_modules/npm/node_modules/@npmcli/run-script/lib/node-gyp-bin:" + npm.env.get("PATH", _LOGIN_PATH),
    }
    return mac.spawn(npm, "sh", ["sh", "-c", command], cwd=pkg, env_add=env, fds="pipe")


def number(events):
    """seq_num (per event type) and global_seq_num, in the order given (time order)."""
    per = {}
    for i, ev in enumerate(events, 1):
        k = ev["event_type"]
        per[k] = per.get(k, 0) + 1
        ev["global_seq_num"], ev["seq_num"] = i, per[k]


def key_path(ev):
    """The file a file event touched; for an exec, the program that started."""
    kind, b = next(iter(ev["event"].items()))
    if kind == "exec":
        return b["target"]["executable"]["path"]
    if kind == "open":
        return b["file"]["path"]
    if kind in ("close", "unlink"):
        return b["target"]["path"]
    if kind in ("create", "rename"):
        d = b["destination"]
        if "existing_file" in d:
            return d["existing_file"]["path"]
        return d["new_path"]["dir"]["path"].rstrip("/") + "/" + d["new_path"]["filename"]
    if kind == "btm_launch_item_add":
        return b["executable_path"] or b["item"]["item_url"]
    if kind == "tcc_modify":
        return b["identity"]
    return ev["process"]["executable"]["path"]


_GUI_ENV = {
    "__CF_USER_TEXT_ENCODING": "0x1F5:0x0:0x0", "COMMAND_MODE": "unix2003", "HOME": HOME, "LOGNAME": USER,
    "PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "SHELL": "/bin/zsh", "SSH_AUTH_SOCK": "/private/tmp/com.apple.launchd.Xq3vT8kd2L/Listeners",
    "TMPDIR": "/var/folders/7x/k2m9q4z51b3c0000gn/T/", "USER": USER, "XPC_FLAGS": "0x0", "XPC_SERVICE_NAME": "0",
}
_TERM_ENV = {
    "Terminal": {"TERM": "xterm-256color", "TERM_PROGRAM": "Apple_Terminal", "TERM_PROGRAM_VERSION": "466", "LANG": "en_US.UTF-8"},
    "iTerm": {"TERM": "xterm-256color", "TERM_PROGRAM": "iTerm.app", "TERM_PROGRAM_VERSION": "3.6.2", "LANG": "en_US.UTF-8", "COLORTERM": "truecolor"},
    "Ghostty": {"TERM": "xterm-ghostty", "TERM_PROGRAM": "ghostty", "TERM_PROGRAM_VERSION": "1.2.3", "LANG": "en_US.UTF-8", "COLORTERM": "truecolor"},
}
_LOGIN_PATH = ("/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin"
               f":{HOME}/.cargo/bin:{HOME}/.local/bin:{HOME}/.bun/bin")
_BREW_ENV = {"HOMEBREW_PREFIX": "/opt/homebrew", "HOMEBREW_CELLAR": "/opt/homebrew/Cellar", "HOMEBREW_REPOSITORY": "/opt/homebrew",
             "INFOPATH": "/opt/homebrew/share/info:", "BUN_INSTALL": f"{HOME}/.bun"}
