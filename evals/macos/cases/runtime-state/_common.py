"""Shared builders for the runtime-state cases (gen.py skips files that start
with `_`). Load it from a case with:

    import importlib.util, os
    _s = importlib.util.spec_from_file_location("rs_common", os.path.join(os.path.dirname(__file__), "_common.py"))
    rs = importlib.util.module_from_spec(_s); _s.loader.exec_module(rs)

Programs not in es.PROGRAMS: identifiers are codesign(1)'s on a macOS 26.3
Mac; their code-signing flags are assumed to be the platform default
(es.PLATFORM), not read with csops."""
import es

P = es.Program
CLT = "/Library/Developer/CommandLineTools/usr/libexec/git-core"

DEFAULTS = P("defaults", "/usr/bin/defaults", "com.apple.defaults", size=178560)
OPEN = P("open", "/usr/bin/open", "com.apple.open", size=306720)
PGREP = P("pgrep", "/usr/bin/pgrep", "com.apple.pkill", size=138224)
GREP = P("grep", "/usr/bin/grep", "com.apple.egrep", size=153760)
HEAD = P("head", "/usr/bin/head", "com.apple.head", size=101952)
WC = P("wc", "/usr/bin/wc", "com.apple.wc", size=102240)
CP = P("cp", "/bin/cp", "com.apple.cp", size=153360)
MKDIR = P("mkdir", "/bin/mkdir", "com.apple.mkdir", size=101472)
SCP = P("scp", "/usr/bin/scp", "com.apple.scp", size=305664)
# /usr/bin/tar is a symlink to bsdtar: the kernel runs bsdtar (es.PROGRAMS' "tar" names the link)
BSDTAR = P("tar", "/usr/bin/bsdtar", "com.apple.bsdtar", dyld="/usr/bin/tar", size=274784)
GIT_REMOTE_HTTPS = P("git-remote-https", f"{CLT}/git-remote-http", "com.apple.git-remote-http", "59GAB85EFG", es.PLATFORM_CLT,
                     dyld=f"{CLT}/git-remote-https")  # a symlink to git-remote-http
SSHD_WRAPPER = P("sshd-keygen-wrapper", "/usr/libexec/sshd-keygen-wrapper", "com.apple.sshd-keygen-wrapper", size=410528)
SSHD = P("sshd", "/usr/sbin/sshd", "com.apple.sshd", size=763152)
SSHD_SESSION = P("sshd-session", "/usr/libexec/sshd-session", "com.apple.sshd-session", size=1502960)

TMPDIR = "/var/folders/7x/k2m9q4z51b3c0000gn/T"


def app(mac, prog):
    """mac.app for a Program not in es.PROGRAMS (mac.app takes a name): a
    process launchd started as dev at login, responsible for itself, no tty."""
    k = ("app", prog.path)
    if k in mac._running:
        return mac._running[k]
    t, mac.t = mac.t, mac.boot + 90 * 10**9 + es._h(prog.path) % (60 * 10**9)
    pid = mac._ticket("pid", 400, 1, 99000)
    p = es.Proc(pid=pid, pv=mac._ticket("pidversion", 1000, 2), ppid=1, oppid=1, ruid=es.UID, euid=es.UID, rgid=es.STAFF,
                egid=es.STAFF, asid=es.ASID, auid=es.UID, tty=None, pgid=pid, sid=pid, start=mac.t, prog=prog, cwd="/",
                env=dict(es._GUI_ENV), thread=mac._ticket("thread", 1000, 50), interactive=False, args=[prog.path])
    p.responsible = p
    p.parent_token = mac._token(mac.launchd)
    mac.t = t
    mac._running[k] = p
    return p


def launchd_job(mac, label, *, user=False):
    """launchd starts a job: it forks, and the child execs xpcproxy, which
    sets the job up and execs its program (exec that next). `user`: a job in
    dev's GUI domain (an app), as dev; otherwise a system job, as root. The
    xpcproxy step and its argument (the job's label) are from memory, not
    verified against a capture. Returns the child, running xpcproxy."""
    c = mac.fork(mac.launchd)
    c.sid = c.pgid = c.pid
    c.responsible = c
    if user:
        c.ruid, c.euid, c.rgid, c.egid, c.auid, c.asid = es.UID, es.UID, es.STAFF, es.STAFF, es.UID, es.ASID
    mac.exec(c, "xpcproxy", ["xpcproxy", label], env={})
    return c


def orphan(mac, proc):
    """`proc`'s parent has exited: the kernel reparents it to launchd, so its
    `ppid` (and parent token) name launchd from now on; `original_ppid` keeps
    the parent it was born to (ESMessageCore.h)."""
    proc.ppid = 1
    proc.parent_token = mac._token(mac.launchd)


# the environment launchd gives com.openssh.sshd (`launchctl print system/com.openssh.sshd` on macOS 26.3)
SSHD_ENV = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "OSLogRateLimit": "64", "MallocSpaceEfficient": "0",
            "XPC_SERVICE_NAME": "com.openssh.sshd"}


def ssh_login(mac, client="203.0.113.24", cwd=None):
    """An inbound SSH login as dev (Remote Login on). From the ssh.plist on
    macOS 26: launchd starts sshd-keygen-wrapper per connection (an inetd-style
    job; xpcproxy first, as for every launchd job), which runs `sshd -i`;
    OpenSSH 9.8+ sshd runs sshd-session; its privileged monitor forks the
    unprivileged session process, which forks the login shell on a new pty.
    The xpcproxy argument, the exec arguments of sshd-session and the audit
    session are not verified against a capture. Returns the shell,
    interactive, with `.ssh` (the processes to exit on logout)."""
    priv = launchd_job(mac, f"com.openssh.sshd.{mac.uuid()}")
    mac.exec(priv, SSHD_WRAPPER, ["sshd-keygen-wrapper"], env=SSHD_ENV)
    mac.exec(priv, SSHD, ["/usr/sbin/sshd", "-i"])
    mac.exec(priv, SSHD_SESSION, ["/usr/libexec/sshd-session", "-i"])
    mac.wait(0.4)
    user = mac.fork(priv)
    user.ruid, user.euid, user.rgid, user.egid = es.UID, es.UID, es.STAFF, es.STAFF
    user.auid, user.asid = es.UID, 100000 + 1000 + mac.rng.randint(0, 899)
    mac.wait(0.05)
    sh = mac.fork(user)
    mac._ttys += 1
    sh.tty = f"/dev/ttys{mac._ttys % 100:03d}"
    sh.sid = sh.pgid = sh.pid
    port = mac.rng.randint(49152, 65000)
    env = {"USER": es.USER, "LOGNAME": es.USER, "HOME": es.HOME, "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
           "MAIL": f"/var/mail/{es.USER}", "SHELL": "/bin/zsh", "TMPDIR": TMPDIR + "/",
           "SSH_CLIENT": f"{client} {port} 22", "SSH_CONNECTION": f"{client} {port} 192.0.2.10 22",
           "SSH_TTY": sh.tty, "TERM": "xterm-256color", "__CF_USER_TEXT_ENCODING": "0x1F5:0x0:0x0"}
    mac.exec(sh, "zsh", ["-zsh"], env=env, cwd=es.HOME)
    sh.interactive = True
    for f in ("/etc/zprofile", "~/.zprofile", "/etc/zshrc", "~/.zshrc"):
        mac.read_file(sh, f)
    mac.run(sh, "path_helper", ["/usr/libexec/path_helper", "-s"], wait=0.003)
    sh.env["PATH"] = es._LOGIN_PATH
    sh.cwd = mac._p(cwd or "~")
    sh.ssh = (user, priv)
    return sh


def ssh_logout(mac, sh):
    mac.exit(sh)
    for p in sh.ssh:
        mac.wait(0.003)
        mac.exit(p)


def git_clone(mac, sh, url, dest, files, *, key=()):
    """`git clone url` typed in `sh`, from dest's parent: the CLT git (through
    the /usr/bin/git shim), its https transport, the .git directory, and the
    checkout's files written new, in order. `key`: the files (relative to
    dest) whose create, open and close are marked. Returns the git process."""
    dest = mac._p(dest)
    g = mac.spawn(sh, "git", ["git", "clone", url], cwd=dest.rsplit("/", 1)[0])
    r = mac.spawn(g, GIT_REMOTE_HTTPS, ["git-remote-https", "origin", url], fds="pipe")
    mac.wait(mac.rng.uniform(0.8, 2.5))
    mac.exit(r)
    for f in ("HEAD", "config", "description", f"objects/pack/pack-{mac.rng.getrandbits(64):016x}.pack"):
        mac.write_file(g, f"{dest}/.git/{f}", new=True)
    for f in files:
        mac.write_file(g, f"{dest}/{f}", new=True, key=f in key)
    mac.write_file(g, f"{dest}/.git/index", new=True)
    mac.wait(0.05)
    mac.exit(g)
    return g


def claude_version(version, size):
    """Claude Code's native install of `version` (~/.local/bin/claude a link
    to it), for a case that needs a version other than es.CLAUDE's: the same
    signature; `size` made up."""
    return P("claude", f"{es.HOME}/.local/share/claude/versions/{version}", "com.anthropic.claude-code", "Q6L2SF6YDW", es.DEVID,
             dyld=f"{es.HOME}/.local/bin/claude", size=size)


def claude_at(mac, shell, cwd, cfg=None, args=None, *, prog="claude", env_add=None, key=False):
    """es.claude, for a Claude Code started with CLAUDE_CONFIG_DIR=cfg, or (cfg
    None) for another installed version (`prog`, claude_version()). With cfg,
    what es.claude reads and writes under ~/.claude and ~/.claude.json it reads
    and writes under cfg (cfg/settings.json, cfg/.claude.json, cfg/projects/,
    cfg/shell-snapshots/), as the env-vars and directory docs say the
    variable moves them. The keychain item it reads for a non-default config
    directory is not verified: written as the default one."""
    cwd = mac._p(cwd)
    if cfg:
        cfg = mac._p(cfg)
        env_add = dict(env_add or {}, CLAUDE_CONFIG_DIR=cfg)
        settings, state = f"{cfg}/settings.json", f"{cfg}/.claude.json"
    else:
        cfg = f"{es.HOME}/.claude"
        settings, state = f"{cfg}/settings.json", f"{es.HOME}/.claude.json"
    c = mac.spawn(shell, prog, args or ["claude"], cwd=cwd, env_add=env_add, key=key)
    c.env.update(CLAUDECODE="1", CLAUDE_CODE_ENTRYPOINT="cli", GIT_EDITOR="true", COREPACK_ENABLE_AUTO_PIN="0")
    c.env.pop("_", None)
    c.session = f"{cfg}/projects/" + "".join(ch if ch.isalnum() else "-" for ch in cwd) + f"/{mac.uuid().lower()}.jsonl"
    c.snapshot = f"{cfg}/shell-snapshots/snapshot-zsh-{mac.t // 10**6}-{mac.rng.getrandbits(30):06x}.sh"
    for f in (settings, f"{cwd}/.claude/settings.json", f"{cwd}/CLAUDE.md", state):
        if f in mac.files:
            mac.read_file(c, f)
    snap = mac.spawn(c, "zsh", ["/bin/zsh", "-c", "-l", f"SNAPSHOT_FILE={c.snapshot}; source ~/.zshrc < /dev/null; typeset -f > $SNAPSHOT_FILE"],
                     setsid=True)
    mac.read_file(snap, "~/.zshrc")
    mac.write_file(snap, c.snapshot)
    mac.exit(snap)
    mac.run(c, "security", ["security", "find-generic-password", "-a", es.USER, "-w", "-s", "Claude Code-credentials"],
            setsid=True, wait=0.03)
    mac.atomic_write(c, state)
    return c


def tool(mac, agent, prog, args, *, cwd=None):
    """Claude Code runs one command in its tool shell, which forks it: the
    shell and the command's process (exit both when done)."""
    sh = es.agent_shell(mac, agent, " ".join(args), cwd=cwd)
    return sh, mac.spawn(sh, prog, args, fds="null")
