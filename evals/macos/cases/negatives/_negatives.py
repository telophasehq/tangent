"""What the negatives share. Not a case: gen.py skips files that start with
`_`. A case loads it with

    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import _negatives as neg

Every value is synthetic."""
import es

HOME = es.HOME
WEB, API, SDK = f"{HOME}/code/acme-web", f"{HOME}/code/acme-api", f"{HOME}/code/globex-sdk"
HUNTING = "https://github.com/0x4D31/endpoint-ai-agent-abuse/blob/main/hunting.md"
CATALOG = "https://github.com/0x4D31/endpoint-ai-agent-abuse/blob/main/index.md"

# Git's own helper for subcommands it runs (index-pack): as codesign(1) showed it on a macOS 26.3 Mac
GIT_CORE = es.Program("git-core", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git", "com.apple.git", "59GAB85EFG",
                      es.PLATFORM_CLT)


def job(mac, label, prog, args):
    """launchd starts a job of dev's: it forks, the child execs xpcproxy,
    which execs the program; no tty, a session of its own (as background.py
    starts mdworker and ssh-agent)."""
    c = mac.fork(mac.launchd)
    c.ruid = c.euid = es.UID
    c.rgid = c.egid = es.STAFF
    c.asid, c.auid = es.ASID, es.UID
    c.sid = c.pgid = c.pid
    c.tty, c.cwd, c.responsible = None, "/", c
    c.env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": HOME, "USER": es.USER, "LOGNAME": es.USER, "SHELL": "/bin/zsh",
             "TMPDIR": es._GUI_ENV["TMPDIR"], "XPC_SERVICE_NAME": label, "XPC_FLAGS": "0x0"}
    mac.exec(c, "xpcproxy", ["xpcproxy", label])
    return c, mac.exec(c, prog, args)


def session_end(mac, c, sh):
    """Claude Code's reply, its config saved, /exit, the tab closed."""
    mac.write_file(c, c.session, append=True)
    mac.atomic_write(c, "~/.claude.json")
    mac.wait(mac.rng.uniform(5, 20))
    mac.exit(c)
    mac.wait(3)
    mac.logout(sh)
