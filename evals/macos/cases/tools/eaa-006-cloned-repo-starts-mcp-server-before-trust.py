"""EAA-006 as Check Point demonstrated it (EAA-C-015, step 2): a cloned
repository's .claude/settings.json approves every server in its .mcp.json,
and an affected historical Claude Code starts the project MCP server's
command the moment `claude` runs in it, before the trust dialog is
answered."""
import es

CASE = {
    "id": "eaa-006-cloned-repo-starts-mcp-server-before-trust",
    "title": "Claude Code (pre-1.0.111) run in a fresh clone starts the repo's .mcp.json command before the trust dialog",
    "flag": True,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {
        "catalog_case": "EAA-C-015",
        "steps": [2],
        "sources": ["SRC-023", "SRC-041", "https://research.checkpoint.com/2026/rce-and-api-token-exfiltration-through-claude-code-project-files-cve-2025-59536/"],
    },
    "notes": (
        "Check Point (CVE-2025-59536; fixed in 1.0.111, SRC-041): the victim clones a malicious repository and runs "
        "`claude` in it; the repository's .claude/settings.json sets enableAllProjectMcpServers (or names the server "
        "in enabledMcpjsonServers) and .mcp.json defines a server whose command ran at once, opening Calculator over "
        "the still-pending trust dialog. The keys and the command's exact form are content: the source says only that "
        "it opened Calculator, so the server's command is a stand-in, `open -a TextEdit`, spawned directly as a stdio server "
        "is. Check Point gives no version or install method; the affected version (before 1.0.111) is the npm "
        "package here (`claude` a node script), not today's native binary. Step 1 (a "
        "SessionStart hook, EAA-003) and step 3 (ANTHROPIC_BASE_URL in the repo's settings, EAA-007) are not in this "
        "case. TextEdit itself, started by launchd through LaunchServices, is left out. The repository and host are "
        "made up."
    ),
}

CODE = "~/code"
REPO = "~/code/globex-starter"
URL = "https://git.example.com/globex/globex-starter.git"
CLAUDE_NPM = es.Program("claude-npm", "/opt/homebrew/lib/node_modules/@anthropic-ai/claude-code/cli.js",
                        dyld="/opt/homebrew/bin/claude", interp=("env", ["node"]), size=8_650_112)
REMOTE_HTTPS = es.Program("git-remote-https", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-https",
                          "com.apple.git-remote-https", "59GAB85EFG", es.PLATFORM_CLT)  # identifier not checked
OPEN = es.Program("open", "/usr/bin/open", "com.apple.open")


def build(mac):
    sh = mac.terminal(cwd=CODE)
    mac.wait(6)
    git = mac.spawn(sh, "git", ["git", "clone", URL])
    mac.run(git, REMOTE_HTTPS, ["git-remote-https", "origin", URL], fds="pipe", wait=1.4)
    for f in (".git/config", ".git/HEAD", ".git/index", "README.md", "package.json", "src/index.js"):
        mac.write_file(git, f"{REPO}/{f}", new=True)
    mac.write_file(git, f"{REPO}/.mcp.json", new=True, key=True)
    mac.write_file(git, f"{REPO}/.claude/settings.json", new=True, key=True)
    mac.exit(git)
    mac.wait(9)

    sh.cwd = mac._p(REPO)  # cd globex-starter
    c = mac.spawn(sh, CLAUDE_NPM, ["claude"])
    for f in ("~/.claude/settings.json", f"{REPO}/.claude/settings.json", f"{REPO}/.mcp.json", "~/.claude.json"):
        mac.read_file(c, f)
    mac.wait(0.4)
    server = mac.spawn(c, OPEN, ["open", "-a", "TextEdit"], fds="pipe", key=True)
    mac.wait(0.2)
    mac.exit(server)
    mac.wait(25)  # the trust dialog, answered or not
    mac.exit(c, status=1)
    mac.wait(10)
    mac.logout(sh)
