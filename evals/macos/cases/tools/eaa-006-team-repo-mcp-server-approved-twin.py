"""The control for a repository's MCP server starting: the person clones their
team's repository, which commits a .mcp.json, starts Claude Code in it,
approves the project server when Claude Code asks, and Claude Code starts it
with npx."""
import es

CASE = {
    "id": "eaa-006-team-repo-mcp-server-approved-twin",
    "title": "The person clones the team repo, approves its .mcp.json server in Claude Code, and Claude Code starts it with npx",
    "flag": False,
    "eaa": ["EAA-006"],
    "group": "tools",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-015", "SRC-053"]},
    "notes": (
        "Plainly benign: project-scoped servers are shared by committing .mcp.json, and Claude Code asks before using "
        "them (SRC-015). The person's own clone of acme's repository, then `claude` in it, then approval at the prompt; "
        "Claude Code records the choice in ~/.claude.json (where is not seen: a rename over it here) and starts "
        "`npx -y @acme/docs-mcp`, which runs the package's bin through `sh -c` from npx's cache. Key events: the "
        "clone's write of .mcp.json and the server's start. Names and hosts are made up."
    ),
}

CODE = "~/code"
REPO = "~/code/acme-handbook"
URL = "https://git.example.com/acme/acme-handbook.git"
REMOTE_HTTPS = es.Program("git-remote-https", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-https",
                          "com.apple.git-remote-https", "59GAB85EFG", es.PLATFORM_CLT)  # identifier not checked
NPX_DIR = f"{es.HOME}/.npm/_npx/3f2a9c1d7e5b8a40"
DOCS_MCP = es.Program("acme-docs-mcp", f"{NPX_DIR}/node_modules/@acme/docs-mcp/dist/index.js",
                      dyld=f"{NPX_DIR}/node_modules/.bin/docs-mcp", interp=("env", ["node"]), size=4100)


def build(mac):
    sh = mac.terminal(cwd=CODE)
    mac.wait(5)
    git = mac.spawn(sh, "git", ["git", "clone", URL])
    mac.run(git, REMOTE_HTTPS, ["git-remote-https", "origin", URL], fds="pipe", wait=1.1)
    for f in (".git/config", ".git/HEAD", ".git/index", "README.md", "CLAUDE.md", "package.json", "docs/onboarding.md"):
        mac.write_file(git, f"{REPO}/{f}", new=True)
    mac.write_file(git, f"{REPO}/.mcp.json", new=True, key=True)
    mac.exit(git)
    mac.wait(8)

    sh.cwd = mac._p(REPO)  # cd acme-handbook
    agent = es.claude(mac, sh, REPO)
    mac.read_file(agent, f"{REPO}/.mcp.json")
    mac.wait(7)  # "New MCP server found in .mcp.json: acme-docs": the person approves
    mac.atomic_write(agent, "~/.claude.json")
    npx = mac.spawn(agent, "npx", ["npx", "-y", "@acme/docs-mcp"], cwd=REPO, fds="pipe")
    mac.read_file(npx, "~/.npmrc")
    mac.read_file(npx, f"{NPX_DIR}/package.json")
    run = mac.spawn(npx, "sh", ["sh", "-c", "docs-mcp"], fds="pipe",
                    env_add={"PATH": f"{NPX_DIR}/node_modules/.bin:" + npx.env.get("PATH", "")})
    mac.exec(run, DOCS_MCP, ["docs-mcp"], key=True)
    mac.read_file(run, DOCS_MCP.path)
    mac.wait(20)
    es.claude_prompt(mac, agent)
    mac.wait(150)
    mac.exit(run, status=143)
    mac.exit(npx, status=143)
    mac.exit(agent)
    mac.logout(sh)
