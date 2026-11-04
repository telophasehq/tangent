"""EAA-004 as Mitiga's incident report has it (EAA-C-034, steps 1 and 5): a
take-home assessment's repository carries poisoned Cursor project rules; the
candidate clones it, opens it in Cursor and asks the agent how to run it; the
agent follows the rules' "environment validation" workflow and reads the
cloud and Kubernetes credentials through its terminal."""
import es

CASE = {
    "id": "eaa-004-cursor-rule-steers-agent-to-credentials",
    "title": "Following a cloned repo's Cursor rules, Cursor's agent reads ~/.aws/credentials and ~/.kube/config in its terminal",
    "flag": True,
    "eaa": ["EAA-004", "EAA-015"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-034",
        "steps": [1, 5],
        "sources": ["SRC-077", "https://www.mitiga.io/blog/poisoned-coding-test-ai-agent-attack"],
    },
    "notes": (
        "Mitiga: Cursor with terminal access, MCP tools and auto-run already on; the person asks how to run the project "
        "(14:02:11) and the agent loads .cursor/rules, README.md and .cursor/mcp.json, then runs `cat ~/.aws/credentials` "
        "(+29 s), `aws sts get-caller-identity` (+42 s), `cat ~/.kube/config` (+58 s), `terraform workspace list && "
        "terraform state list` (+69 s) and `grep -rEi 'secret|token|key' .` (+90 s), and sends it all through an MCP "
        "tool call (+111 s). The commands and timings are the report's; the grep reads every file in the repository, "
        "and what Terraform reads is not given. Rule file names and how Cursor runs agent commands are not given, so: "
        ".cursor/rules/getting-started.mdc; each command line as `zsh -c` under Cursor's extension host (not verified), which "
        "execs a lone command in place and, for the && pair, forks the first and execs the last, as the tools case "
        "eaa-006-cursor-loads-repo-mcp-and-reads-credentials has it; Homebrew's aws CLI; terraform as that case defines "
        "it (Homebrew core's last formula, 1.5.7, linker-signed ad hoc; a hashicorp/tap install would be HashiCorp's "
        "own signed build instead). The MCP server and its call (steps 3, 4 and the transmission) are not written: no "
        "process is named and the send is network. Step 2's README comments are content. The clone's rule write is "
        "ordinary on its own; it is keyed as the delivery."
    ),
}

URL = "https://git.example.com/globex-hiring/take-home-ledger.git"
REPO = "~/code/take-home-ledger"
CLT = "/Library/Developer/CommandLineTools/usr/bin/git"
REMOTE = es.Program("git-remote-https", "/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-http",
                    "com.apple.git-remote-http", "59GAB85EFG", es.PLATFORM_CLT,
                    dyld="/Library/Developer/CommandLineTools/usr/libexec/git-core/git-remote-https")
TERRAFORM = es.Program("terraform", "/opt/homebrew/Cellar/terraform/1.5.7/bin/terraform", es._adhoc("terraform"),
                       cs=es.ADHOC, dyld="/opt/homebrew/bin/terraform", size=81_234_560)  # as the tools case has it


def checkout(mac, git, path, key=False):
    """git writes a working-tree file: created, opened exclusive, written, closed."""
    out = [mac.create(git, path), mac.open(git, path, es.FWRITE | es.O_CREAT | es.O_EXCL)]
    mac.wait(0.0003)
    out.append(mac.close(git, path, modified=True))
    return mac.key(out) if key else out


def clone(mac, sh, url, dest, files, keys=()):
    g = mac.spawn(sh, "git", ["git", "clone", url])
    mac.write_file(g, f"{dest}/.git/config.lock", new=True)
    mac.rename(g, f"{dest}/.git/config.lock", f"{dest}/.git/config")
    mac.write_file(g, f"{dest}/.git/HEAD", new=True)
    r = mac.spawn(g, REMOTE, ["git-remote-https", "origin", url], fds="pipe")
    mac.wait(0.9)
    pack = f"{dest}/.git/objects/pack/pack-{mac.rng.getrandbits(160):040x}"
    ip = mac.spawn(g, "clt-git", [CLT, "index-pack", "--stdin", "--fix-thin", "--check-self-contained-and-connected"], fds="pipe")
    mac.write_file(ip, pack + ".pack", new=True)
    mac.write_file(ip, pack + ".idx", new=True)
    mac.exit(ip)
    mac.exit(r)
    for f in files:
        checkout(mac, g, f"{dest}/{f}", key=f in keys)
    mac.write_file(g, f"{dest}/.git/index.lock", new=True)
    mac.rename(g, f"{dest}/.git/index.lock", f"{dest}/.git/index")
    mac.wait(0.05)
    mac.exit(g)


def command(mac, host, line, prog, argv, reads=(), keyed=(), wait=0.2):
    """The agent runs one command line, `zsh -c <line>`, which execs `argv` in place; it reads `reads`, and
    `keyed` of them are key events. The shell's exec and the program's are key events."""
    start = len(mac.log)
    sh = es.agent_shell(mac, host, line, cwd=REPO)
    mac.key([e for e, _, _ in mac.log[start:] if "exec" in e["event"]])
    mac.exec(sh, prog, argv, key=True)
    for f in reads:
        mac.read_file(sh, f, key=f in keyed)
    mac.wait(wait)
    mac.exit(sh)


def build(mac):
    mac.exists("~/.kube/config", days=60)
    sh = mac.terminal(cwd="~/code")
    mac.wait(6)
    clone(mac, sh, URL, REPO, ["README.md", "CLAUDE.md", "package.json", "src/ledger.ts", "src/api.ts",
                               ".cursor/rules/getting-started.mdc", ".cursor/mcp.json"], keys={".cursor/rules/getting-started.mdc"})
    mac.wait(15)
    cursor = mac.app("Cursor")
    host = mac.spawn(cursor, "Cursor Helper (Plugin)", [es.PROGRAMS["Cursor Helper (Plugin)"].path, "--type=utility",
                     "--utility-sub-type=node.mojom.NodeService", "--lang=en-US", "--service-sandbox-type=none"], cwd="/")
    mac.wait(20)
    t0 = mac.t  # 14:02:11: "how do I run this project?"

    def at(seconds):  # the report's timeline, from the prompt
        mac.t = max(mac.t, t0 + int(seconds * 10**9))

    for f in (".cursor/rules/getting-started.mdc", "README.md", ".cursor/mcp.json"):
        mac.read_file(host, f"{REPO}/{f}")
    at(29)
    command(mac, host, "cat ~/.aws/credentials", "cat", ["cat", mac._p("~/.aws/credentials")],
            reads=["~/.aws/credentials"], keyed=["~/.aws/credentials"])
    at(42)
    command(mac, host, "aws sts get-caller-identity", "aws", ["aws", "sts", "get-caller-identity"],
            reads=["~/.aws/config", "~/.aws/credentials"], keyed=["~/.aws/config", "~/.aws/credentials"], wait=1.2)
    at(58)
    command(mac, host, "cat ~/.kube/config", "cat", ["cat", mac._p("~/.kube/config")],
            reads=["~/.kube/config"], keyed=["~/.kube/config"])
    at(69)  # one command line; zsh forks the first, execs the last in place
    start = len(mac.log)
    tsh = es.agent_shell(mac, host, "terraform workspace list && terraform state list", cwd=REPO)
    mac.key([e for e, _, _ in mac.log[start:] if "exec" in e["event"]])
    mac.run(tsh, TERRAFORM, ["terraform", "workspace", "list"], wait=0.8, fds="pipe", key=True)
    mac.exec(tsh, TERRAFORM, ["terraform", "state", "list"], key=True)
    mac.wait(1.1)
    mac.exit(tsh)
    at(90)
    repo = mac._p(REPO) + "/"
    command(mac, host, "grep -rEi 'secret|token|key' .", "grep", ["grep", "-rEi", "secret|token|key", "."],
            reads=sorted(f for f in mac.files if f.startswith(repo)))

    at(111)  # project-setup.env_check: stdio to the server, then the network
    mac.wait(40)
    mac.exit(host)
    mac.logout(sh)
