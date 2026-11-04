"""EAA-004 and EAA-014 as the Hades payload carried it (EAA-C-006, steps 2
and 3): a PyPI package's __init__.py import hook runs when the person's test
run imports it, fetches a Bun runtime into the temp directory and runs the
payload's script on the module search path with it; the payload walks the workspace
tree and plants rule, instruction and hook files for several assistants in
each repository it finds."""
import es

CASE = {
    "id": "eaa-004-python-import-plants-rules-across-workspaces",
    "title": "A Python package's import hook runs Bun from the temp directory, which plants assistant rules and hooks in every repo under ~/code",
    "flag": True,
    "eaa": ["EAA-004", "EAA-014"],
    "group": "control-plane",
    "grounding": {
        "catalog_case": "EAA-C-006",
        "steps": [2, 3],
        "sources": ["SRC-008", "https://www.stepsecurity.io/blog/the-hades-campaign-pypi-packages"],
    },
    "notes": (
        "StepSecurity: an obfuscated import hook in the package's __init__.py runs on import (not at install) unless "
        "a marker file in tempfile.gettempdir() exists; it downloads Bun with urllib to <tmp>/b.zip, extracts it with "
        "`unzip -q -d <tmp>/b <tmp>/b.zip`, makes <tmp>/b/bun executable, runs `<tmp>/b/bun run <dir>/<script>.js` "
        "with subprocess.run (a copy of the environment, no stdio options), for the first sys.path directory holding "
        "the script, and writes the marker after bun returns. gettempdir() is $TMPDIR on macOS (the source's IOCs show "
        "/tmp). The payload walks the workspace tree for 14 tools' rule files and config directories and plants "
        ".cursorrules, .windsurfrules, .cursor/rules/, .github/copilot-instructions.md, .aider.conf.yml, "
        ".claude/settings.json and .vscode/tasks.json, with payload scripts beside them in .claude/ and .vscode/, per "
        "repository. The marker's, the script's and the payload scripts' names are stand-ins (the report's are its "
        "IOCs), and so are the archive and Bun's directory in the events, <tmp>/pyrt.zip and <tmp>/pyrt/bun."
        " Chosen where the source is silent: the import is a `uv run pytest -q` the person typed (the venv's "
        "python, with the terminal for its stdio); the script in site-packages; the rule's name "
        ".cursor/rules/bootstrap.mdc; the tree walked is ~/code; each repository gets the full set. chmod leaves no "
        "event. Key events: bun's exec and the instruction, rule and config writes, not the payload scripts. The same "
        "payload's LaunchAgents and C2 daemon are outside the catalog's steps and not written. The catalog has the "
        "routines present, not seen running on a victim. Package made up."
    ),
}

SDK = "~/code/globex-sdk"
SITE = f"{SDK}/.venv/lib/python3.14/site-packages"
PKG = f"{SITE}/globex_telemetry"
TMP = "/var/folders/7x/k2m9q4z51b3c0000gn/T"  # $TMPDIR as gettempdir() returns it; the kernel's path is under /private
REAL_TMP = "/private" + TMP
_PY = es.PROGRAMS["python"]
VENV_PY = es.Program("venv-python", _PY.path, _PY.signing_id, cs=_PY.cs, dyld=f"{es.HOME}/code/globex-sdk/.venv/bin/python3",
                     then="Python.app")  # as background.py has it
BUN = es.Program("bun", f"{REAL_TMP}/pyrt/bun", "bun", "7FRXF46ZSN", es.DEVID_TOOL, dyld=f"{TMP}/pyrt/bun", size=63072928)
UNZIP = es.Program("unzip", "/usr/bin/unzip", "com.apple.unzip")
REPOS = ["~/code/acme-api", "~/code/acme-web", "~/code/globex-sdk"]
RULES = [".cursorrules", ".windsurfrules", ".cursor/rules/bootstrap.mdc", ".github/copilot-instructions.md",
         ".aider.conf.yml", ".claude/settings.json", ".vscode/tasks.json"]
PAYLOAD = [".claude/hook.js", ".claude/init.mjs", ".vscode/init.mjs"]


def build(mac):
    for f in ("__init__.py", "client.py"):
        mac.exists(f"{PKG}/{f}", days=1)
    mac.exists(f"{SITE}/_main.js", days=1)
    mac.exists(f"{SDK}/uv.lock")
    mac.exists(f"{SDK}/tests/test_client.py")
    sh = mac.terminal(cwd=SDK)
    mac.wait(8)
    uv = mac.spawn(sh, "uv", ["uv", "run", "pytest", "-q"])
    mac.read_file(uv, f"{SDK}/pyproject.toml")
    mac.read_file(uv, f"{SDK}/uv.lock")
    py = mac.spawn(uv, VENV_PY, [f"{mac._p(SDK)}/.venv/bin/python3", "-m", "pytest", "-q"])
    mac.read_file(py, f"{SDK}/pyproject.toml")
    mac.read_file(py, f"{SDK}/tests/test_client.py")
    mac.read_file(py, f"{PKG}/__init__.py")
    mac.wait(0.3)

    # urlretrieve: the zip is written as it downloads
    mac.create(py, f"{REAL_TMP}/pyrt.zip")
    mac.open(py, f"{REAL_TMP}/pyrt.zip", es.FWRITE | es.O_CREAT | es.O_TRUNC | es.O_CLOEXEC)
    mac.wait(2.0)
    mac.close(py, f"{REAL_TMP}/pyrt.zip", modified=True)
    u = mac.spawn(py, UNZIP, ["unzip", "-q", "-d", f"{TMP}/pyrt", f"{TMP}/pyrt.zip"])
    mac.read_file(u, f"{REAL_TMP}/pyrt.zip")
    mac.write_file(u, f"{REAL_TMP}/pyrt/bun", new=True)
    mac.wait(0.4)
    mac.exit(u)

    # subprocess.run([bun, "run", js], env=dict(os.environ)): python waits for it
    bun = mac.spawn(py, BUN, [f"{TMP}/pyrt/bun", "run", f"{mac._p(SITE)}/_main.js"], key=True)
    mac.read_file(bun, f"{SITE}/_main.js")
    mac.wait(1.2)
    for repo in REPOS:
        for f in RULES:
            mac.write_file(bun, f"{repo}/{f}", key=True)
        for f in PAYLOAD:
            mac.write_file(bun, f"{repo}/{f}")
        mac.wait(0.05)
    mac.exit(bun)
    mac.write_file(py, f"{REAL_TMP}/.rt_ok", new=True)

    mac.read_file(py, f"{PKG}/client.py")
    mac.wait(1.5)
    mac.exit(py)
    mac.exit(uv)
    mac.wait(25)
    mac.logout(sh)
