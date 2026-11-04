"""The person runs `npm install` after adding a dependency with a native
addon: its install script, `node-gyp rebuild`, runs python's gyp, make and
the compiler and writes only inside its own package directory."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "npm-install-builds-native-addon",
    "title": "The person's npm install runs a dependency's node-gyp rebuild, which compiles inside node_modules",
    "flag": False,
    "eaa": ["EAA-014"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-007", "https://docs.npmjs.com/cli/v11/using-npm/scripts"]},
    "notes": (
        "Plainly benign: npm runs a package's install script, and a package with a binding.gyp and no install script "
        "gets `node-gyp rebuild` by default (npm's scripts documentation); the Phantom Gyp campaign (SRC-007) abused "
        "this same path, which is what this case resembles. Here the addon's build (gyp_main.py, make, c++, ld) "
        "writes only build/ inside node_modules/acme-native, and nothing touches an agent's files or a credential "
        "other than npm's own ~/.npmrc. The person types it at their tty; acme-native is made up. Key events: npm's "
        "read of ~/.npmrc, the install script's sh exec and node-gyp's node exec."
    ),
}

NATIVE = f"{neg.WEB}/node_modules/acme-native"


def build(mac):
    sh = mac.terminal(cwd=neg.WEB)
    mac.wait(5)
    es.vim_edit(mac, sh, f"{neg.WEB}/package.json")  # the new dependency
    mac.wait(3)
    n = mac.spawn(sh, "npm", ["npm", "install"], cwd=neg.WEB)
    mac.read_file(n, "~/.npmrc", key=True)
    mac.read_file(n, f"{neg.WEB}/package.json")
    mac.wait(4)
    for f in ("package.json", "binding.gyp", "src/addon.cc", "index.js"):
        mac.write_file(n, f"{NATIVE}/{f}", new=True)
    s = es.npm_lifecycle(mac, n, NATIVE, "node-gyp rebuild", event="install", version="3.2.0")
    mac.key(mac.last)
    mac.exec(s, "node-gyp", ["node-gyp", "rebuild"], key=True)
    gyp = mac.spawn(s, "python3", ["python3", "/opt/homebrew/lib/node_modules/npm/node_modules/node-gyp/gyp/gyp_main.py",
                                   "binding.gyp", "-f", "make", "-I", "build/config.gypi"], fds="pipe")
    mac.read_file(gyp, f"{NATIVE}/binding.gyp")
    mac.write_file(gyp, f"{NATIVE}/build/Makefile", new=True)
    mac.write_file(gyp, f"{NATIVE}/build/acme_native.target.mk", new=True)
    mac.exit(gyp)
    mk = mac.spawn(s, "make", ["make", "BUILDTYPE=Release", "-C", "build"], cwd=NATIVE, fds="pipe")
    cc = mac.spawn(mk, "c++", ["c++", "-o", "Release/obj.target/acme_native/src/addon.o", "../src/addon.cc", "-O3"], fds="pipe")
    mac.read_file(cc, f"{NATIVE}/src/addon.cc")
    mac.wait(3)
    mac.write_file(cc, f"{NATIVE}/build/Release/obj.target/acme_native/src/addon.o", new=True)
    mac.exit(cc)
    link = mac.spawn(mk, "c++", ["c++", "-bundle", "-o", "Release/acme_native.node", "Release/obj.target/acme_native/src/addon.o"],
                     fds="pipe")
    ld = mac.spawn(link, "ld", ["/Library/Developer/CommandLineTools/usr/bin/ld", "-bundle", "-o", "Release/acme_native.node"],
                   fds="pipe")
    mac.write_file(ld, f"{NATIVE}/build/Release/acme_native.node", new=True)
    mac.exit(ld)
    mac.exit(link)
    mac.exit(mk)
    mac.exit(s)
    mac.write_file(n, f"{neg.WEB}/node_modules/.package-lock.json")
    mac.write_file(n, f"{neg.WEB}/package-lock.json")
    mac.exit(n)
    mac.wait(20)
    mac.logout(sh)
