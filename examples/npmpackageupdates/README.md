# npmpackageupdates

Watch npm for new versions of the packages you care about, and flag a version that runs code when it
is installed but was not published from CI. That is how npm worms spread: a stolen token publishes a
new version that adds a `postinstall` script, from the attacker's machine rather than the project's
release workflow.

```
npm_registry (source)  ->  npmpackageupdates (Go plugin, main.go)  ->  alerts.ndjson (file sink)
```

* The `npm_registry` source polls `https://registry.npmjs.org/<package>` every `interval_secs` and
  sends each version published in the last 500 minutes as
  `{"kind": "npm_package_version", "npm": <that version's registry manifest>}`.
* The plugin writes one alert per version: who published it, whether it came from CI (npm trusted
  publishing or a provenance attestation), and its install script, if any. `triggered` is true when the
  version has a `preinstall`, `install` or `postinstall` script and did not come from CI.

No npm account or token is needed for public packages.

## Requirements

* `tangent` on your `PATH`
* Go 1.26 or later, a [TinyGo](https://tinygo.org/getting-started/install/) that supports it, and
  `wasm-tools`. `./setup.sh` installs TinyGo 0.43.0 and `wasm-tools` when they are missing, and
  changes nothing when both are already installed. Installing TinyGo also appends `PATH` and
  `TINYGOROOT` lines to your shell profiles (`~/.zshrc`, `~/.zprofile`, `~/.bashrc`,
  `~/.bash_profile`, `~/.profile`, whichever exist): open a new shell before compiling.
* Network access to `registry.npmjs.org`

If the compile fails with:

* `requires go version 1.19 through 1.25, got go1.27`: your TinyGo is older than your Go. TinyGo
  0.43 builds this with Go 1.27; `setup.sh` leaves a TinyGo already on `PATH` alone.
* `could not autodetect root directory` or `open .../tinygo/<version>/src/...: no such file or
  directory`: `TINYGOROOT` is unset or names another TinyGo than the `tinygo` on `PATH`. Open a new
  shell, or set it, e.g. `export TINYGOROOT=~/.local/opt/tinygo/0.43.0`.

## Build and test

```bash
./setup.sh
tangent plugin compile --config tangent.yaml
tangent plugin test --config tangent.yaml
```

`compile` writes `plugins/npmpackageupdates.cwasm`. `test` runs `tests/input.json` (three versions shaped
like the source's records, and one record from another source) through it and ends with
`✅ test passed: output matches expected`. The warning before it, `a record from this source matched
none of its targets and was dropped`, is the fourth record: the plugin's selector only takes
`kind: npm_package_version`.

## Run

```bash
tangent run --config tangent.yaml
```

The first poll is immediate. In another terminal:

```bash
tail -f alerts.ndjson
```

```json
{"triggered":false,"package_name":"@anthropic-ai/claude-code","version":"2.1.319","shasum":"f7a75c88de713be16d7fa7b367761db779a71e85","publisher":"GitHub Actions","provenance":true,"install_script":"postinstall: node install.cjs"}
{"triggered":false,"package_name":"@cloudflare/workers-types","version":"5.20261106.1","shasum":"c7389a75c70af98f962227accecd737dd01f1f1a","publisher":"GitHub Actions","provenance":true,"install_script":""}
```

Stop it with Ctrl-C. Two things to know about the source:

* It only sends versions published in the last 500 minutes. If none of the packages in `tangent.yaml`
  released in that time, `alerts.ndjson` stays empty until one does. The three packages it watches
  release most days.
* It does not remember what it sent: every poll sends every version still inside the 500 minutes again,
  so the same alert repeats once per `interval_secs` (300 here) until the version ages out.

## Watch your own packages

In `tangent.yaml`, list the packages:

```yaml
sources:
  npm_registry:
    type: npm_registry
    packages: ["your-package", "@your-scope/other-package"]
    interval_secs: 300
```

or name an npm org or user instead of `packages:` and every package it publishes is polled (the list
is fetched on every poll):

```yaml
    orgs: ["your-org"]
```

The source sends `token:` as a bearer token on every request. Only add it if you need one, and only
with the variable set: `token: "${NPM_TOKEN}"` with `NPM_TOKEN` unset is an empty token, the registry
answers the org listing with `401 Unauthorized`, and the source stops for the rest of the run.

Check what tangent will run with `tangent config --config tangent.yaml` (secrets left out).

## Using the Makefile

```bash
make test   # setup, compile, test
make run    # setup, compile, run
```

`Agents.md` describes the plugin contract for anyone (or any model) changing `main.go`.
