# Prebuilt plugins

A plugin is a WASM component, compiled once and run on every machine, so only
a pack's author needs the toolchain. Each release of tangent is to carry every
pack's plugin as a release asset, `<plugin>.component.wasm`, built by
[packs/dist.sh](../packs/dist.sh) in the release workflow, and a pack's
`ingest.yaml` says so:

```yaml
plugins:
  aws:
    module_type: rust
    path: .                                          # the source, for `tangent plugin compile`
    prebuilt: { repo: EthanBlackburn/tangent, workflow: .github/workflows/attest-packs.yml }
```

Not yet: v0.1.9, the latest release, came before the packs and carries none
of their plugins, so until the next release `tangent plugin fetch` has
nothing to fetch for them, and `tangent plugin compile` builds them from
source instead ([packs](../packs/README.md#using-packs)).

`tangent plugin fetch --config tangent.yaml` downloads each `prebuilt`
plugin from `repo`'s release of this tangent's version (`release:` names
another; `asset:` another file name; `host:` a GitHub Enterprise host) and
precompiles it into `runtime.plugins_path`, where `tangent plugin compile`
puts it and `tangent run` reads it. A plugin with `prebuilt` and no `path` is
fetched only.

| key | default |
|---|---|
| `repo` | the GitHub repository, `owner/name`, whose release has the component and whose workflow must have built it |
| `release` | `v` and this tangent's version |
| `asset` | `<plugin>.component.wasm` |
| `workflow` | any workflow of `repo`; set, the one that must have signed it |
| `sha256` | none; set, the component's digest, hex, pinned beyond its attestation |
| `host` | `github.com` |

## The check

A prebuilt plugin is code off the network that reads every record, so before
one is kept its build is checked: the release workflow attests each
component, on its own, with
[GitHub artifact attestations](https://docs.github.com/en/actions/concepts/security/artifact-attestations)
([.github/workflows/attest-packs.yml](../.github/workflows/attest-packs.yml)),
a Sigstore signature naming the component's file name and digest, the
repository, the workflow and the commit that built it, with the attestation
bundle uploaded beside the component as `<asset>.sigstore.json`. `fetch` has
`gh attestation verify`, GitHub's own verifier, check it: signed for `repo`
from the release's tag (`--source-ref refs/tags/<release>`), by `workflow`
when named; and then that the statement names the file as the asset asked
for, not only by its digest, so one attested component of a release cannot
be served under another's name. With the bundle on the release nothing asks
GitHub's API, so `gh` need not be logged in; a release without the bundle
falls back to the API, which needs `gh auth login`. A plugin that does not
verify is not kept, and there is no flag to skip the check. `sha256:` pins
the component's digest as well, for a lockfile's certainty. The command
`fetch` ran is printed, to run again by hand:

```bash
gh attestation verify plugins/aws.component.wasm --repo EthanBlackburn/tangent --source-ref refs/tags/v0.1.10 \
  --bundle plugins/aws.component.wasm.sigstore.json --signer-workflow EthanBlackburn/tangent/.github/workflows/attest-packs.yml
```

An attestation says who built a plugin and from what. It does not limit what
the plugin can do once it runs: [plugins](plugins.md#what-a-plugin-can-reach)
says what it can reach.

## Publishing your own

A pack of your own is published the same way: a workflow that builds the
component with `cargo component build --release --target wasm32-wasip2`,
attests it with `actions/attest-build-provenance` under the file name it will
have on the release (the step's `bundle-path` output is the bundle), and
uploads both to a release; its users write
`prebuilt: { repo: you/packs, release: <tag>, workflow: .github/workflows/release.yml }`.
