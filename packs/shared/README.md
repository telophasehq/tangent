# shared

Not packs: the Rust libraries every pack's plugin links by path
(`normkit = { path = "../shared/normkit" }`), so each plugin is a few
provider-specific rules and nothing else. None is a plugin, and none reads a
file.

| library | what it is |
|---|---|
| [normkit](normkit) | `Norm`, the one definition of `_norm`; the reserved keys a raw record loses on the way in; the scrubs and trim helpers (per-event ids, URL signatures, `pick`, `prune`, `shrink`) |
| [enrichkit](enrichkit) | an address described from the runtime's enrichment tables `geo`, `asn` and `networks`, and the profile's `networks`: country, network and owner, hosting, Tor, VPN, one of yours, private |
| [factskit](factskit) | the profile's facts (principal, usual country, cloud, destination and target, keys, watch, notes), the own-org fold, and the record written out with `_norm` and `_facts` |

[packs/core](../core#writing-a-packs-plugin) has the contract a plugin keeps
with them. Each has its own tests: `make -C packs test-shared`.
