# enrich-ip

Not a provider: the tables that describe an address, which every pack's
plugin reads. A config includes [ingest.yaml](ingest.yaml) beside the packs
it uses, or includes [judge](../judge), which includes it. What every pack's
plugin writes, [the record](../README.md#the-record), and
[the profile](../README.md#the-profile) it reads beside these tables are in
[packs](../README.md).

## ingest.yaml

**The enrichment tables** every pack's plugin reads, from the files
[examples/enrich/setup.sh](/examples/enrich/setup.sh) fetches into
examples/enrich/data. The runtime loads each once when the pipeline starts,
and every plugin on every worker reads that one copy. A missing file is a
warning, and the fields it would give are left out.

| table | type | file | what a pack reads from it |
|---|---|---|---|
| `geo` | `mmdb` | GeoLite2-Country.mmdb | an address's country |
| `asn` | `mmdb` | GeoLite2-ASN.mmdb | its network's number and owner |
| `networks` | `cidr` | networks.csv | its labels: `tor` and `vpn` are flags, and any other label names its hosting provider (`aws`, `gcp`, `cloudflare`, `datacenter`; the first in the file wins) |

No sources, no sinks and no profile: your config adds them, sends each source
to its pack, and sends each pack's plugin on to where its records go: a sink
of yours ([the lake](../README.md#the-lake) in the examples),
[behavior-baseline](../behavior-baseline), the judge. A plugin with no edge
out keeps nothing: its records go nowhere.
