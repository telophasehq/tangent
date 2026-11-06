# enrich

What is known about the IP addresses in a record, added to the record, from
files on disk. Nothing leaves the machine.

```
{"event": "login", "sourceIPAddress": "65.108.22.91"}
  ──▶ enrich ──▶
{"event": "login", "sourceIPAddress": "65.108.22.91",
 "_ip": {"sourceIPAddress": {"ip": "65.108.22.91", "country": "FI", "asn": 24940,
                             "org": "Hetzner Online GmbH", "hosting": "datacenter",
                             "tor": false, "vpn": false, "ours": false, "private": false}}}
```

Per address: country, ASN and organization (MaxMind GeoLite2), whether it is in
a cloud provider's published ranges (`hosting: aws | gcp | cloudflare`, or
`datacenter` from a broad public list), whether it is a Tor exit or in a VPN
provider's range, whether it is one of the org's own (`ours`, from config), and
whether it is private. A model or a rule downstream reads "Hetzner", "Tor
exit", "one of ours" instead of a bare number.

## Data

`./setup.sh` puts the files under `data/`, which `tangent.yaml` hands to the
plugin read-only as `/data` (`plugins.enrich.data`). `./setup.sh data` refreshes
them; MaxMind publishes on Tuesdays and Fridays. The packs read the same
files from here: [core's ingest.yaml](/packs/core/ingest.yaml) names them as
its enrichment tables, so run `./setup.sh data` once before a pack's pipeline.

| file | source | terms |
|---|---|---|
| `GeoLite2-Country.mmdb`, `GeoLite2-ASN.mmdb` | [MaxMind GeoLite2](https://www.maxmind.com/en/geolite2/signup) with `MAXMIND_LICENSE_KEY` set; otherwise the [P3TERX mirror](https://github.com/P3TERX/GeoLite.mmdb) of the same files | CC BY-SA 4.0; this product includes GeoLite2 data created by MaxMind |
| `aws-ip-ranges.json` | [AWS](https://ip-ranges.amazonaws.com/ip-ranges.json) | public |
| `gcp-ip-ranges.json` | [Google Cloud](https://www.gstatic.com/ipranges/cloud.json) | public |
| `cloudflare-ips-v4.txt`, `-v6.txt` | [Cloudflare](https://www.cloudflare.com/ips/) | public |
| `tor-exits.txt` | [Tor Project](https://check.torproject.org/torbulkexitlist) | public |
| `datacenter-ips.txt`, `vpn-ips.txt` | [X4BNet lists_vpn](https://github.com/X4BNet/lists_vpn), broad datacenter and VPN ranges | MIT |
| `networks.csv` | every range above as `prefix,label` (`aws`, `gcp`, `cloudflare`, `datacenter`, `vpn`, `tor`), written by `setup.sh` for the runtime's `cidr` [enrichment tables](/docs/enrichment-tables.md); this plugin does not read it | as above |

Drop in `GeoLite2-City.mmdb` instead of Country to get the same fields from
the larger database. A file that is missing leaves its fields out; nothing
fails.

Each worker instance reads the files into its own memory: about 15 MB for
Country, ASN and the ranges, 60 MB with City. Picking up a refreshed file
takes a restart. The runtime's [enrichment tables](/docs/enrichment-tables.md)
serve the same files to every plugin from one copy per process
(`GeoLite2-*.mmdb` as `mmdb` tables, `networks.csv` as a `cidr` table).

## Config

```yaml
plugins:
  enrich:
    module_type: rust
    path: .
    data: data/
    config:
      fields: [sourceIPAddress, client.ipAddress, actor_ip, ip]   # record paths that hold an address
      ours: [203.0.113.0/24, 198.51.100.7]                       # office, VPN, CI egress
      output: _ip                                                # default
```

`fields` default to the common names (`sourceIPAddress`, `client.ipAddress`,
`actor_ip`, `ipAddress`, `ip`, `remote_addr`, `src_ip`, `client_ip`). A field
may hold `a.b.c.d:port`, `[v6]:port`, or a comma-separated list; the first
address counts. Every record passes through; those with an address in one of
the fields gain `<output>.<field>`. Addresses on the Tor exit list come back
with `tor: true`:

```
{"event": "login", "sourceIPAddress": "185.220.101.4"}
  ──▶ {"_ip": {"sourceIPAddress": {"ip": "185.220.101.4", "country": "DE", "asn": 60729,
               "org": "Stiftung Erneuerbare Freiheit", "hosting": "datacenter",
               "tor": true, "vpn": true, "ours": false, "private": false}}, ...}
```

## Run

```bash
./setup.sh                                     # the Rust WASM toolchain, and data/
tangent plugin compile --config tangent.yaml
tangent plugin test --config tangent.yaml      # tests/input.json → tests/expected.json
tangent run --config tangent.yaml              # listens on :9000, writes out.ndjson
```

In a second terminal, send records, one JSON object per line (the `tcp`
source reads NDJSON, so the fixture's array goes through `jq -c`; `-w 1`
lets `nc` hang up once its input ends, which Linux's netcat does not do on
its own):

```bash
jq -c '.[]' tests/input.json | nc -w 1 127.0.0.1 9000
echo '{"event": "login", "sourceIPAddress": "185.220.101.4"}' | nc -w 1 127.0.0.1 9000
cat out.ndjson
```

Records come out in no fixed order: the workers run in parallel. `make test`
and `make run` do the same steps, each after `./setup.sh`, which downloads
only what has changed.

`tests/expected.json` holds the answers in the data when it was written.
MaxMind and the providers move an address now and then; after a refresh, such
a move shows up as a difference in `tangent plugin test`, and the expected
line is updated to the new answer.
