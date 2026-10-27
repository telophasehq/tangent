# enrich

What is known about the IP addresses in a record, added to the record, from
files on disk. Nothing leaves the machine.

```
{"event": "login", "sourceIPAddress": "65.108.22.91"}
  ──▶ enrich ──▶
{"event": "login", "sourceIPAddress": "65.108.22.91",
 "_ip": {"sourceIPAddress": {"ip": "65.108.22.91", "country": "FI", "asn": 24940,
                             "org": "Hetzner Online GmbH", "tor": false, "ours": false,
                             "private": false}}}
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
them; MaxMind publishes on Tuesdays and Fridays.

| file | source | terms |
|---|---|---|
| `GeoLite2-Country.mmdb`, `GeoLite2-ASN.mmdb` | [MaxMind GeoLite2](https://www.maxmind.com/en/geolite2/signup) with `MAXMIND_LICENSE_KEY` set; otherwise the [P3TERX mirror](https://github.com/P3TERX/GeoLite.mmdb) of the same files | CC BY-SA 4.0; this product includes GeoLite2 data created by MaxMind |
| `aws-ip-ranges.json` | [AWS](https://ip-ranges.amazonaws.com/ip-ranges.json) | public |
| `gcp-ip-ranges.json` | [Google Cloud](https://www.gstatic.com/ipranges/cloud.json) | public |
| `cloudflare-ips-v4.txt`, `-v6.txt` | [Cloudflare](https://www.cloudflare.com/ips/) | public |
| `tor-exits.txt` | [Tor Project](https://check.torproject.org/torbulkexitlist) | public |
| `datacenter-ips.txt`, `vpn-ips.txt` | [X4BNet lists_vpn](https://github.com/X4BNet/lists_vpn), broad datacenter and VPN ranges | MIT |

Drop in `GeoLite2-City.mmdb` instead of Country to get the same fields from
the larger database. A file that is missing leaves its fields out; nothing
fails.

Each worker instance reads the files into its own memory: about 15 MB for
Country, ASN and the ranges, 60 MB with City. Picking up a refreshed file
takes a restart.

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
               "org": "Stiftung Erneuerbare Freiheit", "tor": true, "ours": false, "private": false}}, ...}
```

## Run

```bash
./setup.sh
tangent plugin compile --config tangent.yaml
tangent plugin test --config tangent.yaml      # tests/input.json → tests/expected.json
tangent run --config tangent.yaml
```
