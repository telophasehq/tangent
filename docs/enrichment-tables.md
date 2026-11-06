# Enrichment tables

Reference data every plugin can read, loaded once for the process: an IP
database, a list of networks, a company profile. Each table has a name, a
`type` and a `path`, relative to the file that names it:

```yaml
enrichment_tables:
  geo:      { type: mmdb, path: data/GeoLite2-Country.mmdb }
  asn:      { type: mmdb, path: data/GeoLite2-ASN.mmdb }
  networks: { type: cidr, path: data/networks.csv }
  profile:  { type: document, path: profile/profile.yaml, required: true }
```

A plugin looks a key up in a table by name through the `enrichment`
interface ([assets/wit/processor.wit](../assets/wit/processor.wit)) and gets
JSON back, or nothing:

```rust
use tangent::logs::enrichment::lookup;

let geo = lookup("geo", ip);              // Some(r#"{"country":{"iso_code":"FI",...},...}"#)
let nets = lookup("networks", ip);        // Some(r#"["aws","datacenter"]"#), None when in none
let org = lookup("profile", "org");       // Some(r#""acme""#)
```

Every address in a record is a call: a plugin that knows three fields
holding addresses looks up all three.

| `type` | file | key | answer |
|---|---|---|---|
| `mmdb` | a MaxMind DB | an address | its record, as JSON |
| `cidr` | `prefix,label` lines, IPv4 and IPv6; `#` comments; a bare address is its own /32 or /128; an IPv6 prefix holds no IPv4 address, however wide (`::/0`), and `::ffff:192.0.2.0/120` is the IPv4 prefix it maps | an address | the label of every prefix that holds it, as a list: each label once, in the order the labels first appear in the file, so a file lists first the labels that should win |
| `document` | YAML, or JSON when the name ends `.json` | a dotted path, `[n]` or `.n` for an item of a list, `""` for all of it | the value there; a key may hold dots |

An IPv4-mapped IPv6 address (`::ffff:203.0.113.7`) is looked up as the IPv4
address. A key that is not an address, a path that is not there, and a null
all answer nothing.

## Loading

`tangent run` reads every table when it builds the DAG, before the first
record, into one copy every worker and every plugin shares: two plugins, or
two packs, that read GeoLite2 hold it in memory once, and a lookup is a
search in that memory, with no file read and no lock: tens of nanoseconds for
a `cidr` table, about a microsecond for a GeoLite2 Country record. A file
named by two tables, as the same type, is read once.

- A file that is missing is a warning, once, and its table answers nothing,
  as a missing file did for a plugin that read its own.
- A file that is there but cannot be read as its type stops `tangent run`,
  naming the table and the file.
- A table with `required: true` must have its file: without it
  `tangent run`, `tangent plugin test` and `tangent config` stop, naming
  both. Mark a table so when nothing is right without it, as for a company
  profile.
- A table no config defines answers nothing too, with a warning the first
  time a plugin asks for it.
- A refreshed file is picked up at the next start.

## In tests

`tangent plugin test` loads the tables its config declares, so a fixture runs
against the same data. A test may name tables of its own
(`tests: [{input, expected, enrichment_tables: {...}}]`), which take the place
of the config's of the same name for that test: the data its expected output
was written against, so a pack's fixtures pass in any config that includes
the pack.

## Where the data comes from

The runtime knows these three formats and nothing about what is in them. A
provider's own format is converted beforehand:
[examples/enrich/setup.sh](../examples/enrich/setup.sh) fetches GeoLite2, the
AWS, GCP and Cloudflare ranges, X4BNet's datacenter and VPN lists and the Tor
exit list, and writes the ranges into `data/networks.csv` labelled `aws`,
`gcp`, `cloudflare`, `datacenter`, `vpn` and `tor`, in that order
(`./setup.sh networks` writes it again from the files already there).

## Plugins built before the interface

A plugin built before the interface existed does not import it and runs as
before. To call it from an existing plugin, compile the plugin again:
`tangent plugin compile` writes this tangent's WIT ([assets/wit](../assets/wit))
into each plugin's `.tangent/wit/` before building it, so a plugin builds
against the interface of the tangent that compiles it, and keeps no copy of
its own (`--wit <dir>` builds against another). A plugin that imports it
needs a runtime that provides it.
