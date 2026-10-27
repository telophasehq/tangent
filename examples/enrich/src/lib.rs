//! Decorates records with what is known about the IP addresses in them, from
//! files under `/data` and nothing else: no lookup leaves the machine.
//!
//! `setup.sh` fills `data/` (mounted read-only at `/data` by `plugins.enrich.data`):
//!   GeoLite2-Country.mmdb, GeoLite2-ASN.mmdb   MaxMind GeoLite2 (CC BY-SA)
//!   aws-ip-ranges.json, gcp-ip-ranges.json,    the providers' published ranges
//!   cloudflare-ips-v4.txt, cloudflare-ips-v6.txt
//!   tor-exits.txt                              the Tor Project's exit list
//!   datacenter-ips.txt, vpn-ips.txt            X4BNet's broad datacenter and VPN range lists
//! A file that is missing just leaves its fields out.
//!
//! plugins.enrich.config:
//!   fields: [sourceIPAddress, client.ipAddress, ...]   # record paths that hold an IP
//!   ours: [203.0.113.0/24, 198.51.100.7]              # the org's own addresses
//!   output: _ip                                        # where the facts land
//!
//! For each configured field that holds an address, the record gains
//! `<output>.<field>`: `{ip, country, asn, org, hosting, tor, vpn, ours, private}`.
//! Every record passes through; this plugin only adds.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::OnceLock;

use exports::tangent::logs::mapper::{Guest, Meta, Selector};
use ipnet::IpNet;
use maxminddb::{geoip2, Reader};
use serde::Serialize;
use serde_json::{Map, Value};
use tangent::logs::config;
use tangent::logs::log::Logview;

struct Component;

export!(Component);

const DEFAULT_FIELDS: &[&str] = &[
    "sourceIPAddress",
    "client.ipAddress",
    "actor_ip",
    "ipAddress",
    "ip",
    "remote_addr",
    "src_ip",
    "client_ip",
];

struct Cfg {
    fields: Vec<String>,
    ours: Vec<(u128, u128)>,
    output: String,
}

/// Everything under /data, read once per instance.
struct Data {
    country: Option<Reader<Vec<u8>>>,
    asn: Option<Reader<Vec<u8>>>,
    /// Each provider's published ranges, merged and sorted, as spans over
    /// the IPv6 address space (IPv4 mapped), so one binary search answers.
    hosting: Vec<(&'static str, Vec<(u128, u128)>)>,
    vpn: Vec<(u128, u128)>,
    tor: HashSet<IpAddr>,
}

#[derive(Serialize)]
struct IpInfo {
    ip: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    asn: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    org: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hosting: Option<&'static str>,
    tor: bool,
    vpn: bool,
    ours: bool,
    private: bool,
}

fn setting(name: &str) -> Option<String> {
    let raw = config::get(name)?;
    let t = raw.trim();
    let t = t
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(t);
    (!t.is_empty()).then(|| t.to_string())
}

/// A list setting: a JSON array (how the runtime hands a YAML list over), or
/// comma-separated text.
fn list_setting(name: &str) -> Option<Vec<String>> {
    let raw = setting(name)?;
    if raw.starts_with('[') {
        if let Ok(v) = serde_json::from_str::<Vec<String>>(&raw) {
            return Some(v);
        }
    }
    Some(
        raw.split(',')
            .map(|s| s.trim().trim_matches('"').to_string())
            .filter(|s| !s.is_empty())
            .collect(),
    )
}

fn cfg() -> &'static Cfg {
    static CFG: OnceLock<Cfg> = OnceLock::new();
    CFG.get_or_init(|| Cfg {
        fields: list_setting("fields")
            .unwrap_or_else(|| DEFAULT_FIELDS.iter().map(|s| s.to_string()).collect()),
        ours: merged(
            list_setting("ours")
                .unwrap_or_default()
                .iter()
                .filter_map(|s| parse_net(s))
                .map(span)
                .collect(),
        ),
        output: setting("output").unwrap_or_else(|| "_ip".to_string()),
    })
}

fn data() -> &'static Data {
    static DATA: OnceLock<Data> = OnceLock::new();
    DATA.get_or_init(load)
}

/// One address space for both families: IPv4 as IPv4-mapped IPv6.
fn key(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => u128::from(v4.to_ipv6_mapped()),
        IpAddr::V6(v6) => u128::from(v6),
    }
}

fn span(net: IpNet) -> (u128, u128) {
    match net {
        IpNet::V4(n) => (
            u128::from(n.network().to_ipv6_mapped()),
            u128::from(n.broadcast().to_ipv6_mapped()),
        ),
        IpNet::V6(n) => (u128::from(n.network()), u128::from(n.broadcast())),
    }
}

/// `10.0.0.0/8`, or a bare address as its own /32 or /128.
fn parse_net(s: &str) -> Option<IpNet> {
    let s = s.trim();
    s.parse::<IpNet>()
        .ok()
        .or_else(|| s.parse::<IpAddr>().ok().map(IpNet::from))
}

/// Sorted, with overlapping and adjacent spans merged.
fn merged(mut spans: Vec<(u128, u128)>) -> Vec<(u128, u128)> {
    spans.sort_unstable();
    let mut out: Vec<(u128, u128)> = Vec::with_capacity(spans.len());
    for (s, e) in spans {
        match out.last_mut() {
            Some(last) if s <= last.1.saturating_add(1) => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

fn contains(spans: &[(u128, u128)], k: u128) -> bool {
    match spans.binary_search_by(|(s, _)| s.cmp(&k)) {
        Ok(_) => true,
        Err(0) => false,
        Err(i) => spans[i - 1].1 >= k,
    }
}

fn read(name: &str) -> Option<Vec<u8>> {
    std::fs::read(format!("/data/{name}")).ok()
}

fn read_text(name: &str) -> Option<String> {
    read(name).and_then(|b| String::from_utf8(b).ok())
}

fn mmdb(name: &str) -> Option<Reader<Vec<u8>>> {
    let bytes = read(name)?;
    match Reader::from_source(bytes) {
        Ok(r) => Some(r),
        Err(e) => {
            eprintln!("enrich: {name}: {e}");
            None
        }
    }
}

/// One network per non-empty, non-comment line.
fn nets_from_lines(text: &str) -> Vec<IpNet> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(parse_net)
        .collect()
}

/// The prefixes in a provider's JSON, under the given keys.
fn nets_from_json(text: &str, list: &str, keys: &[&str]) -> Vec<IpNet> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    let mut nets = Vec::new();
    for entry in v[list].as_array().into_iter().flatten() {
        for k in keys {
            if let Some(n) = entry[k].as_str().and_then(parse_net) {
                nets.push(n);
            }
        }
    }
    nets
}

fn load() -> Data {
    let mut hosting = Vec::new();
    if let Some(t) = read_text("aws-ip-ranges.json") {
        let mut nets = nets_from_json(&t, "prefixes", &["ip_prefix"]);
        nets.extend(nets_from_json(&t, "ipv6_prefixes", &["ipv6_prefix"]));
        hosting.push(("aws", merged(nets.into_iter().map(span).collect())));
    }
    if let Some(t) = read_text("gcp-ip-ranges.json") {
        let nets = nets_from_json(&t, "prefixes", &["ipv4Prefix", "ipv6Prefix"]);
        hosting.push(("gcp", merged(nets.into_iter().map(span).collect())));
    }
    let mut cf = Vec::new();
    for name in ["cloudflare-ips-v4.txt", "cloudflare-ips-v6.txt"] {
        if let Some(t) = read_text(name) {
            cf.extend(nets_from_lines(&t));
        }
    }
    if !cf.is_empty() {
        hosting.push(("cloudflare", merged(cf.into_iter().map(span).collect())));
    }
    // X4BNet's lists are broad (they take in public DNS and CDNs too), so a
    // provider's own ranges answer first and these come last.
    if let Some(t) = read_text("datacenter-ips.txt") {
        let nets = nets_from_lines(&t);
        hosting.push(("datacenter", merged(nets.into_iter().map(span).collect())));
    }
    let vpn = read_text("vpn-ips.txt")
        .map(|t| merged(nets_from_lines(&t).into_iter().map(span).collect()))
        .unwrap_or_default();
    let tor = read_text("tor-exits.txt")
        .map(|t| t.lines().filter_map(|l| l.trim().parse().ok()).collect())
        .unwrap_or_default();
    Data {
        // The City database answers country questions too.
        country: mmdb("GeoLite2-Country.mmdb").or_else(|| mmdb("GeoLite2-City.mmdb")),
        asn: mmdb("GeoLite2-ASN.mmdb"),
        hosting,
        vpn,
        tor,
    }
}

fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => v.is_private() || v.is_loopback() || v.is_link_local() || v.is_unspecified(),
        IpAddr::V6(v) => {
            v.is_loopback()
                || v.is_unspecified()
                || (v.segments()[0] & 0xfe00) == 0xfc00
                || (v.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

fn enrich(d: &Data, cfg: &Cfg, ip: IpAddr) -> IpInfo {
    let k = key(ip);
    let country = d
        .country
        .as_ref()
        .and_then(|r| r.lookup(ip).ok()?.decode::<geoip2::Country>().ok()?)
        .and_then(|c| c.country.iso_code)
        .map(str::to_string);
    let asn = d
        .asn
        .as_ref()
        .and_then(|r| r.lookup(ip).ok()?.decode::<geoip2::Asn>().ok()?);
    IpInfo {
        ip: ip.to_string(),
        country,
        asn: asn.as_ref().and_then(|a| a.autonomous_system_number),
        org: asn
            .as_ref()
            .and_then(|a| a.autonomous_system_organization)
            .map(str::to_string),
        hosting: d
            .hosting
            .iter()
            .find(|(_, spans)| contains(spans, k))
            .map(|(name, _)| *name),
        tor: d.tor.contains(&ip),
        vpn: contains(&d.vpn, k),
        ours: contains(&cfg.ours, k),
        private: is_private(ip),
    }
}

/// The address in a field's text: the first of a comma-separated list, with
/// brackets and a port stripped.
fn parse_ip(s: &str) -> Option<IpAddr> {
    let s = s.split(',').next()?.trim();
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(ip);
    }
    let s = s.strip_prefix('[').map_or(s, |r| r.split(']').next().unwrap_or(r));
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(ip);
    }
    // a.b.c.d:port
    s.rsplit_once(':')
        .and_then(|(host, _)| host.parse::<IpAddr>().ok())
}

fn lookup<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    if let Some(direct) = v.get(path) {
        return Some(direct);
    }
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "enrich".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    /// Every record: a decorator in a chain must not drop what it cannot
    /// decorate. Records without an address pass through unchanged.
    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: Vec::new(),
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let cfg = cfg();
        let d = data();
        let mut out = Vec::new();
        for lv in input {
            let mut rec: Value = serde_json::from_str(&lv.log()).map_err(|e| e.to_string())?;
            let mut info = Map::new();
            for field in &cfg.fields {
                let Some(ip) = lookup(&rec, field)
                    .and_then(Value::as_str)
                    .and_then(parse_ip)
                else {
                    continue;
                };
                let facts = serde_json::to_value(enrich(d, cfg, ip)).map_err(|e| e.to_string())?;
                info.insert(field.clone(), facts);
            }
            if !info.is_empty() {
                match rec.get_mut(&cfg.output) {
                    Some(Value::Object(existing)) => existing.extend(info),
                    _ => {
                        rec[&cfg.output] = Value::Object(info);
                    }
                }
            }
            out.extend(serde_json::to_vec(&rec).map_err(|e| e.to_string())?);
            out.push(b'\n');
        }
        Ok(out)
    }
}
