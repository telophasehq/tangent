//! What is known about an address, from the runtime's enrichment tables: no
//! file is read here, and nothing leaves the machine. The tables, as
//! packs/core/ingest.yaml names them:
//!
//!   geo        mmdb      GeoLite2 Country: `country.iso_code`
//!   asn        mmdb      GeoLite2 ASN: the network's number and organization
//!   networks   cidr      `prefix,label`: `tor` and `vpn` are flags, any other
//!                        label names a hosting provider (`aws`, `gcp`,
//!                        `cloudflare`, `datacenter`); the first one wins
//!   profile    document  the company profile; its `networks` are the org's own
//!
//! A table that is missing answers nothing, and its fields are left out.

use std::net::IpAddr;

use ipnet::{IpNet, Ipv4Net};
use serde::Serialize;
use serde_json::{Map, Value};

/// The runtime's `enrichment` lookup, as each plugin's bindings generate it.
pub type Lookup = fn(&str, &str) -> Option<String>;

/// An address, as the tables describe it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Ip {
    pub ip: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hosting: Option<String>,
    pub tor: bool,
    pub vpn: bool,
    /// one of the org's own networks (the profile's `networks`)
    pub ours: bool,
    pub private: bool,
}

impl Ip {
    /// What `_facts` holds for it: the address itself and every `false` left out.
    pub fn facts(&self) -> Map<String, Value> {
        let Ok(Value::Object(m)) = serde_json::to_value(self) else {
            return Map::new();
        };
        m.into_iter()
            .filter(|(k, v)| k != "ip" && *v != Value::Bool(false))
            .collect()
    }
}

/// The tables, and the org's own networks read from the profile once.
pub struct Tables {
    lookup: Lookup,
    ours: Vec<IpNet>,
}

impl Tables {
    pub fn new(lookup: Lookup) -> Self {
        let ours = json(lookup, "profile", "networks")
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter_map(parse_net)
            .collect();
        Tables { lookup, ours }
    }

    /// The address in `text` (see [`parse_ip`]), described; None when there is none.
    pub fn ip(&self, text: &str) -> Option<Ip> {
        let ip = parse_ip(text)?;
        let key = ip.to_string();
        let geo = json(self.lookup, "geo", &key);
        let asn = json(self.lookup, "asn", &key);
        let labels = json(self.lookup, "networks", &key);
        let labels: Vec<&str> = labels
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        Some(Ip {
            country: geo
                .pointer("/country/iso_code")
                .and_then(Value::as_str)
                .map(str::to_string),
            asn: asn["autonomous_system_number"].as_u64(),
            org: asn["autonomous_system_organization"]
                .as_str()
                .map(str::to_string),
            hosting: labels
                .iter()
                .find(|l| !matches!(**l, "tor" | "vpn"))
                .map(|l| l.to_string()),
            tor: labels.contains(&"tor"),
            vpn: labels.contains(&"vpn"),
            ours: self.ours.iter().any(|n| n.contains(&ip)),
            private: is_private(ip),
            ip: key,
        })
    }
}

fn json(lookup: Lookup, table: &str, key: &str) -> Value {
    lookup(table, key)
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(Value::Null)
}

/// `10.0.0.0/8`, or a bare address as its own /32 or /128; an IPv4-mapped
/// prefix as the IPv4 prefix it maps, since every address is checked as
/// IPv4 when it is one.
pub fn parse_net(s: &str) -> Option<IpNet> {
    let s = s.trim();
    let net = s
        .parse::<IpNet>()
        .ok()
        .or_else(|| s.parse::<IpAddr>().ok().map(IpNet::from))?;
    if let IpNet::V6(n) = net {
        if let (Some(v4), 96..) = (n.network().to_ipv4_mapped(), n.prefix_len()) {
            return Ipv4Net::new(v4, n.prefix_len() - 96).ok().map(IpNet::V4);
        }
    }
    Some(net)
}

/// The address in a field's text: the first of a comma-separated list, with
/// brackets and a port stripped. An IPv4-mapped IPv6 address
/// (`::ffff:192.0.2.1`, as a dual-stack server logs IPv4) is the IPv4
/// address, for every table and every check.
pub fn parse_ip(s: &str) -> Option<IpAddr> {
    let s = s.split(',').next()?.trim();
    let s = s
        .strip_prefix('[')
        .map_or(s, |r| r.split(']').next().unwrap_or(r));
    let ip = s.parse::<IpAddr>().ok().or_else(|| {
        // a.b.c.d:port
        s.rsplit_once(':')
            .and_then(|(host, _)| host.parse::<IpAddr>().ok())
    })?;
    Some(ip.to_canonical())
}

fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            v.is_private() || v.is_loopback() || v.is_link_local() || v.is_unspecified()
        }
        IpAddr::V6(v) => {
            v.is_loopback()
                || v.is_unspecified()
                || (v.segments()[0] & 0xfe00) == 0xfc00
                || (v.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tables as the runtime would answer them, for a few addresses.
    fn tables(table: &str, key: &str) -> Option<String> {
        let answer = match (table, key) {
            ("geo", "203.0.113.7") => {
                r#"{"country":{"iso_code":"NL","names":{"en":"Netherlands"}}}"#
            }
            ("asn", "203.0.113.7") => {
                r#"{"autonomous_system_number":64500,"autonomous_system_organization":"Example Hosting"}"#
            }
            ("networks", "203.0.113.7") => r#"["datacenter","vpn","tor"]"#,
            ("networks", "198.51.100.9") => r#"["vpn"]"#,
            ("profile", "networks") => r#"["10.0.0.0/8","198.51.100.9"]"#,
            _ => return None,
        };
        Some(answer.to_string())
    }

    #[test]
    fn an_address_described() {
        let t = Tables::new(tables);
        let ip = t.ip("203.0.113.7:443").unwrap();
        assert_eq!(
            serde_json::to_string(&ip).unwrap(),
            r#"{"ip":"203.0.113.7","country":"NL","asn":64500,"org":"Example Hosting","hosting":"datacenter","tor":true,"vpn":true,"ours":false,"private":false}"#
        );
        assert_eq!(
            Value::Object(ip.facts()).to_string(),
            r#"{"asn":64500,"country":"NL","hosting":"datacenter","org":"Example Hosting","tor":true,"vpn":true}"#
        );
    }

    #[test]
    fn the_org_s_own_and_private_addresses() {
        let t = Tables::new(tables);
        let ip = t.ip("198.51.100.9").unwrap();
        assert!(ip.ours && ip.vpn && ip.hosting.is_none());
        let ip = t.ip("10.1.2.3").unwrap();
        assert!(ip.ours && ip.private && ip.country.is_none());
        let mapped = t.ip("::ffff:10.1.2.3").unwrap();
        assert!(mapped.ours && mapped.private && mapped.ip == "10.1.2.3");
        assert_eq!(
            Value::Object(t.ip("10.1.2.3").unwrap().facts()).to_string(),
            r#"{"ours":true,"private":true}"#
        );
    }

    #[test]
    fn no_tables_no_facts() {
        let t = Tables::new(|_, _| None);
        let ip = t.ip("192.0.2.1").unwrap();
        assert_eq!(Value::Object(ip.facts()).to_string(), "{}");
        assert!(t.ip("AWS Internal").is_none());
    }

    #[test]
    fn addresses_in_text() {
        let ip = |s| parse_ip(s).map(|a| a.to_string());
        assert_eq!(ip("192.0.2.1, 10.0.0.1").as_deref(), Some("192.0.2.1"));
        assert_eq!(ip("[2001:db8::1]:443").as_deref(), Some("2001:db8::1"));
        assert_eq!(ip("192.0.2.1:8080").as_deref(), Some("192.0.2.1"));
        assert_eq!(ip("sts.amazonaws.com"), None);
        assert_eq!(ip("::ffff:192.0.2.1").as_deref(), Some("192.0.2.1"));
        assert_eq!(ip("[::ffff:192.0.2.1]:443").as_deref(), Some("192.0.2.1"));
        assert_eq!(ip("::1").as_deref(), Some("::1"));
        let net = |s| parse_net(s).map(|n| n.to_string());
        assert_eq!(net("::ffff:10.0.0.0/104").as_deref(), Some("10.0.0.0/8"));
        assert_eq!(net("::ffff:192.0.2.1").as_deref(), Some("192.0.2.1/32"));
        assert_eq!(net("2001:db8::/32").as_deref(), Some("2001:db8::/32"));
    }
}
