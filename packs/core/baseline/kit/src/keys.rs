//! The keys: each starts with the org, its parts joined by `|`, with `%` and
//! `|` in a part %-escaped. The table of them is in
//! packs/core/baseline/README.md.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr};

use serde::{Deserialize, Serialize};

use crate::kinds::Ev;
use crate::Scalar;

pub fn key(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|p| p.replace('%', "%25").replace('|', "%7C"))
        .collect::<Vec<_>>()
        .join("|")
}

pub fn unesc(x: &str) -> String {
    x.replace("%7C", "|").replace("%25", "%")
}

/// Epoch milliseconds in 13 digits, so rows keyed by time sort by it.
pub fn t13(ms: i64) -> String {
    format!("{:013}", ms.max(0))
}

/// The dimensions an actor's first-seen times are kept for.
pub fn values(e: &Ev) -> Vec<(&'static str, String)> {
    let mut out = vec![("api", e.verb.clone()), ("src", e.src.clone())];
    out.extend(e.res.iter().map(|r| ("res", r.clone())));
    out.extend(e.client.iter().map(|c| ("client", c.clone())));
    out.extend(e.oauth.iter().map(|(i, _)| ("oauth", i.clone())));
    out.extend(e.device.iter().map(|d| ("device", d.clone())));
    out.extend(e.token.iter().map(|t| ("token", t.clone())));
    out
}

/// The networks a person's and the org's first-seen times are kept for.
pub fn nets(e: &Ev) -> Vec<(&'static str, String)> {
    let net =
        e.ip.as_deref()
            .and_then(|ip| match ip.parse::<IpAddr>().ok()? {
                IpAddr::V4(a) => {
                    let [x, y, z, _] = a.octets();
                    Some(format!("{x}.{y}.{z}.0/24"))
                }
                IpAddr::V6(a) => {
                    let [p, q, r, ..] = a.segments();
                    Some(format!("{}/48", Ipv6Addr::new(p, q, r, 0, 0, 0, 0, 0)))
                }
            });
    [("asn", e.asn.clone()), ("cc", e.cc.clone()), ("net", net)]
        .into_iter()
        .filter_map(|(d, v)| Some((d, v?)))
        .collect()
}

/// One assumption of a role session, in its hour's row.
#[derive(Serialize, Deserialize)]
pub struct Assume {
    pub t: i64,
    pub by: String,
    /// `by`'s person, when who is who names one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub person: Option<String>,
    #[serde(rename = "type")]
    pub typ: String,
    pub arn: String,
}

pub fn int(v: &Scalar) -> Option<i64> {
    match v {
        Scalar::Int(i) => Some(*i),
        Scalar::Str(s) => s.parse().ok(),
    }
}

pub fn string(v: Scalar) -> Option<String> {
    match v {
        Scalar::Str(s) => Some(s),
        Scalar::Int(_) => None,
    }
}

/// An hour's row: events per five-minute slot, by category.
pub fn blob(v: Option<Scalar>) -> BTreeMap<String, BTreeMap<String, i64>> {
    v.and_then(string)
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

/// A role session's hour: who assumed it.
pub fn assumes(v: Option<Scalar>) -> Vec<Assume> {
    v.and_then(string)
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{key, t13, unesc};

    /// A part holding `|` or `%` comes back whole: the name the builder files
    /// is the one the renderer reads.
    #[test]
    fn a_part_comes_back_whole() {
        let parts = ["acme", "a", "ci|bot", "%7C%25"];
        let k = key(&parts);
        assert_eq!(k.split('|').map(unesc).collect::<Vec<_>>(), parts);
        assert!(t13(999) < t13(1000), "rows sort by time");
    }
}
