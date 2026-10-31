//! A `cidr` table: `prefix,label` lines, IPv4 and IPv6, `#` comments, and
//! a first line `prefix,label` taken as a header. A bare address is its own
//! /32 or /128. The answer for an address is the label of every prefix that
//! holds it, as a JSON array, each label once, in the order the labels first
//! appear in the file: a file lists first the labels that should win.

use std::collections::{BTreeSet, HashMap};
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use ipnet::{IpNet, Ipv4Net};

/// IPv4 and IPv6 each cut into runs that share one answer. The two are kept
/// apart: an IPv6 prefix never holds an IPv4 address, however wide (`::/0`,
/// `::/8`), and an IPv4-mapped prefix (`::ffff:192.0.2.0/120`) is the IPv4
/// prefix it maps.
pub struct Cidr {
    v4: Runs<u32>,
    v6: Runs<u128>,
    /// Each distinct set of labels once, as its JSON text.
    answers: Vec<Arc<str>>,
    prefixes: usize,
}

/// One address family's runs.
struct Runs<K> {
    /// Where each run starts, ascending.
    starts: Vec<K>,
    /// Each run's answer, an index into `answers`; `NONE` for none.
    runs: Vec<u32>,
}

/// Each prefix as +1 for its label where it starts and -1 just past its end.
type Edges<K> = Vec<(K, bool, u32)>;

const NONE: u32 = u32::MAX;

impl Cidr {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(bytes)?;
        let mut labels: Vec<&str> = Vec::new();
        let mut ids: HashMap<&str, u32> = HashMap::new();
        let mut v4: Edges<u32> = Vec::new();
        let mut v6: Edges<u128> = Vec::new();
        let mut prefixes = 0;
        let mut first = true;
        for (i, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((prefix, label)) = line.split_once(',') else {
                bail!("line {}: want `prefix,label`, got `{line}`", i + 1);
            };
            let (prefix, label) = (prefix.trim(), label.trim());
            if std::mem::take(&mut first) && prefix == "prefix" {
                continue;
            }
            let Some(net) = net(prefix) else {
                bail!("line {}: `{prefix}` is not an address or a prefix", i + 1);
            };
            if label.is_empty() {
                bail!("line {}: `{prefix}` has no label", i + 1);
            }
            let id = *ids.entry(label).or_insert_with(|| {
                labels.push(label);
                labels.len() as u32 - 1
            });
            prefixes += 1;
            match net {
                IpNet::V4(n) => {
                    let (start, end) = (u32::from(n.network()), u32::from(n.broadcast()));
                    v4.push((start, true, id));
                    if let Some(past) = end.checked_add(1) {
                        v4.push((past, false, id));
                    }
                }
                IpNet::V6(n) => {
                    let (start, end) = (u128::from(n.network()), u128::from(n.broadcast()));
                    v6.push((start, true, id));
                    if let Some(past) = end.checked_add(1) {
                        v6.push((past, false, id));
                    }
                }
            }
        }
        let mut sets = Sets {
            labels: &labels,
            ids: HashMap::new(),
            answers: Vec::new(),
        };
        let v4 = sets.runs(v4)?;
        let v6 = sets.runs(v6)?;
        Ok(Self {
            v4,
            v6,
            answers: sets.answers,
            prefixes,
        })
    }

    /// The labels of every prefix that holds `ip`, as a JSON array; None
    /// when no prefix does. An IPv4-mapped address is its IPv4 address.
    pub fn lookup(&self, ip: IpAddr) -> Option<&str> {
        let a = match ip.to_canonical() {
            IpAddr::V4(v4) => self.v4.find(u32::from(v4)),
            IpAddr::V6(v6) => self.v6.find(u128::from(v6)),
        }?;
        Some(&self.answers[a as usize])
    }

    /// Lines read, comments aside.
    pub fn prefixes(&self) -> usize {
        self.prefixes
    }
}

impl<K: Copy + Ord> Runs<K> {
    fn find(&self, k: K) -> Option<u32> {
        let i = self.starts.partition_point(|&s| s <= k).checked_sub(1)?;
        Some(self.runs[i]).filter(|&a| a != NONE)
    }
}

/// The label sets met so far, each with its answer.
struct Sets<'a> {
    labels: &'a [&'a str],
    ids: HashMap<Vec<u32>, u32>,
    answers: Vec<Arc<str>>,
}

impl Sets<'_> {
    /// One pass over the edges: at each, the labels whose prefixes hold the
    /// addresses from there to the next.
    fn runs<K: Copy + Ord>(&mut self, mut edges: Edges<K>) -> Result<Runs<K>> {
        edges.sort_unstable_by_key(|e| e.0);
        let mut held = vec![0u32; self.labels.len()];
        let mut active: BTreeSet<u32> = BTreeSet::new();
        let mut starts: Vec<K> = Vec::new();
        let mut runs: Vec<u32> = Vec::new();
        let mut i = 0;
        while i < edges.len() {
            let at = edges[i].0;
            while i < edges.len() && edges[i].0 == at {
                let (_, opens, id) = edges[i];
                let n = &mut held[id as usize];
                if opens {
                    *n += 1;
                    active.insert(id);
                } else {
                    *n -= 1;
                    if *n == 0 {
                        active.remove(&id);
                    }
                }
                i += 1;
            }
            let run = if active.is_empty() {
                NONE
            } else {
                let set: Vec<u32> = active.iter().copied().collect();
                match self.ids.get(&set) {
                    Some(&a) => a,
                    None => {
                        let names: Vec<&str> =
                            set.iter().map(|&l| self.labels[l as usize]).collect();
                        self.answers.push(Arc::from(serde_json::to_string(&names)?));
                        let a = self.answers.len() as u32 - 1;
                        self.ids.insert(set, a);
                        a
                    }
                }
            };
            if runs.last() != Some(&run) {
                starts.push(at);
                runs.push(run);
            }
        }
        Ok(Runs { starts, runs })
    }
}

/// `10.0.0.0/8` (host bits cleared), or a bare address as its own /32 or
/// /128; an IPv4-mapped prefix as the IPv4 prefix it maps.
fn net(s: &str) -> Option<IpNet> {
    let net = s
        .parse::<IpNet>()
        .ok()
        .map(|n| n.trunc())
        .or_else(|| s.parse::<IpAddr>().ok().map(IpNet::from))?;
    if let IpNet::V6(n) = net {
        if let (Some(v4), 96..) = (n.network().to_ipv4_mapped(), n.prefix_len()) {
            return Ipv4Net::new(v4, n.prefix_len() - 96).ok().map(IpNet::V4);
        }
    }
    Some(net)
}
