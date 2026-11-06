//! `links`: the same person or address across sources and actors. A login
//! from the window's address; a login from a network new for the person in
//! the hour before; an address new for this actor, and who used it before.
//! new.rs adds when an OAuth app in the window was first seen.

use baselinekit::time::H;
use baselinekit::{Kv, R};
use serde_json::Value;

use crate::auth::{by_of, new_net};
use crate::before::Before;
use crate::header::Win;
use crate::text::{ago, cap, nn};

pub fn links<K: Kv>(
    b: &mut Before<K>,
    w: &Win,
    logins: &[(i64, Value)],
    links: &mut Vec<String>,
) -> R<()> {
    let (a, ws) = (w.actor, w.ws);
    let mut wips: Vec<&str> = Vec::new();
    for ip in w.fresh.iter().filter_map(|e| e.ip.as_deref()) {
        if !wips.contains(&ip) {
            wips.push(ip);
        }
    }
    if let Some((t, x)) = logins
        .iter()
        .find(|(_, x)| x["ip"].as_str().is_some_and(|ip| wips.contains(&ip)))
    {
        let (ip, what, by) = (
            x["ip"].as_str().unwrap_or(""),
            x["what"].as_str().unwrap_or(""),
            x["by"].as_str().unwrap_or(""),
        );
        links.push(format!(
            "window ip {ip} matches {what} login by {by} {} before",
            ago(ws - t)
        ));
    }
    for (t, x) in logins {
        let p = by_of(x);
        if ws - t <= H && !new_net(b, &p, x, *t)?.is_empty() {
            let l = format!(
                "{} login from a network new for {p} {} before this window",
                x["what"].as_str().unwrap_or(""),
                ago(ws - t)
            );
            if !links.contains(&l) {
                links.push(l);
            }
        }
    }
    let mut shown = 0;
    for ip in wips {
        if shown == 2 || b.first(&["o", "ip", ip, a])?.is_some() {
            continue;
        }
        shown += 1;
        let others: Vec<String> = b
            .firsts(&["o", "ip", ip])?
            .into_iter()
            .map(|(x, _)| x)
            .filter(|x| x != a)
            .collect();
        links.push(if others.is_empty() {
            format!("ip {ip} new for this actor, no other actor used it before")
        } else {
            format!(
                "ip {ip} new for this actor, used before by {} ({})",
                nn(others.len() as i64, "other actor"),
                cap(others, 3)
            )
        });
    }
    Ok(())
}
