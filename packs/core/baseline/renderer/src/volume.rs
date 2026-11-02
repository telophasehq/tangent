//! `volume`: the last hour against the p90 of the actor's active hours in 28
//! days, in total and per category.

use std::collections::BTreeMap;

use baselinekit::time::{H, SLOT};

use crate::header::Win;
use crate::seen::Slots;
use crate::text::nn;

const FLOOR: f64 = 3.0; // events an hour below which volume is normal

/// `active`: the actor was active in the 28 days, as `time`'s last active says.
pub fn volume(w: &Win, slots: &Slots, hourly: &Slots, active: bool) -> String {
    if !active {
        return "no activity in 28d to compare with".to_string();
    }
    if hourly.is_empty() {
        return "no earlier active hour in 28d to compare with".to_string();
    }
    let (ws, we) = (w.ws, w.we);
    // The slot holding ws is not in `slots`; the request's `_context` covers it.
    let open = ws - ws.rem_euclid(SLOT);
    let period = (we - ws).max(H);
    let mut cur: BTreeMap<String, i64> = BTreeMap::new();
    let lately = w.ctx.iter().filter(|e| e.ms >= open.max(we - period));
    for e in w.fresh.iter().chain(lately) {
        *cur.entry(e.cat.to_string()).or_default() += 1;
    }
    for (_, c) in slots.range(we - period..) {
        for (k, n) in c {
            *cur.entry(k.clone()).or_default() += n;
        }
    }
    let rate = |n: i64| n as f64 * H as f64 / period as f64;
    let tot = p90(hourly.values().map(|c| c.values().sum()).collect());
    let mut out = vec![format!("total {}", bucket(rate(cur.values().sum()), tot))];
    let cols = [
        ("r", "read"),
        ("w", "write"),
        ("i", "iam"),
        ("c", "own creds"),
        ("d", "delete"),
    ];
    for (k, name) in cols {
        let base = p90(hourly
            .values()
            .filter_map(|c| c.get(k).copied())
            .filter(|n| *n > 0)
            .collect());
        out.push(format!(
            "{name} {}",
            bucket(rate(cur.get(k).copied().unwrap_or(0)), base)
        ));
    }
    format!(
        "last hour vs p90 of its {} in 28d: {}",
        nn(hourly.len() as i64, "active hour"),
        out.join(", ")
    )
}

fn bucket(n: f64, base: f64) -> &'static str {
    if n <= 0.0 {
        return "-";
    }
    if base <= 0.0 {
        return "none before";
    }
    match n / base.max(FLOOR) {
        x if x < 2.0 => "normal",
        x if x < 5.0 => "2-5x",
        x if x < 10.0 => "5-10x",
        _ => ">10x",
    }
}

fn p90(mut xs: Vec<i64>) -> f64 {
    xs.sort_unstable();
    let i = ((xs.len() as f64 * 0.9) as usize).min(xs.len().saturating_sub(1));
    xs.get(i).copied().unwrap_or(0) as f64
}

#[cfg(test)]
mod tests {
    use super::bucket;

    #[test]
    fn volume_buckets_have_a_floor() {
        assert_eq!(bucket(0.0, 4.0), "-");
        assert_eq!(bucket(4.0, 0.0), "none before");
        assert_eq!(bucket(5.0, 1.0), "normal"); // 5 against the floor of 3, not 5x
        assert_eq!(bucket(7.0, 1.0), "2-5x");
        assert_eq!(bucket(40.0, 2.0), ">10x");
    }
}
