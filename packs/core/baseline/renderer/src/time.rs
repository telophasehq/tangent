//! `time`: the window's hours against the actor's usual hours in 28 days, and
//! how long since it was last active; or dormant.

use std::collections::{BTreeMap, BTreeSet};

use baselinekit::time::{D, H, SLOT};

use crate::header::Win;
use crate::seen::Slots;
use crate::text::{ago, join, nn};

/// How long before the window the actor was last active in 28 days: its
/// latest slot's end, or a `_context` record in the window's own five
/// minutes, which no slot holds.
pub fn last(w: &Win, slots: &Slots) -> Option<String> {
    (slots.keys().next_back().map(|t| t + SLOT))
        .max(w.ctx.iter().map(|e| e.ms).max())
        .map(|t| ago(w.ws - t))
}

pub fn time(w: &Win, days: &BTreeMap<i64, i64>, hourly: &Slots, last: Option<&str>) -> String {
    let ws = w.ws;
    let before_that = days.keys().next_back().map(|d| ago(ws - (d + 1) * D));
    let Some(last) = last else {
        let day = before_that
            .map(|x| format!(", last active day {x} before"))
            .unwrap_or_default();
        return format!("dormant: no activity in 28d{day}");
    };
    if hourly.is_empty() {
        let day = before_that
            .map(|x| format!("; before that, last active day {x} before"))
            .unwrap_or_default();
        return format!("active only this hour in 28d, last active {last} before{day}");
    }
    let mut dayhours: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
    for hr in hourly.keys() {
        dayhours
            .entry(hr.rem_euclid(24))
            .or_default()
            .insert(hr.div_euclid(24));
    }
    let ndays = hourly
        .keys()
        .map(|hr| hr.div_euclid(24))
        .collect::<BTreeSet<_>>()
        .len();
    let usual: Vec<i64> = (dayhours.iter())
        .filter(|(_, ds)| ds.len() as f64 >= (0.1 * ndays as f64).max(2.0))
        .map(|(x, _)| *x)
        .collect();
    let wh: BTreeSet<i64> = w
        .fresh
        .iter()
        .map(|e| e.ms.div_euclid(H).rem_euclid(24))
        .collect();
    let at = wh.iter().map(|x| {
        format!(
            "{x:02} UTC active on {}",
            dayhours.get(x).map_or(0, BTreeSet::len)
        )
    });
    let usual = if usual.is_empty() {
        "hours: none yet".to_string()
    } else {
        format!("{} UTC", ranges(&usual))
    };
    format!(
        "window {} of its {} in 28d; usual {usual}; last active {last} before",
        join(at),
        nn(ndays as i64, "active day")
    )
}

fn ranges(hours: &[i64]) -> String {
    let mut out: Vec<(i64, i64)> = Vec::new();
    for &x in hours {
        match out.last_mut() {
            Some((_, b)) if x == *b + 1 => *b = x,
            _ => out.push((x, x)),
        }
    }
    join(out.iter().map(|(a, b)| {
        if a == b {
            format!("{a:02}")
        } else {
            format!("{a:02}-{b:02}")
        }
    }))
}
