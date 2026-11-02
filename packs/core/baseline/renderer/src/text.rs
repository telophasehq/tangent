//! How `_baseline` writes durations, dates, counts and lists.

use serde_json::{Map, Value};

pub fn ago(ms: i64) -> String {
    let s = ms.max(0) / 1000;
    match s {
        0..90 => format!("{s}s"),
        90..5400 => format!("{}m", s / 60),
        5400..172_800 => format!("{}h", s / 3600),
        _ => format!("{}d", s / 86_400),
    }
}

/// Epoch milliseconds to `YYYY-MM-DD`.
pub fn date(ms: i64) -> String {
    let z = ms.div_euclid(86_400_000) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{m:02}-{d:02}", yoe + era * 400 + i64::from(m <= 2))
}

pub fn nn(n: i64, word: &str) -> String {
    format!("{n} {word}{}", if n == 1 { "" } else { "s" })
}

pub fn cap(xs: impl IntoIterator<Item = String>, n: usize) -> String {
    let xs: Vec<String> = xs.into_iter().collect();
    let more = if xs.len() > n {
        format!(" +{} more", xs.len() - n)
    } else {
        String::new()
    };
    join(xs.into_iter().take(n)) + &more
}

pub fn join<T: std::fmt::Display>(xs: impl IntoIterator<Item = T>) -> String {
    xs.into_iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Printable ASCII with no `"`, `\` or `=`, so logfmt writes each line bare.
pub fn safe(h: Map<String, Value>) -> Map<String, Value> {
    h.into_iter()
        .map(|(k, v)| {
            let v = match v {
                Value::String(s) => Value::String(
                    s.chars()
                        .map(|c| {
                            if matches!(c, ' '..='~') && !matches!(c, '"' | '\\' | '=') {
                                c
                            } else {
                                '?'
                            }
                        })
                        .collect(),
                ),
                Value::Object(m) => Value::Object(safe(m)),
                other => other,
            };
            (k, v)
        })
        .collect()
}
