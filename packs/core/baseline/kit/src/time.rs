//! Time as epoch milliseconds: the spans keys are cut in, and RFC 3339 to
//! and from it.

pub const M: i64 = 60_000;
pub const H: i64 = 3_600_000;
pub const D: i64 = 24 * H;
/// An hour's row counts events per five minutes.
pub const SLOT: i64 = 300_000;

/// RFC 3339 to epoch milliseconds.
pub fn ms_of(ts: &str) -> Option<i64> {
    let num = |a: usize, b: usize| ts.get(a..b).and_then(|x| x.parse::<i64>().ok());
    let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (h, mi, sec) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    let rest = ts.get(19..)?;
    let frac_len = rest
        .strip_prefix('.')
        .map_or(0, |f| f.bytes().take_while(u8::is_ascii_digit).count());
    let frac = if frac_len > 0 {
        rest.get(1..1 + frac_len.min(3))
            .and_then(|f| f.parse::<i64>().ok())
    } else {
        None
    };
    let ms = frac.map_or(0, |f| f * 10_i64.pow(3 - frac_len.min(3) as u32));
    let tz = rest.get(if frac_len > 0 { 1 + frac_len } else { 0 }..)?;
    let off = match tz {
        "" | "Z" | "z" => 0,
        _ => {
            let sign = if tz.starts_with('-') { -1 } else { 1 };
            let hh = tz.get(1..3)?.parse::<i64>().ok()?;
            let mm = tz
                .get(4..6)
                .and_then(|x| x.parse::<i64>().ok())
                .unwrap_or(0);
            sign * (hh * 60 + mm) * 60_000
        }
    };
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + sec) * 1000 + ms - off)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
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
