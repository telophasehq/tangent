use reqwest;
use std::time::Duration;

pub struct Stats {
    pub sink_bytes: f64,
    pub sink_bytes_uncompressed: f64,
    pub inflight: f64,
    pub wal_pending: f64,
    pub consumer_bytes: f64,
    pub guest_bytes: f64,
    pub guest_seconds_sum: f64,
    pub guest_seconds_count: f64,
}

pub async fn scrape_stats(url: &str) -> anyhow::Result<Stats> {
    let body = reqwest::get(url).await?.text().await?;
    let scrape = prometheus_parse::Scrape::parse(body.lines().map(|s| Ok(s.to_string())))?;
    let sum_exact = |name: &str| -> f64 {
        scrape
            .samples
            .iter()
            .filter(|s| s.metric == name)
            .map(|s| match &s.value {
                prometheus_parse::Value::Counter(v)
                | prometheus_parse::Value::Gauge(v)
                | prometheus_parse::Value::Untyped(v) => *v,
                prometheus_parse::Value::Histogram(_) => 0.0,
                prometheus_parse::Value::Summary(_) => 0.0,
            })
            .sum()
    };
    Ok(Stats {
        sink_bytes: sum_exact("tangent_sink_bytes_total"),
        sink_bytes_uncompressed: sum_exact("tangent_sink_bytes_uncompressed_total"),
        inflight: sum_exact("tangent_inflight"),
        wal_pending: sum_exact("tangent_wal_pending_files"),
        consumer_bytes: sum_exact("tangent_consumer_bytes_total"),
        guest_bytes: sum_exact("tangent_guest_bytes_total"),
        guest_seconds_sum: sum_exact("tangent_guest_seconds_sum"),
        guest_seconds_count: sum_exact("tangent_guest_seconds_count"),
    })
}
