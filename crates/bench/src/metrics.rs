use reqwest;

pub struct Stats {
    pub sink_bytes: f64,
    pub sink_bytes_uncompressed: f64,
    pub inflight: f64,
    pub wal_pending: f64,
    pub consumer_bytes: f64,
}

pub async fn scrape_stats(url: &str) -> anyhow::Result<Stats> {
    let body = reqwest::get(url).await?.text().await?;
    let scrape = prometheus_parse::Scrape::parse(body.lines().map(|s| Ok(s.to_string())))?;
    let sum = |name: &str| -> f64 {
        scrape
            .samples
            .iter()
            .filter(|s| s.metric == name)
            .map(|s| match &s.value {
                prometheus_parse::Value::Counter(v)
                | prometheus_parse::Value::Gauge(v)
                | prometheus_parse::Value::Untyped(v) => *v,
                prometheus_parse::Value::Histogram(b) => b.last().map(|x| x.count).unwrap_or(0.0),
                prometheus_parse::Value::Summary(q) => q.last().map(|x| x.count).unwrap_or(0.0),
            })
            .sum()
    };
    Ok(Stats {
        sink_bytes: sum("tangent_sink_bytes_total"),
        inflight: sum("tangent_inflight"),
        wal_pending: sum("tangent_wal_pending_files"),
        consumer_bytes: sum("tangent_consumer_bytes_total"),
        sink_bytes_uncompressed: sum("tangent_sink_bytes_uncompressed_total"),
    })
}

pub async fn wait_for_drain(url: &str) -> anyhow::Result<Stats> {
    use std::time::Duration;

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let now = scrape_stats(url).await?;
        if now.inflight == 0.0 && now.wal_pending == 0.0 {
            return Ok(now);
        }
    }
}
