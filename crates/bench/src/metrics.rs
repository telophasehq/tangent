use reqwest;

pub struct Stats {
    pub batch_objects: f64,
    pub sink_bytes: f64,
    pub sink_objects: f64,
    pub inflight: f64,
    pub consumer_objects: f64,
    pub consumer_bytes: f64,
    pub wal_pending: f64,
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
        batch_objects: sum("tangent_batch_objects_total"),
        sink_bytes: sum("tangent_sink_bytes_total"),
        sink_objects: sum("tangent_sink_objects_total"),
        inflight: sum("tangent_inflight"),
        consumer_bytes: sum("tangent_consumer_bytes_total"),
        consumer_objects: sum("tangent_consumer_objects_total"),
        wal_pending: sum("tangent_wal_pending_files"),
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
