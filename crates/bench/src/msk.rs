use anyhow::{Context, Result};
use bytes::BytesMut;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use secrecy::ExposeSecret;
use serde_json::Value;
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
use tangent_shared::msk::{MSKAuth, MSKConfig};
use tokio::task;
use tracing::info;

// Build a SCRAM producer from config
fn build_scram_producer(bootstrap: &str, username: &str, password: &str) -> FutureProducer {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "SCRAM-SHA-512")
        .set("sasl.username", username)
        .set("sasl.password", password)
        .set("compression.type", "snappy")
        .set("linger.ms", "5")
        .set("batch.num.messages", "10000")
        .set("queue.buffering.max.kbytes", "1048576")
        .set("message.timeout.ms", "60000")
        .set("acks", "1");
    cfg.create().expect("create producer")
}

pub async fn run_bench(
    name: &String,
    kcfg: &MSKConfig,
    connections: u16,
    payload_path: PathBuf,
    max_bytes: usize,
    seconds: u64,
) -> Result<()> {
    let payload = fs::read_to_string(&payload_path)
        .with_context(|| format!("failed to read payload file {}", payload_path.display()))?;
    let one_line = serde_json::to_string(&serde_json::from_str::<Value>(&payload)?)?;

    let mut line = Vec::with_capacity(one_line.len() + 1);
    line.extend_from_slice(one_line.as_bytes());
    line.push(b'\n');

    info!("===Starting {name} benchmark===");
    info!(
        "bootstrap={} topic={} payload={:?} bytes/line={} connections={}",
        kcfg.bootstrap_servers,
        kcfg.topic,
        payload_path,
        line.len(),
        connections,
    );

    let producer = match &kcfg.auth {
        MSKAuth::Scram {
            sasl_mechanism: _,
            username,
            password,
        } => build_scram_producer(&kcfg.bootstrap_servers, username, password.expose_secret()),
    };

    let topic = kcfg.topic.clone();
    let seconds = seconds;
    let max_bytes = max_bytes;

    let mut tasks = Vec::with_capacity(connections as usize);
    for _ in 0..connections {
        let p = producer.clone();
        let topic = topic.clone();
        let line_cl = line.clone();

        tasks.push(task::spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(seconds);

            let mut buf = BytesMut::with_capacity(max_bytes.max(line_cl.len()));
            let mut events_per_buf: u64 = 0;
            while max_bytes < line_cl.len() || buf.len() + line_cl.len() <= max_bytes {
                buf.extend_from_slice(&line_cl);
                events_per_buf += 1;
            }

            let mut total_events: u64 = 0;

            while Instant::now() < deadline {
                let rec = FutureRecord::<(), _>::to(&topic).payload(&buf[..]);
                match p.send(rec, Duration::from_secs(5)).await {
                    Ok(_) => total_events += events_per_buf,
                    Err((e, _msg)) => return Err(anyhow::anyhow!("produce error: {e}")),
                }
            }
            Ok::<u64, anyhow::Error>(total_events)
        }));
    }

    futures::future::try_join_all(tasks)
        .await?
        .into_iter()
        .try_fold(0u64, |acc, res| res.map(|v| acc + v))?;

    Ok(())
}
