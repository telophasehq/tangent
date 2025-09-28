use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    Message,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::worker::{Incoming, WorkerPool};
use tangent_shared::msk::{MSKAuth, MSKConfig};

pub async fn run_consumer(
    kc: MSKConfig,
    pool: std::sync::Arc<WorkerPool>,
    shutdown: CancellationToken,
) -> Result<()> {
    let consumer: StreamConsumer = build_consumer(&kc)?;
    consumer
        .subscribe(&[kc.topic.as_str()])
        .with_context(|| format!("subscribing to topic {}", kc.topic))?;

    let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(1024);

    let fwd_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let mut stream = consumer.stream();

        loop {
            tokio::select! {
                _ = fwd_shutdown.cancelled() => break,
                next = stream.next() => {
                    match next {
                        None => break,
                        Some(Err(e)) => {
                            if tx.send(Err(anyhow::Error::new(e))).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(msg)) => {
                            let b = match msg.payload() {
                                Some(p) => Bytes::copy_from_slice(p),
                                None => Bytes::new(),
                            };
                            if tx.send(Ok(b)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }
    });

    let record_stream = ReceiverStream::new(rx).map(|r| r);
    pool.dispatch(Incoming::Records(Box::pin(record_stream)))
        .await;
    shutdown.cancelled().await;
    Ok(())
}

pub fn build_consumer(kc: &MSKConfig) -> Result<StreamConsumer> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &kc.bootstrap_servers)
        .set("group.id", &kc.group_id)
        .set("enable.auto.commit", "true")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "latest")
        .set("security.protocol", kc.security_protocol.as_str());

    if let Some(p) = kc.ssl_ca_location.as_deref() {
        cfg.set("ssl.ca.location", p);
    }
    if let Some(p) = kc.ssl_certificate_location.as_deref() {
        cfg.set("ssl.certificate.location", p);
    }
    if let Some(p) = kc.ssl_key_location.as_deref() {
        cfg.set("ssl.key.location", p);
    }

    let mechanism = match &kc.auth {
        MSKAuth::Scram {
            sasl_mechanism,
            username,
            password,
        } => {
            cfg.set("sasl.mechanism", sasl_mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
            "SCRAM"
        }
    };

    cfg.set("fetch.max.bytes", "10485760");
    cfg.set("max.partition.fetch.bytes", "10485760");

    let consumer: StreamConsumer = cfg
        .create()
        .map_err(|e| anyhow!("creating StreamConsumer failed: {e:#?}"))?;

    tracing::info!("Kafka consumer built with {}", mechanism);
    Ok(consumer)
}
