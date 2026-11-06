use anyhow::{anyhow, Result};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    ClientContext, Message,
};
use secrecy::ExposeSecret;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::{router::Router, sources::decoding::normalize_to_ndjson};
use rdkafka::message::Headers;
use tangent_shared::{
    dag::NodeRef,
    sources::msk::{MSKAuth, MSKConfig},
};

use crate::sources::decoding;

#[derive(Default)]
struct StatState {
    prev_ts_us: i64,
    prev_rxmsgs: i64,
    prev_rxmsg_bytes: i64,
}

fn lag_from_stats(s: &rdkafka::statistics::Statistics) -> (i64, i64, usize) {
    let mut total_lag: i64 = 0;
    let mut max_lag: i64 = 0;
    let mut assigned_parts: usize = 0;

    for t in s.topics.values() {
        for p in t.partitions.values() {
            let lag = p.consumer_lag.max(0);
            total_lag += lag;
            if lag > max_lag {
                max_lag = lag;
            }
            assigned_parts += 1;
        }
    }
    (total_lag, max_lag, assigned_parts)
}

pub struct Ctx {
    state: Arc<Mutex<StatState>>,
}

impl ClientContext for Ctx {
    fn stats(&self, s: rdkafka::Statistics) {
        let mut st = self.state.lock().unwrap();

        if st.prev_ts_us != 0 && s.ts > st.prev_ts_us {
            let dt = (s.ts - st.prev_ts_us) as f64 / 1_000_000.0;
            let d_msgs = (s.rxmsgs - st.prev_rxmsgs) as f64;
            let d_bytes = (s.rxmsg_bytes - st.prev_rxmsg_bytes) as f64;

            let msgs_per_s = d_msgs / dt;
            let mib_per_s = (d_bytes / dt) / (1024.0 * 1024.0);

            let (total_lag, max_lag, assigned) = lag_from_stats(&s);
            let cg_state = s.cgrp.as_ref().map_or("n/a", |c| c.state.as_str());

            tracing::debug!(target:"kafka_stats",
                msgs_per_s = format_args!("{:.0}", msgs_per_s),
                mib_per_s  = format_args!("{:.2}", mib_per_s),
                cg_state   = %cg_state,
                assigned   = assigned,
                total_lag  = total_lag,
                max_lag    = max_lag,
                "kafka ingest"
            );
        }

        st.prev_ts_us = s.ts;
        st.prev_rxmsgs = s.rxmsgs;
        st.prev_rxmsg_bytes = s.rxmsg_bytes;
    }
}
impl ConsumerContext for Ctx {}

fn header_str<'a>(m: &'a rdkafka::message::BorrowedMessage<'a>, key: &str) -> Option<&'a str> {
    m.headers().and_then(|hs| {
        (0..hs.count()).find_map(|i| {
            let h = hs.get(i);
            if h.key == key {
                std::str::from_utf8(h.value?).ok()
            } else {
                None
            }
        })
    })
}

pub async fn run_consumer(
    name: Arc<str>,
    kc: MSKConfig,
    chunks: usize,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let consumer: StreamConsumer<Ctx> = build_consumer(&kc)?;
    consumer.subscribe(&[kc.topic.as_str()])?;

    let fwd_shutdown = shutdown.clone();
    let dc = kc.decoding.clone();
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };

    loop {
        tokio::select! {
            () = fwd_shutdown.cancelled() => break,
            msg = consumer.recv() => {
                match msg {
                    Ok(m) => {
                        if let Some(p) = m.payload() {
                            let meta_ce   = header_str(&m, "content-encoding");
                            let filename  = header_str(&m, "filename");
                            let sniff     = &p[..std::cmp::min(8, p.len())];
                            let comp = dc.resolve_compression(meta_ce, filename, sniff);

                            let ndjson = decoding::decompress_vec(&comp, p)
                                .and_then(|raw| normalize_to_ndjson(&kc.decoding.format, raw));
                            match ndjson {
                                Ok(mut ndjson) => {
                                    let frames_mut = decoding::chunk_ndjson(&mut ndjson, chunks);
                                    router.forward(&from, frames_mut, vec![]).await?;
                                }
                                Err(e) => {
                                    let what = format!("message {}/{}@{}", m.topic(), m.partition(), m.offset());
                                    decoding::skip_undecodable(&name, &what, &e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("kafka recv error: {e}");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn build_consumer(kc: &MSKConfig) -> Result<StreamConsumer<Ctx>> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &kc.bootstrap_servers)
        .set("group.id", &kc.group_id)
        .set("enable.auto.commit", "true")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "latest")
        .set("statistics.interval.ms", "1000")
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

    match &kc.auth {
        MSKAuth::Scram {
            sasl_mechanism,
            username,
            password,
        } => {
            cfg.set("sasl.mechanism", sasl_mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password.expose_secret());
        }
    }

    cfg.set("fetch.max.bytes", "10485760");
    cfg.set("max.partition.fetch.bytes", "10485760");

    let ctx = Ctx {
        state: Arc::new(Mutex::new(StatState::default())),
    };
    let consumer: StreamConsumer<Ctx> = cfg
        .create_with_context(ctx)
        .map_err(|e| anyhow!("creating StreamConsumer failed: {e:#?}"))?;

    Ok(consumer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use crate::SOURCE_UNDECODABLE_TOTAL;
    use async_trait::async_trait;
    use rdkafka::message::{Header, OwnedHeaders};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use std::time::Instant;
    use tangent_shared::sources::common::{DecodeCompression, DecodeFormat, Decoding};

    #[derive(Default)]
    struct Lake(tokio::sync::Mutex<Vec<String>>);

    #[async_trait]
    impl Sink for Lake {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            let write = String::from_utf8_lossy(&req.payload).into_owned();
            self.0.lock().await.push(write);
            Ok(())
        }
    }

    /// A message that does not decode, not being NDJSON or not being the
    /// gzip its header says, is counted and skipped, and the source reads
    /// on. It ended the source: tangent ran on with nothing from the topic.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_message_that_does_not_decode_is_skipped() {
        const TOPIC: &str = "audit";
        let name = "msk-undecodable";
        let kafka = MockCluster::new(1).unwrap();
        kafka.create_topic(TOPIC, 1, 1).unwrap();
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", kafka.bootstrap_servers())
            .create()
            .unwrap();

        let lake = Arc::new(Lake::default());
        let sinks: Vec<(Arc<str>, Arc<dyn Sink>)> =
            vec![(Arc::from("lake"), Arc::clone(&lake) as Arc<dyn Sink>)];
        let manager = Arc::new(SinkManager::for_test(sinks, 1));
        let mut outs = ahash::AHashMap::default();
        outs.insert(
            NodeRef::Source {
                name: Arc::from(name),
            },
            vec![NodeRef::Sink {
                name: Arc::from("lake"),
            }],
        );
        let router = Arc::new(Router::new(outs, manager));
        let kc = MSKConfig {
            bootstrap_servers: kafka.bootstrap_servers(),
            topic: TOPIC.into(),
            group_id: name.into(),
            security_protocol: "PLAINTEXT".into(),
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            auth: MSKAuth::Scram {
                sasl_mechanism: "SCRAM-SHA-512".into(),
                username: "tangent".into(),
                password: "unused".into(),
            },
            decoding: Decoding {
                format: DecodeFormat::Ndjson,
                compression: DecodeCompression::Auto,
            },
        };
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_consumer(
            Arc::from(name),
            kc,
            64,
            router,
            shutdown.clone(),
        ));

        // The source starts at the topic's end once it has joined its
        // group, so rounds are sent until two good records are in; between
        // any two good ones come both bad ones.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut round = 0;
        while lake.0.lock().await.len() < 2 {
            if task.is_finished() {
                panic!("the source ended: {:?}", task.await.unwrap());
            }
            assert!(Instant::now() < deadline, "no good record reached the sink");
            let good = format!("{{\"n\":{round}}}");
            let gzip = OwnedHeaders::new().insert(Header {
                key: "content-encoding",
                value: Some("gzip"),
            });
            for record in [
                FutureRecord::<(), str>::to(TOPIC)
                    .payload("{\"n\":\"not gzip\"}")
                    .headers(gzip),
                FutureRecord::to(TOPIC).payload("not ndjson"),
                FutureRecord::to(TOPIC).payload(good.as_str()),
            ] {
                producer.send(record, Duration::from_secs(5)).await.unwrap();
            }
            round += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        shutdown.cancel();
        task.await.unwrap().unwrap();
        for write in lake.0.lock().await.iter() {
            let v: serde_json::Value = serde_json::from_str(write).unwrap();
            assert!(v["n"].is_u64(), "only good records: {write}");
        }
        let skipped = SOURCE_UNDECODABLE_TOTAL.with_label_values(&[name]);
        assert!(
            skipped.get() >= 2,
            "both bad ones, at least: {}",
            skipped.get()
        );
    }
}
