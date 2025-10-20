use anyhow::{anyhow, Result};
use bytes::Bytes;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    ClientContext, Message,
};
use secrecy::ExposeSecret;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::worker::{Record, WorkerPool};
use rdkafka::message::Headers;
use tangent_shared::sources::msk::{MSKAuth, MSKConfig};

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

    for (_tname, t) in &s.topics {
        for (_pnum, p) in &t.partitions {
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
            let cg_state = s.cgrp.as_ref().map(|c| c.state.as_str()).unwrap_or("n/a");

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
    kc: MSKConfig,
    pool: Arc<WorkerPool>,
    shutdown: CancellationToken,
) -> Result<()> {
    let consumer: StreamConsumer<Ctx> = build_consumer(&kc)?;
    consumer.subscribe(&[kc.topic.as_str()])?;

    let fwd_shutdown = shutdown.clone();
    let dc = kc.decoding.clone();

    loop {
        tokio::select! {
            _ = fwd_shutdown.cancelled() => break,
            msg = consumer.recv() => {
                match msg {
                    Ok(m) => {
                        if let Some(p) = m.payload() {
                            let meta_ce   = header_str(&m, "content-encoding");
                            let filename  = header_str(&m, "filename");
                            let sniff     = &p[..std::cmp::min(8, p.len())];
                            let comp = dc.resolve_compression(meta_ce, filename, sniff);

                            let raw = decoding::decompress_bytes(comp, p)?;

                            let fmt = dc.resolve_format(&raw);

                            let ndjson = decoding::normalize_to_ndjson(fmt, &raw);

                            let _ = pool.dispatch(Record {
                                payload: Bytes::from(ndjson),
                                ack: None,
                            }).await;
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
    };

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
