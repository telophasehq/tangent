//! Inference nodes: a model reads each record. Each node owns one task and one
//! bounded channel; the router dispatches records to it the way it dispatches
//! to the worker pool, and the node forwards whatever it emits back through
//! the router under `NodeRef::Inference { name }`.

use ahash::AHashMap as HashMap;
use anyhow::{anyhow, bail, Result};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tangent_shared::inference::InferenceConfig;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::router::Router;
use crate::worker::Record;

mod node;

const CHANNEL_CAPACITY: usize = 4096;

pub struct InferenceNodes {
    senders: HashMap<Arc<str>, mpsc::Sender<Record>>,
    handles: Vec<JoinHandle<()>>,
}

impl InferenceNodes {
    pub fn new(
        cfgs: &BTreeMap<Arc<str>, InferenceConfig>,
        config_dir: &Path,
        router: Arc<Router>,
    ) -> Result<Self> {
        let mut senders = HashMap::with_capacity(cfgs.len());
        let mut handles = Vec::with_capacity(cfgs.len());
        for (name, cfg) in cfgs {
            let (tx, rx) = mpsc::channel::<Record>(CHANNEL_CAPACITY);
            senders.insert(Arc::clone(name), tx);
            let (name, cfg, dir, router) = (
                Arc::clone(name),
                cfg.clone(),
                config_dir.to_path_buf(),
                Arc::clone(&router),
            );
            handles.push(tokio::spawn(async move {
                let label = Arc::clone(&name);
                let res = node::run(name, cfg, &dir, rx, router).await;
                if let Err(e) = res {
                    tracing::error!(node = %label, "inference node exited: {e:#}");
                }
            }));
        }
        Ok(Self { senders, handles })
    }

    pub fn empty() -> Self {
        Self {
            senders: HashMap::default(),
            handles: Vec::new(),
        }
    }

    pub async fn dispatch(&self, name: &Arc<str>, rec: Record) -> Result<()> {
        let Some(tx) = self.senders.get(name) else {
            bail!("unknown inference node: {name}");
        };
        tx.send(rec)
            .await
            .map_err(|_| anyhow!("inference node {name} is no longer running"))
    }

    /// Close the inputs and wait for every node to finish what it has.
    pub async fn join(self) {
        let Self { senders, handles } = self;
        drop(senders);
        for h in handles {
            if let Err(e) = h.await {
                tracing::warn!("inference node task failed: {e}");
            }
        }
    }
}

/// Dot-path lookup into a JSON value: "a.b.c".
pub fn lookup<'a>(v: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    if let Some(direct) = v.get(path) {
        return Some(direct);
    }
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// A frame may carry several NDJSON lines (a plugin returns one blob per
/// batch); yield each non-empty line.
pub fn lines(payload: &[u8]) -> impl Iterator<Item = &[u8]> {
    payload
        .split(|b| *b == b'\n')
        .filter(|l| !l.iter().all(u8::is_ascii_whitespace))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use crate::worker::Payload;
    use async_trait::async_trait;
    use bytes::BytesMut;
    use serde_json::{json, Map, Value};
    use tangent_shared::dag::NodeRef;
    use tangent_shared::inference::{Answer, Backend, InferenceConfig, OnError};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        writes: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            self.writes
                .lock()
                .await
                .push(String::from_utf8_lossy(&req.payload).into_owned());
            Ok(())
        }
    }

    /// An OpenAI-compatible chat server in a few lines: it flags any request
    /// whose body mentions "bad", wraps the other verdict in a think block and
    /// a code fence, and records every request body.
    async fn stub_model() -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let log = Arc::clone(&log);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let (body_start, body_len) = loop {
                        let n = sock.read(&mut tmp).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                            let len = head
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:"))
                                .and_then(|v| v.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            break (pos + 4, len);
                        }
                    };
                    while buf.len() < body_start + body_len {
                        let n = sock.read(&mut tmp).await.unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    let body = String::from_utf8_lossy(&buf[body_start..]).into_owned();
                    let content = if body.contains("answer yes or no") {
                        if body.contains("bad") { "Yes." } else { "no" }.to_string()
                    } else if body.contains("bad") {
                        r#"{"flag": true, "one_line": "bad thing"}"#.to_string()
                    } else {
                        "<think>looks fine</think>\n```json\n{\"flag\": false, \"one_line\": \"fine\"}\n```"
                            .to_string()
                    };
                    log.lock().await.push(body);
                    let reply = json!({
                        "choices": [{"message": {"content": content}, "finish_reason": "stop"}]
                    })
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        reply.len(),
                        reply
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        (url, seen)
    }

    fn config(url: &str, on_error: OnError) -> InferenceConfig {
        let mut extra = Map::new();
        extra.insert(
            "chat_template_kwargs".into(),
            json!({"enable_thinking": false}),
        );
        InferenceConfig {
            backend: Backend::Openai,
            url: url.to_string(),
            model: "stub".into(),
            region: None,
            profile: None,
            prompt_file: "judge.md".into(),
            answer: Answer::Json,
            fields: Vec::new(),
            output: None,
            forward_if: Some("flag".into()),
            concurrency: 2,
            timeout_ms: 5_000,
            max_tokens: 50,
            temperature: 0.0,
            extra,
            api_key: None,
            on_error,
        }
    }

    /// Run `records` through one inference node into a recording sink.
    async fn run_pipeline(
        cfg: InferenceConfig,
        prompt: &str,
        records: &[&str],
    ) -> (Vec<String>, Vec<String>) {
        let dir = std::env::temp_dir().join(format!(
            "tangent-inference-{}-{}",
            std::process::id(),
            cfg.on_error as u8 + 2 * cfg.answer as u8
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("judge.md"), prompt).unwrap();

        let sink = Arc::new(RecordingSink::default());
        let sink_name: Arc<str> = Arc::from("out");
        let sink_manager = Arc::new(SinkManager::for_test(
            vec![(sink_name.clone(), sink.clone() as Arc<dyn Sink>)],
            1,
        ));
        let name: Arc<str> = Arc::from("judge");
        let mut outs: HashMap<NodeRef, Vec<NodeRef>> = HashMap::default();
        outs.insert(
            NodeRef::Inference { name: name.clone() },
            vec![NodeRef::Sink {
                name: sink_name,
                key_prefix: None,
            }],
        );
        let router = Arc::new(Router::new(outs, Arc::clone(&sink_manager)));
        let mut cfgs = BTreeMap::new();
        cfgs.insert(name.clone(), cfg);
        let pm = Arc::new(InferenceNodes::new(&cfgs, &dir, Arc::clone(&router)).unwrap());
        router.set_inference(&pm);

        for r in records {
            pm.dispatch(
                &name,
                Record {
                    payload: Payload::Raw(BytesMut::from(format!("{r}\n").as_str())),
                    ack: None,
                },
            )
            .await
            .unwrap();
        }
        drop(router);
        Arc::try_unwrap(pm).ok().expect("manager refs").join().await;
        Arc::try_unwrap(sink_manager)
            .ok()
            .expect("sink manager refs")
            .join()
            .await
            .unwrap();
        let _ = std::fs::remove_dir_all(dir);
        let writes = sink.writes.lock().await.clone();
        (writes, Vec::new())
    }

    #[tokio::test]
    async fn forwards_only_what_the_model_flags() {
        let (url, seen) = stub_model().await;
        let (writes, _) = run_pipeline(
            config(&url, OnError::Pass),
            "You are a test judge.\n",
            &[
                r#"{"id": 1, "msg": "all good"}"#,
                r#"{"id": 2, "msg": "bad login"}"#,
                r#"{"id": 3, "msg": "also fine"}"#,
            ],
        )
        .await;
        assert_eq!(writes.len(), 1, "{writes:?}");
        let out: Value = serde_json::from_str(writes[0].trim()).unwrap();
        assert_eq!(out["id"], 2);
        assert_eq!(out["_judge"]["flag"], true);
        assert_eq!(out["_judge"]["one_line"], "bad thing");

        let seen = seen.lock().await;
        assert_eq!(seen.len(), 3, "one request per record");
        for body in seen.iter() {
            let req: Value = serde_json::from_str(body).unwrap();
            assert_eq!(req["model"], "stub");
            assert_eq!(req["messages"][0]["role"], "system");
            // The system prompt, as a string or as content parts.
            assert!(
                req["messages"][0]["content"]
                    .to_string()
                    .contains("You are a test judge."),
                "{}",
                req["messages"][0]
            );
            assert_eq!(req["messages"][1]["role"], "user");
            assert_eq!(
                req["chat_template_kwargs"]["enable_thinking"], false,
                "extra is merged"
            );
            assert_eq!(req["max_tokens"], 50);
        }
    }

    #[tokio::test]
    async fn a_yes_no_filter_forwards_only_the_yeses() {
        let (url, seen) = stub_model().await;
        let mut cfg = config(&url, OnError::Pass);
        cfg.answer = Answer::YesNo;
        cfg.forward_if = None;
        let (writes, _) = run_pipeline(
            cfg,
            "Should someone look? answer yes or no.\n",
            &[
                r#"{"id": 1, "msg": "all good"}"#,
                r#"{"id": 2, "msg": "bad login"}"#,
                r#"{"id": 3, "msg": "also fine"}"#,
            ],
        )
        .await;
        assert_eq!(writes.len(), 1, "{writes:?}");
        let out: Value = serde_json::from_str(writes[0].trim()).unwrap();
        assert_eq!(out["id"], 2);
        assert_eq!(out["_judge"]["answer"], "yes");
        assert_eq!(seen.lock().await.len(), 3, "every record was asked about");
    }

    #[tokio::test]
    async fn an_unreachable_model_passes_or_drops_as_configured() {
        // Nothing listens on port 1.
        let dead = "http://127.0.0.1:1/v1";
        let (writes, _) = run_pipeline(
            config(dead, OnError::Pass),
            "You are a test judge.\n",
            &[r#"{"id": 9}"#],
        )
        .await;
        assert_eq!(writes.len(), 1, "{writes:?}");
        let out: Value = serde_json::from_str(writes[0].trim()).unwrap();
        assert_eq!(out["id"], 9);
        assert!(
            !out["_judge"]["error"].as_str().unwrap().is_empty(),
            "{out}"
        );

        let (writes, _) = run_pipeline(
            config(dead, OnError::Drop),
            "You are a test judge.\n",
            &[r#"{"id": 9}"#],
        )
        .await;
        assert!(writes.is_empty(), "{writes:?}");
    }
}
