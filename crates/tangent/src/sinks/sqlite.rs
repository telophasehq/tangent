use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use parking_lot::Mutex;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tangent_shared::sinks::sqlite::SqliteConfig;

use crate::inference::lines;
use crate::partition;
use crate::sinks::manager::{Sink, SinkWrite};
use crate::state::now_ms;
use crate::wasm::host::JsonLogView;
use crate::{SINK_BYTES_TOTAL, SINK_BYTES_UNCOMPRESSED_TOTAL, SINK_OBJECTS_TOTAL};

pub struct SqliteSink {
    conn: Mutex<Connection>,
    insert: String,
    time: Option<String>,
    partitions: Vec<Vec<String>>,
}

impl SqliteSink {
    pub fn new(cfg: &SqliteConfig) -> Result<Arc<Self>> {
        let names: Vec<String> = cfg.partitions.keys().cloned().collect();
        let conn = open(&cfg.path, &names)?;
        let cols: String = names.iter().map(|n| format!(", \"{n}\"")).collect();
        let params: String = (0..names.len()).map(|i| format!(", ?{}", i + 3)).collect();
        Ok(Arc::new(Self {
            conn: Mutex::new(conn),
            insert: format!("INSERT INTO logs (at, record{cols}) VALUES (?1, ?2{params})"),
            time: cfg.time.clone(),
            partitions: cfg.partitions.values().cloned().collect(),
        }))
    }
}

/// The logs table, with a column and an index per partition.
pub fn open(path: &Path, partitions: &[String]) -> Result<Connection> {
    if let Some(bad) = partitions
        .iter()
        .find(|p| p.is_empty() || !p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        bail!("partition name {bad:?}: letters, digits and _ only");
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY,
            at INTEGER NOT NULL,
            record TEXT NOT NULL,
            emitted INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS logs_emitted ON logs(emitted, at);",
    )?;
    let have: Vec<String> = conn
        .prepare("SELECT name FROM pragma_table_info('logs')")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for p in partitions {
        if !have.contains(p) {
            conn.execute_batch(&format!("ALTER TABLE logs ADD COLUMN \"{p}\" TEXT"))?;
        }
        conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS \"logs_{p}\" ON logs(\"{p}\", at)"
        ))?;
    }
    Ok(conn)
}

#[async_trait]
impl Sink for SqliteSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        let now = now_ms() as i64;
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for line in lines(&req.payload) {
            let lv = match JsonLogView::from_bytes(BytesMut::from(line)) {
                Ok(lv) => lv,
                Err(e) => {
                    tracing::warn!("sqlite sink: record is not JSON; dropping: {e}");
                    continue;
                }
            };
            // a time ahead of the clock would make every partition look due
            let at = partition::time_of(&lv, self.time.as_deref()).map_or(now, |t| t.min(now));
            let mut row = vec![
                Value::Integer(at),
                Value::Text(String::from_utf8_lossy(line).into_owned()),
            ];
            for fields in &self.partitions {
                let key: Vec<String> = fields
                    .iter()
                    .map(|f| {
                        lv.lookup(f)
                            .and_then(JsonLogView::to_scalar)
                            .map(partition::scalar_text)
                            .unwrap_or_default()
                    })
                    .collect();
                row.push(Value::Text(key.join("|")));
            }
            rows.push(row);
        }

        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        {
            let mut st = tx.prepare_cached(&self.insert)?;
            for row in rows {
                st.execute(params_from_iter(row))?;
            }
        }
        tx.commit()?;

        SINK_OBJECTS_TOTAL.inc();
        SINK_BYTES_TOTAL.inc_by(req.payload.len() as u64);
        SINK_BYTES_UNCOMPRESSED_TOTAL.inc_by(req.payload.len() as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tangent-sqlite-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn write(payload: &str) -> SinkWrite {
        SinkWrite {
            sink_name: Arc::from("logs"),
            payload: BytesMut::from(payload),
            s3: None,
        }
    }

    #[tokio::test]
    async fn stores_a_record_once_with_its_partitions() {
        let dir = dir("once");
        let mut partitions = BTreeMap::new();
        partitions.insert("org".to_string(), vec!["_norm.org".to_string()]);
        partitions.insert(
            "actor".to_string(),
            vec!["_norm.org".to_string(), "_norm.actor".to_string()],
        );
        let sink = SqliteSink::new(&SqliteConfig {
            path: dir.join("logs.sqlite"),
            time: Some("ts".into()),
            partitions,
        })
        .unwrap();
        let first = r#"{"ts":"2026-10-20T13:00:00Z","_norm":{"org":"acme","actor":"ann"}}"#;
        sink.write(write(&format!(
            "{first}\n{{\"_norm\":{{\"actor\":\"bob\"}}}}\n{{}}\n"
        )))
        .await
        .unwrap();

        let conn = Connection::open(dir.join("logs.sqlite")).unwrap();
        let rows: Vec<(i64, String, String, String)> = conn
            .prepare("SELECT at, record, org, actor FROM logs ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, 1_792_501_200_000);
        assert_eq!(rows[0].1, first);
        assert_eq!(
            (rows[0].2.as_str(), rows[0].3.as_str()),
            ("acme", "acme|ann")
        );
        assert_eq!((rows[1].2.as_str(), rows[1].3.as_str()), ("", "|bob"));
        assert_eq!((rows[2].2.as_str(), rows[2].3.as_str()), ("", "|"));
        assert!(rows[1].0 > rows[0].0, "arrival time without a ts");
        let _ = std::fs::remove_dir_all(dir);
    }
}
