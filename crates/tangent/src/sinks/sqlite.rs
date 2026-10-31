use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use parking_lot::Mutex;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};
use std::collections::{BTreeMap, BTreeSet};
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
    timestamp_key: Option<String>,
    fields: Vec<String>,
}

impl SqliteSink {
    pub fn new(cfg: &SqliteConfig) -> Result<Arc<Self>> {
        let conn = open(&cfg.path, &cfg.table, &cfg.partitions)?;
        let fields = fields(&cfg.partitions);
        let cols: String = fields.iter().map(|f| format!(", {}", quote(f))).collect();
        let params: String = (0..fields.len()).map(|i| format!(", ?{}", i + 3)).collect();
        Ok(Arc::new(Self {
            conn: Mutex::new(conn),
            insert: format!(
                "INSERT INTO \"{}\" (at, record{cols}) VALUES (?1, ?2{params})",
                cfg.table
            ),
            timestamp_key: cfg.timestamp_key.clone(),
            fields,
        }))
    }
}

/// The table, a column per partition field, and per partition an index on its fields then at.
pub fn open(
    path: &Path,
    table: &str,
    partitions: &BTreeMap<String, Vec<String>>,
) -> Result<Connection> {
    if !plain(table) {
        bail!("table name {table:?}: letters, digits and _ only");
    }
    if let Some(bad) = partitions.keys().find(|p| !plain(p)) {
        bail!("partition name {bad:?}: letters, digits and _ only");
    }
    let mut seen = BTreeSet::from(["pending", "at"].map(String::from));
    if let Some(p) = partitions
        .keys()
        .find(|p| !seen.insert(p.to_ascii_lowercase()))
    {
        bail!("partition name {p:?} is taken by another index of {table}; names ignore case");
    }
    let fields = fields(partitions);
    let mut seen = BTreeSet::from(["id", "at", "record", "emitted"].map(String::from));
    if let Some(f) = fields.iter().find(|f| !seen.insert(f.to_ascii_lowercase())) {
        bail!("partition field {f:?} is a column of {table} already; names ignore case");
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS \"{table}\" (
            id INTEGER PRIMARY KEY,
            at INTEGER NOT NULL,
            record TEXT NOT NULL,
            emitted INTEGER
        );
        CREATE INDEX IF NOT EXISTS \"{table}_pending\" ON \"{table}\"(at) WHERE emitted IS NULL;
        CREATE INDEX IF NOT EXISTS \"{table}_at\" ON \"{table}\"(at);"
    ))?;
    let have: Vec<String> = conn
        .prepare("SELECT name FROM pragma_table_info(?1)")?
        .query_map([table], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for f in &fields {
        if !have.iter().any(|h| h.eq_ignore_ascii_case(f)) {
            conn.execute_batch(&format!(
                "ALTER TABLE \"{table}\" ADD COLUMN {} TEXT",
                quote(f)
            ))?;
        }
    }
    for (p, cols) in partitions {
        let index = format!("{table}_{p}");
        let mut want = cols.clone();
        want.push("at".into());
        if index_columns(&conn, &index)? != want {
            let on: String = cols.iter().map(|f| quote(f) + ", ").collect();
            conn.execute_batch(&format!(
                "BEGIN;
                DROP INDEX IF EXISTS \"{index}\";
                CREATE INDEX \"{index}\" ON \"{table}\"({on}at);
                COMMIT;"
            ))?;
        }
    }
    Ok(conn)
}

/// An index's columns in order; none without the index.
pub fn index_columns(conn: &Connection, index: &str) -> Result<Vec<String>> {
    Ok(conn
        .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")?
        .query_map([index], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn fields(partitions: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let set: BTreeSet<&String> = partitions.values().flatten().collect();
    set.into_iter().cloned().collect()
}

fn plain(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
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
            let at =
                partition::time_of(&lv, self.timestamp_key.as_deref()).map_or(now, |t| t.min(now));
            let mut row = vec![
                Value::Integer(at),
                Value::Text(String::from_utf8_lossy(line).into_owned()),
            ];
            for f in &self.fields {
                row.push(
                    lv.lookup(f)
                        .and_then(JsonLogView::to_scalar)
                        .map_or(Value::Null, |s| Value::Text(partition::scalar_text(s))),
                );
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
    async fn stores_a_record_once_with_its_fields() {
        let dir = dir("once");
        let mut partitions = BTreeMap::new();
        partitions.insert("org".to_string(), vec!["_norm.org".to_string()]);
        partitions.insert(
            "actor".to_string(),
            vec!["_norm.org".to_string(), "_norm.actor".to_string()],
        );
        let mut cfg = SqliteConfig {
            path: dir.join("logs.sqlite"),
            table: "logs".into(),
            timestamp_key: Some("ts".into()),
            partitions,
        };
        let sink = SqliteSink::new(&cfg).unwrap();
        let first = r#"{"ts":"2026-10-20T13:00:00Z","_norm":{"org":"acme","actor":"ann"}}"#;
        sink.write(write(&format!(
            "{first}\n{{\"_norm\":{{\"actor\":\"bob\"}}}}\n{{}}\n"
        )))
        .await
        .unwrap();

        let conn = Connection::open(dir.join("logs.sqlite")).unwrap();
        let rows: Vec<(i64, Option<String>, Option<String>)> = conn
            .prepare("SELECT at, \"_norm.org\", \"_norm.actor\" FROM logs ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let fields: Vec<_> = rows
            .iter()
            .map(|r| (r.1.as_deref(), r.2.as_deref()))
            .collect();
        assert_eq!(
            fields,
            [
                (Some("acme"), Some("ann")),
                (None, Some("bob")),
                (None, None)
            ]
        );
        assert_eq!(rows[0].0, 1_792_501_200_000);
        assert!(rows[1].0 > rows[0].0, "arrival time without a ts");
        let record: String = conn
            .query_row("SELECT record FROM logs WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(record, first);
        let pending: i64 = conn
            .query_row("SELECT count(*) FROM logs WHERE emitted IS NULL", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(pending, 3);
        let cols: i64 = conn
            .query_row("SELECT count(*) FROM pragma_table_info('logs')", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(cols, 6, "one column per field");
        assert_eq!(
            index_columns(&conn, "logs_actor").unwrap(),
            ["_norm.org", "_norm.actor", "at"]
        );
        assert_eq!(
            index_columns(&conn, "logs_org").unwrap(),
            ["_norm.org", "at"]
        );

        cfg.partitions
            .insert("actor".to_string(), vec!["_norm.actor".to_string()]);
        SqliteSink::new(&cfg).unwrap();
        assert_eq!(
            index_columns(&conn, "logs_actor").unwrap(),
            ["_norm.actor", "at"]
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn table_and_partition_names_are_plain() {
        let path = std::env::temp_dir().join("tangent-never.sqlite");
        let none = BTreeMap::new();
        assert!(open(&path, "logs\"", &none).is_err());
        assert!(open(&path, "", &none).is_err());
        for p in ["a b", "pending", "Pending", "AT"] {
            let partitions = BTreeMap::from([(p.to_string(), vec!["x".to_string()])]);
            assert!(open(&path, "logs", &partitions).is_err());
        }
        let x = vec!["x".to_string()];
        let partitions = BTreeMap::from([("a".to_string(), x.clone()), ("A".to_string(), x)]);
        assert!(open(&path, "logs", &partitions).is_err());
        for f in [vec!["at"], vec!["ID"], vec!["x", "X"]] {
            let fields = f.into_iter().map(String::from).collect();
            let partitions = BTreeMap::from([("p".to_string(), fields)]);
            assert!(open(&path, "logs", &partitions).is_err());
        }
        assert!(!path.exists());
    }
}
