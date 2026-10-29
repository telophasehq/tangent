//! Durable state in one SQLite file, WAL mode, shared by every worker through
//! a mutex. The runtime holds an exclusive file lock while it runs; readers
//! (`tangent state ...`) open the same file read-only without the lock. Calls
//! are microseconds of local I/O, so they run inline on the async executor.
//!
//! Schema: `state(ns, partition, key, kind, value, expires_at, updated_at)`.

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use fs2::FileExt;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rusqlite::types::Value;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::fs::{create_dir_all, OpenOptions};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tangent_shared::runtime::StateConfig;
use tracing::info;

use super::{expires_at, now_ms, sqlite_path, Count, Entry, StateStore, StateValue};

static OPEN_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS state (
    ns         TEXT NOT NULL,
    partition  TEXT NOT NULL DEFAULT '',
    key        TEXT NOT NULL,
    kind       TEXT NOT NULL,
    value      BLOB NOT NULL,
    expires_at INTEGER,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (ns, partition, key)
);
CREATE INDEX IF NOT EXISTS state_expires_idx ON state(expires_at);
";

const UPSERT: &str = "INSERT INTO state (ns, partition, key, kind, value, expires_at, updated_at)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
     ON CONFLICT (ns, partition, key) DO UPDATE SET
        kind = excluded.kind, value = excluded.value,
        expires_at = excluded.expires_at, updated_at = excluded.updated_at";

pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
    _lock: Option<Arc<std::fs::File>>,
    cfg: StateConfig,
}

impl SqliteStore {
    /// The runtime's handle: exclusive, creates the file and schema.
    pub fn open(cfg: &StateConfig, base_dir: &Path) -> Result<Self> {
        let _open_guard = OPEN_GUARD.lock();
        let path = sqlite_path(cfg, base_dir);
        if let Some(parent) = path.parent() {
            create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        let lock = acquire_lock(&path, Duration::from_millis(cfg.lock_timeout_ms))?;
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .with_context(|| format!("opening state db at {}", path.display()))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "wal_autocheckpoint", 1000i64)?;
        conn.execute_batch(SCHEMA)
            .context("creating state schema")?;
        info!(target = "state", path = %path.display(), "state db initialized");
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _lock: Some(Arc::new(lock)),
            cfg: cfg.clone(),
        })
    }

    /// A reader's handle: no lock, no writes, nothing created.
    pub fn open_readonly(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .with_context(|| format!("opening state db at {} read-only", path.display()))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _lock: None,
            cfg: StateConfig::default(),
        })
    }

    /// Run a read-only SQL statement and return the rows as JSON objects.
    pub fn sql(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        let conn = self.conn.lock();
        let mut st = conn.prepare(query)?;
        if !st.readonly() {
            bail!("only read-only statements are allowed here");
        }
        let names: Vec<String> = st.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = st.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let mut m = serde_json::Map::new();
            for (i, name) in names.iter().enumerate() {
                let v: Value = row.get(i)?;
                m.insert(name.clone(), sql_to_json(v));
            }
            out.push(m);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

fn sql_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(i) => i.into(),
        Value::Real(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => s.into(),
        Value::Blob(b) => match std::str::from_utf8(&b) {
            Ok(s) => s.into(),
            Err(_) => format!("<{} bytes>", b.len()).into(),
        },
    }
}

fn to_sqlite(v: &StateValue) -> Result<(&'static str, Value)> {
    Ok(match v {
        StateValue::Str(s) => ("str", Value::Text(s.clone())),
        StateValue::Int(i) => ("int", Value::Integer(*i)),
        StateValue::Float(f) => ("float", Value::Real(*f)),
        StateValue::Bool(b) => ("bool", Value::Integer(i64::from(*b))),
        StateValue::Bytes(b) => ("bytes", Value::Blob(b.clone())),
        StateValue::Json(j) => ("json", Value::Text(serde_json::to_string(j)?)),
    })
}

fn from_sqlite(kind: &str, v: Value) -> Result<StateValue> {
    use Value as V;
    Ok(match (kind, v) {
        ("str", V::Text(s)) => StateValue::Str(s),
        ("int", V::Integer(i)) => StateValue::Int(i),
        ("float", V::Real(f)) => StateValue::Float(f),
        ("bool", V::Integer(i)) => StateValue::Bool(i != 0),
        ("bytes", V::Blob(b)) => StateValue::Bytes(b),
        ("json", V::Text(s)) => StateValue::Json(serde_json::from_str(&s)?),
        (k, v) => bail!("mismatched SQLite value for kind={k}: {v:?}"),
    })
}

fn row_entry(r: &rusqlite::Row<'_>) -> rusqlite::Result<Entry> {
    let kind: String = r.get(3)?;
    let raw: Value = r.get(4)?;
    let expires_at: Option<i64> = r.get(5)?;
    Ok(Entry {
        ns: r.get(0)?,
        partition: r.get(1)?,
        key: r.get(2)?,
        value: from_sqlite(&kind, raw).unwrap_or(StateValue::Str("<unreadable>".into())),
        expires_at_ms: expires_at.map(|e| e as u64),
        updated_at_ms: r.get::<_, i64>(6)? as u64,
    })
}

#[async_trait]
impl StateStore for SqliteStore {
    async fn get(&self, ns: &str, partition: &str, key: &str) -> Result<Option<StateValue>> {
        let now = now_ms() as i64;
        let conn = self.conn.lock();
        let found = conn
            .prepare_cached(
                "SELECT kind, value, expires_at FROM state WHERE ns = ?1 AND partition = ?2 AND key = ?3",
            )?
            .query_row(params![ns, partition, key], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Value>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                ))
            })
            .optional()?;
        match found {
            None => Ok(None),
            Some((_, _, Some(exp))) if exp <= now => {
                conn.execute(
                    "DELETE FROM state WHERE ns = ?1 AND partition = ?2 AND key = ?3",
                    params![ns, partition, key],
                )?;
                Ok(None)
            }
            Some((kind, raw, _)) => Ok(Some(from_sqlite(&kind, raw)?)),
        }
    }

    async fn set(
        &self,
        ns: &str,
        partition: &str,
        key: &str,
        value: &StateValue,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let now = now_ms();
        let (kind, raw) = to_sqlite(value)?;
        let expires_at = expires_at(&self.cfg, ttl_ms, now).map(|e| e as i64);
        self.conn.lock().execute(
            UPSERT,
            params![ns, partition, key, kind, raw, expires_at, now as i64],
        )?;
        Ok(())
    }

    /// In one transaction: a failure anywhere writes none of them.
    async fn set_many(
        &self,
        entries: &[(String, String, String, StateValue)],
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let now = now_ms();
        let expires_at = expires_at(&self.cfg, ttl_ms, now).map(|e| e as i64);
        let mut guard = self.conn.lock();
        let tx = guard.transaction()?;
        {
            let mut st = tx.prepare_cached(UPSERT)?;
            for (ns, partition, key, value) in entries {
                let (kind, raw) = to_sqlite(value)?;
                st.execute(params![ns, partition, key, kind, raw, expires_at, now as i64])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    async fn del(&self, ns: &str, partition: &str, key: &str) -> Result<bool> {
        let n = self.conn.lock().execute(
            "DELETE FROM state WHERE ns = ?1 AND partition = ?2 AND key = ?3",
            params![ns, partition, key],
        )?;
        Ok(n > 0)
    }

    async fn incr(
        &self,
        ns: &str,
        partition: &str,
        key: &str,
        by: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64> {
        let now = now_ms();
        let expires_at = expires_at(&self.cfg, ttl_ms, now).map(|e| e as i64);
        let mut guard = self.conn.lock();
        let tx = guard.transaction()?;
        let current: Option<(String, Value, Option<i64>)> = tx
            .query_row(
                "SELECT kind, value, expires_at FROM state WHERE ns = ?1 AND partition = ?2 AND key = ?3",
                params![ns, partition, key],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let base = match current {
            Some((_, _, Some(exp))) if exp <= now as i64 => 0,
            Some((kind, raw, _)) => match from_sqlite(&kind, raw)? {
                StateValue::Int(i) => i,
                other => bail!("incr on a non-integer value: {other:?}"),
            },
            None => 0,
        };
        let next = base.saturating_add(by);
        tx.execute(
            "INSERT INTO state (ns, partition, key, kind, value, expires_at, updated_at)
             VALUES (?1, ?2, ?3, 'int', ?4, ?5, ?6)
             ON CONFLICT (ns, partition, key) DO UPDATE SET
                kind = 'int', value = excluded.value,
                expires_at = excluded.expires_at, updated_at = excluded.updated_at",
            params![
                ns,
                partition,
                key,
                Value::Integer(next),
                expires_at,
                now as i64
            ],
        )?;
        tx.commit()?;
        Ok(next)
    }

    async fn scan(
        &self,
        ns: Option<&str>,
        partition: Option<&str>,
        key_prefix: &str,
        limit: usize,
    ) -> Result<Vec<Entry>> {
        let now = now_ms() as i64;
        let conn = self.conn.lock();
        let mut st = conn.prepare_cached(
            "SELECT ns, partition, key, kind, value, expires_at, updated_at FROM state
             WHERE (?1 IS NULL OR ns = ?1) AND (?2 IS NULL OR partition = ?2)
               AND substr(key, 1, length(?3)) = ?3
               AND (expires_at IS NULL OR expires_at > ?4)
             ORDER BY ns, partition, key LIMIT ?5",
        )?;
        let rows = st.query_map(
            params![ns, partition, key_prefix, now, limit as i64],
            row_entry,
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    async fn range(
        &self,
        ns: &str,
        partition: &str,
        from: &str,
        to: &str,
        limit: usize,
    ) -> Result<Vec<Entry>> {
        let now = now_ms() as i64;
        let conn = self.conn.lock();
        // Walks the primary key (ns, partition, key): one index range.
        let mut st = conn.prepare_cached(
            "SELECT ns, partition, key, kind, value, expires_at, updated_at FROM state
             WHERE ns = ?1 AND partition = ?2 AND key >= ?3 AND key < ?4
               AND (expires_at IS NULL OR expires_at > ?5)
             ORDER BY key LIMIT ?6",
        )?;
        let rows = st.query_map(
            params![ns, partition, from, to, now, limit as i64],
            row_entry,
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    async fn range_rev(
        &self,
        ns: &str,
        partition: &str,
        from: &str,
        to: &str,
        limit: usize,
    ) -> Result<Vec<Entry>> {
        let now = now_ms() as i64;
        let conn = self.conn.lock();
        // The same index range, walked backwards.
        let mut st = conn.prepare_cached(
            "SELECT ns, partition, key, kind, value, expires_at, updated_at FROM state
             WHERE ns = ?1 AND partition = ?2 AND key >= ?3 AND key < ?4
               AND (expires_at IS NULL OR expires_at > ?5)
             ORDER BY key DESC LIMIT ?6",
        )?;
        let rows = st.query_map(
            params![ns, partition, from, to, now, limit as i64],
            row_entry,
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    async fn counts(&self, ns: Option<&str>) -> Result<Vec<Count>> {
        let now = now_ms() as i64;
        let conn = self.conn.lock();
        let mut st = conn.prepare_cached(
            "SELECT ns, partition, count(*) FROM state
             WHERE (?1 IS NULL OR ns = ?1) AND (expires_at IS NULL OR expires_at > ?2)
             GROUP BY ns, partition ORDER BY ns, partition",
        )?;
        let rows = st.query_map(params![ns, now], |r| {
            Ok(Count {
                ns: r.get(0)?,
                partition: r.get(1)?,
                keys: r.get::<_, i64>(2)? as u64,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    async fn purge_expired(&self) -> Result<u64> {
        let now = now_ms() as i64;
        let n = self.conn.lock().execute(
            "DELETE FROM state WHERE expires_at IS NOT NULL AND expires_at <= ?1",
            [now],
        )?;
        Ok(n as u64)
    }

    async fn reset(&self) -> Result<()> {
        self.conn.lock().execute("DELETE FROM state", [])?;
        Ok(())
    }
}

fn acquire_lock(path: &Path, timeout: Duration) -> Result<std::fs::File> {
    let mut lock_path = path.to_path_buf();
    lock_path.set_extension("sqlite.lock");
    let start = std::time::Instant::now();
    loop {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("opening state lock file {}", lock_path.display()))?;
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(e) => {
                if start.elapsed() >= timeout {
                    return Err(anyhow!(
                        "failed to acquire state lock {} (is another tangent running?): {e}",
                        lock_path.display()
                    ));
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("tangent-state-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn conforms() {
        let dir = temp_dir("conform");
        let store = SqliteStore::open(&StateConfig::default(), &dir).unwrap();
        crate::state::conformance::run(&store).await;
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Several sets are all or none: one that fails takes the rest with it.
    #[tokio::test]
    async fn set_many_writes_all_or_none() {
        let dir = temp_dir("many");
        let store = SqliteStore::open(&StateConfig::default(), &dir).unwrap();
        // Another handle makes one key fail, as a full disk would.
        Connection::open(sqlite_path(&StateConfig::default(), &dir))
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER refuse BEFORE INSERT ON state WHEN NEW.key = 'bad'
                 BEGIN SELECT RAISE(ABORT, 'disk full'); END;",
            )
            .unwrap();
        let entry = |k: &str| ("ns".to_string(), "p".to_string(), k.to_string(), StateValue::Int(1));
        assert!(store.set_many(&[entry("good"), entry("bad")], None).await.is_err());
        assert_eq!(store.get("ns", "p", "good").await.unwrap(), None, "written first, rolled back");
        store.set_many(&[entry("good")], None).await.unwrap();
        assert_eq!(store.get("ns", "p", "good").await.unwrap(), Some(StateValue::Int(1)));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn readers_see_the_runtime_writes_without_the_lock() {
        let dir = temp_dir("reader");
        let store = SqliteStore::open(&StateConfig::default(), &dir).unwrap();
        store
            .set("detect", "org-1", "seen", &StateValue::Int(3), Some(0))
            .await
            .unwrap();
        let path = sqlite_path(&StateConfig::default(), &dir);
        let reader = SqliteStore::open_readonly(&path).unwrap();
        assert_eq!(
            reader.get("detect", "org-1", "seen").await.unwrap(),
            Some(StateValue::Int(3))
        );
        let rows = reader
            .sql("SELECT ns, partition, key FROM state ORDER BY key", 10)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["partition"], "org-1");
        assert!(
            reader.sql("DELETE FROM state", 10).is_err(),
            "writes are refused"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
