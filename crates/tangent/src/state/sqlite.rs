//! Durable state in one SQLite file, WAL mode, shared by every worker through
//! one connection, whose calls take turns. The runtime holds an exclusive file lock while it runs; readers
//! (`tangent state ...`, a pipeline with `state.read_only`) open the same file
//! read-only without the lock. Calls are microseconds of local I/O, so they
//! run inline on the async executor.
//!
//! Schema: `state(ns, key, kind, value, expires_at, updated_at)`.

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
use tokio::sync::OwnedMutexGuard;
use tracing::info;

use super::{expires_at, now_ms, sqlite_path, Count, Entry, StateStore, StateValue, Txn};

static OPEN_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Expired rows a purge deletes per turn: about 20 ms on a table of a
/// million rows, as long as a call that comes in meanwhile waits on it.
const PURGE_BATCH: usize = 1000;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS state (
    ns         TEXT NOT NULL,
    key        TEXT NOT NULL,
    kind       TEXT NOT NULL,
    value      BLOB NOT NULL,
    expires_at INTEGER,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (ns, key)
);
CREATE INDEX IF NOT EXISTS state_expires_idx ON state(expires_at);
";

pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
    /// Calls take turns: one at a time, and a transaction's for as long as
    /// it is open, so no other call's statements land inside it.
    turn: Arc<tokio::sync::Mutex<()>>,
    /// None: a reader's handle.
    lock: Option<Arc<std::fs::File>>,
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
            turn: Default::default(),
            lock: Some(Arc::new(lock)),
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
        let tables: i64 = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'state'",
            [],
            |r| r.get(0),
        )?;
        if tables == 0 {
            bail!(
                "{} holds no state yet (no `state` table): its pipeline has not opened it",
                path.display()
            );
        }
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            turn: Default::default(),
            lock: None,
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
    let kind: String = r.get(2)?;
    let raw: Value = r.get(3)?;
    let expires_at: Option<i64> = r.get(4)?;
    Ok(Entry {
        ns: r.get(0)?,
        key: r.get(1)?,
        value: from_sqlite(&kind, raw).unwrap_or(StateValue::Str("<unreadable>".into())),
        expires_at_ms: expires_at.map(|e| e as u64),
        updated_at_ms: r.get::<_, i64>(5)? as u64,
    })
}

#[async_trait]
impl StateStore for SqliteStore {
    async fn begin(&self) -> Result<Box<dyn Txn>> {
        let turn = Arc::clone(&self.turn).lock_owned().await;
        let writer = self.lock.is_some();
        // A writer takes SQLite's write lock at once, so nothing outside the
        // process slips a write between its reads and its writes; a reader
        // reads one snapshot.
        self.conn
            .lock()
            .execute_batch(if writer { "BEGIN IMMEDIATE" } else { "BEGIN" })?;
        Ok(Box::new(SqliteTxn {
            conn: Arc::clone(&self.conn),
            cfg: self.cfg.clone(),
            writer,
            open: true,
            _turn: turn,
        }))
    }

    async fn scan(&self, ns: Option<&str>, key_prefix: &str, limit: usize) -> Result<Vec<Entry>> {
        let now = now_ms() as i64;
        let _turn = self.turn.lock().await;
        let conn = self.conn.lock();
        let mut st = conn.prepare_cached(
            "SELECT ns, key, kind, value, expires_at, updated_at FROM state
             WHERE (?1 IS NULL OR ns = ?1)
               AND substr(key, 1, length(?2)) = ?2
               AND (expires_at IS NULL OR expires_at > ?3)
             ORDER BY ns, key LIMIT ?4",
        )?;
        let rows = st.query_map(params![ns, key_prefix, now, limit as i64], row_entry)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    async fn counts(&self, ns: Option<&str>) -> Result<Vec<Count>> {
        let now = now_ms() as i64;
        let _turn = self.turn.lock().await;
        let conn = self.conn.lock();
        let mut st = conn.prepare_cached(
            "SELECT ns, count(*) FROM state
             WHERE (?1 IS NULL OR ns = ?1) AND (expires_at IS NULL OR expires_at > ?2)
             GROUP BY ns ORDER BY ns",
        )?;
        let rows = st.query_map(params![ns, now], |r| {
            Ok(Count {
                ns: r.get(0)?,
                keys: r.get::<_, i64>(1)? as u64,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    async fn purge_expired(&self) -> Result<u64> {
        // A reader leaves the rows to the writer, as `get` does.
        if self.lock.is_none() {
            return Ok(0);
        }
        let mut purged = 0;
        loop {
            let (n, took) = {
                let _turn = self.turn.lock().await;
                let started = std::time::Instant::now();
                // The clock now, which a read judges a row by: rows land
                // between batches, with a TTL from a clock that may have
                // stepped back since the purge began.
                let now = now_ms() as i64;
                let n = self.conn.lock().execute(
                    "DELETE FROM state WHERE rowid IN (
                        SELECT rowid FROM state
                        WHERE expires_at IS NOT NULL AND expires_at <= ?1
                        LIMIT ?2)",
                    params![now, PURGE_BATCH as i64],
                )?;
                (n, started.elapsed())
            };
            purged += n as u64;
            if n < PURGE_BATCH {
                return Ok(purged);
            }
            // The store is the other calls' for as long as the batch held
            // it, so they keep at least half their pace while a purge runs.
            // A yield alone let each waiting call make one call per batch.
            // Nothing waits for a purge to end.
            tokio::time::sleep(took).await;
        }
    }

    async fn reset(&self) -> Result<()> {
        let _turn = self.turn.lock().await;
        self.conn.lock().execute("DELETE FROM state", [])?;
        Ok(())
    }
}

/// A transaction on the store's one connection, holding the store's turn
/// until it ends.
struct SqliteTxn {
    conn: Arc<Mutex<Connection>>,
    cfg: StateConfig,
    writer: bool,
    /// Not committed yet: dropping it rolls back.
    open: bool,
    _turn: OwnedMutexGuard<()>,
}

impl SqliteTxn {
    /// The connection, while the transaction is still SQLite's: an error
    /// (a full disk, say) can make SQLite roll it back, and what came after
    /// would then land on its own.
    fn conn(&self) -> Result<parking_lot::MutexGuard<'_, Connection>> {
        let conn = self.conn.lock();
        if conn.is_autocommit() {
            bail!("SQLite rolled the transaction back; nothing more of it is kept");
        }
        Ok(conn)
    }
}

#[async_trait]
impl Txn for SqliteTxn {
    async fn get(&mut self, ns: &str, key: &str) -> Result<Option<StateValue>> {
        let now = now_ms() as i64;
        let conn = self.conn()?;
        let found = conn
            .prepare_cached("SELECT kind, value, expires_at FROM state WHERE ns = ?1 AND key = ?2")?
            .query_row(params![ns, key], |r| {
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
                // A reader leaves the row to the writer.
                if self.writer {
                    conn.execute(
                        "DELETE FROM state WHERE ns = ?1 AND key = ?2",
                        params![ns, key],
                    )?;
                }
                Ok(None)
            }
            Some((kind, raw, _)) => Ok(Some(from_sqlite(&kind, raw)?)),
        }
    }

    async fn set(
        &mut self,
        ns: &str,
        key: &str,
        value: &StateValue,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let now = now_ms();
        let (kind, raw) = to_sqlite(value)?;
        let expires_at = expires_at(&self.cfg, ttl_ms, now).map(|e| e as i64);
        self.conn()?.execute(
            "INSERT INTO state (ns, key, kind, value, expires_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (ns, key) DO UPDATE SET
                kind = excluded.kind, value = excluded.value,
                expires_at = excluded.expires_at, updated_at = excluded.updated_at",
            params![ns, key, kind, raw, expires_at, now as i64],
        )?;
        Ok(())
    }

    async fn del(&mut self, ns: &str, key: &str) -> Result<bool> {
        let n = self.conn()?.execute(
            "DELETE FROM state WHERE ns = ?1 AND key = ?2",
            params![ns, key],
        )?;
        Ok(n > 0)
    }

    async fn incr(&mut self, ns: &str, key: &str, by: i64, ttl_ms: Option<u64>) -> Result<i64> {
        let now = now_ms();
        let expires_at = expires_at(&self.cfg, ttl_ms, now).map(|e| e as i64);
        let conn = self.conn()?;
        let current: Option<(String, Value, Option<i64>)> = conn
            .query_row(
                "SELECT kind, value, expires_at FROM state WHERE ns = ?1 AND key = ?2",
                params![ns, key],
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
        conn.execute(
            "INSERT INTO state (ns, key, kind, value, expires_at, updated_at)
             VALUES (?1, ?2, 'int', ?3, ?4, ?5)
             ON CONFLICT (ns, key) DO UPDATE SET
                kind = 'int', value = excluded.value,
                expires_at = excluded.expires_at, updated_at = excluded.updated_at",
            params![ns, key, Value::Integer(next), expires_at, now as i64],
        )?;
        Ok(next)
    }

    async fn range(&mut self, ns: &str, from: &str, to: &str, limit: usize) -> Result<Vec<Entry>> {
        let now = now_ms() as i64;
        let conn = self.conn()?;
        // Walks the primary key (ns, key): one index range.
        let mut st = conn.prepare_cached(
            "SELECT ns, key, kind, value, expires_at, updated_at FROM state
             WHERE ns = ?1 AND key >= ?2 AND key < ?3
               AND (expires_at IS NULL OR expires_at > ?4)
             ORDER BY key LIMIT ?5",
        )?;
        let rows = st.query_map(params![ns, from, to, now, limit as i64], row_entry)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.conn.lock().execute_batch("COMMIT")?;
        self.open = false;
        Ok(())
    }
}

impl Drop for SqliteTxn {
    fn drop(&mut self) {
        let conn = self.conn.lock();
        // A failed COMMIT may have ended the transaction already.
        if self.open && !conn.is_autocommit() {
            if let Err(e) = conn.execute_batch("ROLLBACK") {
                tracing::warn!(target: "state", "rolling a transaction back: {e}");
            }
        }
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

    #[tokio::test(flavor = "multi_thread")]
    async fn transactions() {
        let dir = temp_dir("txn");
        let store = Arc::new(SqliteStore::open(&StateConfig::default(), &dir).unwrap());
        crate::state::conformance::transactions(store).await;
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Once SQLite has rolled a transaction back, its later calls are
    /// refused rather than landing on their own.
    #[tokio::test]
    async fn a_transaction_sqlite_rolled_back_keeps_nothing_more() {
        let dir = temp_dir("rolled-back");
        let store = SqliteStore::open(&StateConfig::default(), &dir).unwrap();
        let mut tx = store.begin().await.unwrap();
        tx.set("ns", "a", &StateValue::Int(1), None).await.unwrap();
        // As SQLite does on some errors, a full disk among them.
        store.conn.lock().execute_batch("ROLLBACK").unwrap();
        assert!(tx.set("ns", "b", &StateValue::Int(2), None).await.is_err());
        assert!(tx.commit().await.is_err());
        assert_eq!(store.get("ns", "a").await.unwrap(), None);
        assert_eq!(store.get("ns", "b").await.unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn readers_see_the_runtime_writes_without_the_lock() {
        let dir = temp_dir("reader");
        let store = SqliteStore::open(&StateConfig::default(), &dir).unwrap();
        store
            .set("detect", "org-1:seen", &StateValue::Int(3), Some(0))
            .await
            .unwrap();
        let path = sqlite_path(&StateConfig::default(), &dir);
        let reader = SqliteStore::open_readonly(&path).unwrap();
        assert_eq!(
            reader.get("detect", "org-1:seen").await.unwrap(),
            Some(StateValue::Int(3))
        );
        let rows = reader
            .sql("SELECT ns, key FROM state ORDER BY key", 10)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["key"], "org-1:seen");
        assert!(
            reader.sql("DELETE FROM state", 10).is_err(),
            "writes are refused"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Expired rows are the writer's to delete: a reader's purge leaves
    /// them in the table.
    #[tokio::test]
    async fn a_reader_leaves_expired_rows_to_the_writer() {
        let dir = temp_dir("reader-purge");
        let writer = SqliteStore::open(&StateConfig::default(), &dir).unwrap();
        writer
            .set("sources", "okta:seen:1", &StateValue::Bool(true), Some(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let reader =
            SqliteStore::open_readonly(&sqlite_path(&StateConfig::default(), &dir)).unwrap();
        let rows = || reader.sql("SELECT key FROM state", 10).unwrap().len();
        assert_eq!(reader.purge_expired().await.unwrap(), 0);
        assert_eq!(rows(), 1, "still in the table");
        assert_eq!(writer.purge_expired().await.unwrap(), 1);
        assert_eq!(rows(), 0, "the writer's purge deletes it");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The runtime's purge runs on its timer until it is stopped: a row that
    /// expires after one purge is deleted by a later one.
    #[tokio::test]
    async fn the_purge_runs_until_stopped() {
        let dir = temp_dir("purge-timer");
        let store = Arc::new(SqliteStore::open(&StateConfig::default(), &dir).unwrap());
        let stop = tokio_util::sync::CancellationToken::new();
        let every = Duration::from_millis(20);
        let task = crate::state::spawn_purge(store.clone(), every, stop.clone());
        // Past the first purge, which runs at once.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let seen = StateValue::Bool(true);
        store
            .set("sources", "okta:seen:1", &seen, Some(1))
            .await
            .unwrap();
        let rows = || store.sql("SELECT key FROM state", 10).unwrap().len();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while rows() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(rows(), 0, "a later purge deleted it");

        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the purge stops")
            .unwrap();
        store
            .set("sources", "okta:seen:2", &seen, Some(1))
            .await
            .unwrap();
        tokio::time::sleep(every * 3).await;
        assert_eq!(rows(), 1, "nothing purges once stopped");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// `n` seen ids, expired, and a cursor that never expires.
    async fn expired_ids_and_a_cursor(store: &SqliteStore, n: usize) -> StateValue {
        let mut tx = store.begin().await.unwrap();
        for i in 0..n {
            let key = format!("okta:seen:{i}");
            tx.set("sources", &key, &StateValue::Bool(true), Some(1))
                .await
                .unwrap();
        }
        let cursor = StateValue::Str("2026-11-07T00:00:00Z".into());
        tx.set("sources", "okta:cursor", &cursor, Some(0))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        cursor
    }

    /// The rows in the table, expired or not.
    fn rows(store: &SqliteStore) -> u64 {
        store.sql("SELECT count(*) AS n FROM state", 1).unwrap()[0]["n"]
            .as_u64()
            .unwrap()
    }

    fn spawn_purge(store: &Arc<SqliteStore>) -> tokio::task::JoinHandle<u64> {
        let store = Arc::clone(store);
        tokio::spawn(async move { store.purge_expired().await.unwrap() })
    }

    /// A purge deletes every expired row, a batch per turn: a call that
    /// comes in meanwhile runs between two batches, not after them all.
    #[tokio::test]
    async fn a_purge_takes_a_turn_per_batch() {
        let dir = temp_dir("purge-batches");
        let store = Arc::new(SqliteStore::open(&StateConfig::default(), &dir).unwrap());
        let expired = 2 * PURGE_BATCH + 1;
        let cursor = expired_ids_and_a_cursor(&store, expired).await;

        let purge = spawn_purge(&store);
        let get = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                assert_eq!(
                    store.get("sources", "okta:cursor").await.unwrap(),
                    Some(cursor)
                );
                rows(&store)
            }
        });
        let left_at_get = get.await.unwrap();
        assert_eq!(purge.await.unwrap(), expired as u64);
        assert!(
            left_at_get > 1 && left_at_get <= expired as u64,
            "the get ran between two batches: {left_at_get} rows were left"
        );
        assert_eq!(rows(&store), 1, "every expired row is gone");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Between two batches a purge leaves the store to other calls, so one
    /// that makes call after call keeps going while it runs, not one call
    /// per batch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn calls_keep_going_while_a_purge_runs() {
        let dir = temp_dir("purge-pace");
        let store = Arc::new(SqliteStore::open(&StateConfig::default(), &dir).unwrap());
        let batches = 10;
        let cursor = expired_ids_and_a_cursor(&store, batches * PURGE_BATCH).await;
        let all = rows(&store);

        let purge = spawn_purge(&store);
        // Counting from the end of the first batch.
        while rows(&store) == all {
            tokio::task::yield_now().await;
        }
        let mut calls = 0;
        while !purge.is_finished() {
            let got = store.get("sources", "okta:cursor").await.unwrap();
            assert_eq!(got.as_ref(), Some(&cursor));
            calls += 1;
        }
        assert_eq!(purge.await.unwrap(), (batches * PURGE_BATCH) as u64);
        assert!(
            calls >= 10 * batches,
            "{calls} calls while {batches} batches ran"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Each batch goes by the clock when it runs, as a read does, not the
    /// clock when the purge began: a row written while a purge runs
    /// expires by the clock it was written with.
    #[tokio::test]
    async fn each_batch_goes_by_the_clock_then() {
        let dir = temp_dir("purge-clock");
        let store = Arc::new(SqliteStore::open(&StateConfig::default(), &dir).unwrap());
        let expired = 10 * PURGE_BATCH;
        expired_ids_and_a_cursor(&store, expired).await;
        let all = rows(&store);

        let purge = spawn_purge(&store);
        while rows(&store) == all {
            tokio::task::yield_now().await;
        }
        // Written after the purge began, and expired before it ends.
        store
            .set(
                "sources",
                "okta:seen:late",
                &StateValue::Bool(true),
                Some(1),
            )
            .await
            .unwrap();
        assert_eq!(purge.await.unwrap(), expired as u64 + 1);
        assert_eq!(rows(&store), 1, "only the cursor is left");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn read_only_state_reads_a_locked_store() {
        let dir = temp_dir("read-only");
        let cfg = StateConfig {
            read_only: true,
            ..StateConfig::default()
        };
        let err = crate::state::open(&cfg, &dir)
            .err()
            .expect("a reader creates nothing");
        assert!(
            format!("{err:#}").contains("start the pipeline that writes"),
            "{err:#}"
        );
        assert!(!sqlite_path(&cfg, &dir).exists());
        std::fs::write(sqlite_path(&cfg, &dir), b"").unwrap();
        let err = crate::state::open(&cfg, &dir)
            .err()
            .expect("an empty file holds no state");
        assert!(format!("{err:#}").contains("no `state` table"), "{err:#}");
        let writer = SqliteStore::open(&StateConfig::default(), &dir).unwrap();
        writer
            .set("history", "gone", &StateValue::Int(1), Some(1))
            .await
            .unwrap();
        let reader = crate::state::open(&cfg, &dir).unwrap();
        writer
            .set("history", "seen", &StateValue::Int(3), Some(0))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            reader.get("history", "seen").await.unwrap(),
            Some(StateValue::Int(3))
        );
        assert_eq!(
            reader.get("history", "gone").await.unwrap(),
            None,
            "expired, and left to the writer"
        );
        assert!(reader
            .set("history", "k", &StateValue::Int(1), None)
            .await
            .is_err());
        // A reader's transaction reads one snapshot and writes nothing.
        let mut tx = reader.begin().await.unwrap();
        assert_eq!(
            tx.get("history", "seen").await.unwrap(),
            Some(StateValue::Int(3))
        );
        assert!(tx
            .set("history", "k", &StateValue::Int(1), None)
            .await
            .is_err());
        drop(tx);
        let _ = std::fs::remove_dir_all(dir);
    }
}
