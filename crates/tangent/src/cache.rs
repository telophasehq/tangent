use std::fs::{create_dir_all, OpenOptions};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use fs2::FileExt;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rusqlite::types::Value;
use rusqlite::{params, Connection, OpenFlags};
use tangent_shared::runtime::CacheConfig;
use tracing::info;

use crate::wasm::host::tangent::logs::log::Scalar;

static CACHE_OPEN_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Clone)]
pub struct CacheHandle {
    conn: std::sync::Arc<Mutex<Connection>>,
    _lock: std::sync::Arc<std::fs::File>,
    default_ttl_ms: u64,
    max_ttl_ms: u64,
}

impl CacheHandle {
    pub fn open(cfg: &CacheConfig, base_dir: &Path) -> Result<Self> {
        let _open_guard = CACHE_OPEN_GUARD.lock();

        let path = if cfg.path.is_absolute() {
            cfg.path.clone()
        } else {
            base_dir.join(&cfg.path)
        };

        if let Some(parent) = path.parent() {
            create_dir_all(parent)
                .with_context(|| format!("creating cache dir {}", parent.display()))?;
        }

        let lock = acquire_lock(&path, Duration::from_millis(cfg.lock_timeout_ms))?;

        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .with_context(|| format!("opening cache db at {}", path.display()))?;

        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", &"WAL")?;
        conn.pragma_update(None, "synchronous", &"NORMAL")?;
        conn.pragma_update(None, "wal_autocheckpoint", &1000i64)?;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cache(
                key TEXT PRIMARY KEY,
                kind TEXT,
                value BLOB NOT NULL,
                expires_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS cache_expires_idx ON cache(expires_at);
            "#,
        )
        .context("creating schema")?;

        let guard = std::sync::Arc::new(lock);

        info!(target = "cache", path = %path.display(), "cache db initialized");

        Ok(Self {
            conn: std::sync::Arc::new(Mutex::new(conn)),
            _lock: guard,
            default_ttl_ms: cfg.default_ttl_ms,
            max_ttl_ms: cfg.max_ttl_ms,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Scalar>> {
        let now = now_ms();
        let conn = self.conn.lock();
        let mut stmt =
            conn.prepare_cached("SELECT kind, value, expires_at FROM cache WHERE key = ?1")?;
        let mut rows = stmt.query(params![key])?;

        if let Some(row) = rows.next()? {
            let expires_at: i64 = row.get(2)?;
            if expires_at <= now as i64 {
                drop(rows);
                conn.execute("DELETE FROM cache WHERE key = ?1", params![key])?;
                return Ok(None);
            }
            let kind: String = row.get(0)?;
            let val: Value = row.get(1)?;
            return Ok(Some(Scalar::from_sqlite(&kind, val)?));
        }

        Ok(None)
    }

    pub fn set(&self, key: &str, v: &Scalar, ttl_ms: Option<u64>) -> Result<()> {
        let (kind, val) = v.to_sqlite();

        let ttl = ttl_ms.unwrap_or(self.default_ttl_ms).min(self.max_ttl_ms);
        let expires_at = now_ms()
            .checked_add(ttl)
            .ok_or_else(|| anyhow!("ttl overflow"))?;
        let updated_at = now_ms();

        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO cache(key, kind, value, expires_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(key) DO UPDATE SET kind=excluded.kind, value=excluded.value, expires_at=excluded.expires_at, updated_at=excluded.updated_at",
            rusqlite::params![key, kind, val, expires_at as i64, updated_at as i64],
        )?;
        Ok(())
    }

    pub fn del(&self, key: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let rows = conn.execute("DELETE FROM cache WHERE key = ?1", params![key])?;
        Ok(rows > 0)
    }

    pub fn reset(&self) -> Result<()> {
        let conn = self.conn.lock();
        let _ = conn
            .execute("drop table cache", rusqlite::params![])
            .map_err(|e| anyhow!(e))?;

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
            .open(&lock_path)
            .with_context(|| format!("opening cache lock file {}", lock_path.display()))?;

        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(e) => {
                if start.elapsed() >= timeout {
                    return Err(anyhow!("failed to acquire cache lock: {e}"));
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
