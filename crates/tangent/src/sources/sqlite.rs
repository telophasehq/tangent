use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::sqlite::SqliteConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sinks::sqlite::open;
use crate::worker::Ack;

/// Marks a group's rows emitted once the DAG is done with them.
struct Emitted {
    conn: Arc<Mutex<Connection>>,
    ids: Vec<i64>,
    tx: mpsc::UnboundedSender<Vec<i64>>,
}

#[async_trait]
impl Ack for Emitted {
    async fn ack(&self) -> Result<()> {
        let res = mark(&self.conn, &self.ids);
        // rows that could not be marked go again
        let _ = self.tx.send(self.ids.clone());
        res
    }
}

fn mark(conn: &Mutex<Connection>, ids: &[i64]) -> Result<()> {
    let mut conn = conn.lock();
    let tx = conn.transaction()?;
    {
        let mut st = tx.prepare_cached("UPDATE logs SET emitted = 1 WHERE id = ?1")?;
        for id in ids {
            st.execute([id])?;
        }
    }
    tx.commit()?;
    Ok(())
}

pub async fn run_consumer(
    name: Arc<str>,
    cfg: SqliteConfig,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    if cfg.max_events == 0 || cfg.timeout_secs == 0 {
        bail!("max_events and timeout_secs must be positive");
    }
    // the sink, maybe another process started later, adds the partition column
    let conn = Arc::new(Mutex::new(open(&cfg.path, &[])?));
    let read = format!(
        "SELECT id, \"{}\", at FROM logs WHERE emitted = 0 ORDER BY at, id",
        cfg.partition
    );
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<i64>>();
    let mut stored = (None, Instant::now()); // newest id, seen since
    let mut sent: HashSet<i64> = HashSet::new(); // rows of unacked groups
    let mut unacked = 0usize;
    loop {
        while let Ok(ids) = rx.try_recv() {
            acked(&mut sent, &mut unacked, ids);
        }
        let groups = due(&conn, &read, &cfg, &mut stored, &sent).unwrap_or_else(|e| {
            tracing::warn!("sqlite source {name}: {e:#}");
            Vec::new()
        });
        if groups.is_empty() {
            tokio::select! {
                () = shutdown.cancelled() => break,
                Some(ids) = rx.recv() => acked(&mut sent, &mut unacked, ids),
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            continue;
        }
        for ids in groups {
            while unacked >= 16 {
                tokio::select! {
                    () = shutdown.cancelled() => return Ok(()),
                    Some(ids) = rx.recv() => acked(&mut sent, &mut unacked, ids),
                }
            }
            let frame = match frame(&conn, &ids) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!("sqlite source {name}: {e:#}");
                    continue;
                }
            };
            sent.extend(&ids);
            unacked += 1;
            let ack: Arc<dyn Ack> = Arc::new(Emitted {
                conn: Arc::clone(&conn),
                ids,
                tx: tx.clone(),
            });
            router.forward(&from, vec![frame], vec![ack]).await?;
        }
        if shutdown.is_cancelled() {
            break;
        }
    }
    Ok(())
}

fn acked(sent: &mut HashSet<i64>, unacked: &mut usize, ids: Vec<i64>) {
    for id in ids {
        sent.remove(&id);
    }
    *unacked -= 1;
}

/// Ids of the first `max_events` pending rows, oldest first, of each partition
/// value with none in flight; fewer once its oldest is `timeout_secs` older
/// than the newest row, or nothing has been stored for `timeout_secs`. Groups
/// go in order of their first row.
fn due(
    conn: &Mutex<Connection>,
    read: &str,
    cfg: &SqliteConfig,
    stored: &mut (Option<i64>, Instant),
    sent: &HashSet<i64>,
) -> Result<Vec<Vec<i64>>> {
    let conn = conn.lock();
    let last: Option<i64> = conn.query_row("SELECT max(id) FROM logs", [], |r| r.get(0))?;
    if last != stored.0 {
        *stored = (last, Instant::now());
    }
    let idle = stored.1.elapsed() >= Duration::from_secs(cfg.timeout_secs);
    let newest: Option<i64> = conn.query_row(
        "SELECT max(m) FROM (
            SELECT max(at) m FROM logs WHERE emitted = 0
            UNION ALL SELECT max(at) FROM logs WHERE emitted = 1
        )",
        [],
        |r| r.get(0),
    )?;
    let Some(newest) = newest else {
        return Ok(Vec::new());
    };
    let old = newest - cfg.timeout_secs as i64 * 1000;

    let mut parts: BTreeMap<String, Vec<(i64, i64)>> = BTreeMap::new(); // at, id
    let mut busy: HashSet<String> = HashSet::new(); // values with a group in flight
    let mut st = conn.prepare_cached(read)?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let id: i64 = r.get(0)?;
        let key: Option<String> = r.get(1)?;
        let key = key.unwrap_or_default();
        if sent.contains(&id) {
            busy.insert(key);
            continue;
        }
        let at: i64 = r.get(2)?;
        parts.entry(key).or_default().push((at, id));
    }

    let mut groups: Vec<(i64, Vec<i64>)> = Vec::new(); // first at, ids
    for (key, rows) in parts {
        let chunk = &rows[..rows.len().min(cfg.max_events)];
        if busy.contains(&key) || (chunk.len() < cfg.max_events && !idle && rows[0].0 > old) {
            continue;
        }
        groups.push((chunk[0].0, chunk.iter().map(|r| r.1).collect()));
    }
    groups.sort_by_key(|g| g.0);
    Ok(groups.into_iter().map(|g| g.1).collect())
}

fn frame(conn: &Mutex<Connection>, ids: &[i64]) -> Result<BytesMut> {
    let conn = conn.lock();
    let mut st = conn.prepare_cached("SELECT record FROM logs WHERE id = ?1")?;
    let mut frame = BytesMut::new();
    for id in ids {
        // the sink may have pruned it
        let Some(record) = st.query_row([id], |r| r.get::<_, String>(0)).optional()? else {
            continue;
        };
        frame.extend_from_slice(record.as_bytes());
        frame.extend_from_slice(b"\n");
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use ahash::AHashMap;
    use std::path::{Path, PathBuf};
    use tokio::task::JoinHandle;

    const T0: i64 = 1_767_225_600_000;
    const H: i64 = 3_600_000;

    #[derive(Default)]
    struct RecordingSink {
        writes: tokio::sync::Mutex<Vec<String>>,
        down: bool,
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            let text = String::from_utf8_lossy(&req.payload).into_owned();
            self.writes.lock().await.push(text);
            if self.down {
                bail!("down");
            }
            Ok(())
        }
    }

    fn path(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tangent-source-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("logs.sqlite")
    }

    fn row(at: i64, actor: &str) -> String {
        format!("{{\"at\":{at},\"actor\":\"{actor}\"}}")
    }

    fn insert(path: &Path, rows: &[(i64, &str)]) {
        let conn = open(path, &["actor".to_string()]).unwrap();
        for (at, actor) in rows {
            conn.execute(
                "INSERT INTO logs (at, record, actor) VALUES (?1, ?2, ?3)",
                rusqlite::params![at, row(*at, actor), actor],
            )
            .unwrap();
        }
    }

    fn emitted(path: &Path) -> i64 {
        let conn = open(path, &[]).unwrap();
        conn.query_row("SELECT count(*) FROM logs WHERE emitted = 1", [], |r| {
            r.get(0)
        })
        .unwrap()
    }

    fn start(
        path: &Path,
        sink: &Arc<RecordingSink>,
        timeout_secs: u64,
    ) -> (CancellationToken, JoinHandle<Result<()>>) {
        let from = NodeRef::Source {
            name: Arc::from("actors"),
        };
        let mut outs = AHashMap::default();
        outs.insert(
            from,
            vec![NodeRef::Sink {
                name: Arc::from("out"),
                key_prefix: None,
            }],
        );
        let sinks = vec![(Arc::from("out"), Arc::clone(sink) as Arc<dyn Sink>)];
        let router = Arc::new(Router::new(outs, Arc::new(SinkManager::for_test(sinks, 1))));
        let cfg = SqliteConfig {
            path: path.to_path_buf(),
            partition: "actor".into(),
            max_events: 3,
            timeout_secs,
        };
        let shutdown = CancellationToken::new();
        let h = tokio::spawn(run_consumer(
            Arc::from("actors"),
            cfg,
            router,
            shutdown.clone(),
        ));
        (shutdown, h)
    }

    async fn until(what: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !what() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("in time");
    }

    async fn writes(sink: &RecordingSink, n: usize) -> Vec<String> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let w = sink.writes.lock().await.clone();
                if w.len() >= n {
                    return w;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("writes in time")
    }

    fn group(rows: &[(i64, &str)]) -> String {
        rows.iter().map(|(at, a)| row(*at, a) + "\n").collect()
    }

    #[tokio::test]
    async fn full_groups_go_and_the_rest_waits() {
        let path = path("full");
        let a = [(T0 + 1, "a"), (T0 + 2, "a"), (T0 + 3, "a"), (T0 + 4, "a")];
        insert(&path, &a);
        insert(&path, &[(T0 + 5, "b"), (T0 + 6, "b")]);
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 3600);
        writes(&sink, 1).await;
        insert(&path, &[(T0 + 7, "b")]);
        let got = writes(&sink, 2).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(
            got,
            [
                group(&a[..3]),
                group(&[(T0 + 5, "b"), (T0 + 6, "b"), (T0 + 7, "b")]),
            ]
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn one_group_of_a_value_in_flight() {
        let path = path("one");
        let a: Vec<(i64, &str)> = (1..=6).map(|k| (T0 + k, "a")).collect();
        insert(&path, &a);
        let down = Arc::new(RecordingSink {
            down: true,
            ..Default::default()
        });
        let (stop, h) = start(&path, &down, 3600);
        writes(&down, 1).await;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let got = down.writes.lock().await.clone();
        stop.cancel();
        h.await.unwrap().unwrap();
        assert!(got.iter().all(|w| *w == group(&a[..3])), "{got:?}");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn a_quiet_partition_goes_after_timeout_secs() {
        let path = path("quiet");
        let a = [(T0 + 1, "a"), (T0 + 2, "a")];
        let b = [(T0 + H, "b"), (T0 + H, "b"), (T0 + H, "b")];
        insert(&path, &a);
        insert(&path, &b);
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 3600);
        writes(&sink, 1).await;
        insert(&path, &[(T0 + H + 1, "b")]);
        let got = writes(&sink, 2).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(got, [group(&b), group(&a)]);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn all_goes_when_nothing_is_stored_for_timeout_secs() {
        let path = path("idle");
        insert(&path, &[(T0 + 1, "b"), (T0 + 2, "a")]);
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 1);
        let got = writes(&sink, 2).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(got, [group(&[(T0 + 1, "b")]), group(&[(T0 + 2, "a")])]);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn a_late_row_is_pending_like_any_other() {
        let path = path("late");
        let a = [(T0 + 10, "a"), (T0 + 11, "a"), (T0 + 12, "a")];
        insert(&path, &a);
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 3600);
        writes(&sink, 1).await;
        insert(&path, &[(T0 + 20, "a"), (T0 + 1, "a"), (T0 + 21, "a")]);
        let got = writes(&sink, 2).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(
            got,
            [
                group(&a),
                group(&[(T0 + 1, "a"), (T0 + 20, "a"), (T0 + 21, "a")]),
            ]
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn acked_once_unacked_again_after_restart() {
        let path = path("restart");
        let a = [(T0 + 1, "a"), (T0 + 2, "a"), (T0 + 3, "a")];
        insert(&path, &a);

        let down = Arc::new(RecordingSink {
            down: true,
            ..Default::default()
        });
        let (stop, h) = start(&path, &down, 3600);
        assert_eq!(writes(&down, 1).await[0], group(&a));
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(emitted(&path), 0);

        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 3600);
        assert_eq!(writes(&sink, 1).await, [group(&a)], "unacked: again");
        until(|| emitted(&path) == 3).await;
        stop.cancel();
        h.await.unwrap().unwrap();

        let b = [(T0 + 4, "b"), (T0 + 5, "b"), (T0 + 6, "b")];
        insert(&path, &b);
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 3600);
        let got = writes(&sink, 1).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(got, [group(&b)], "acked: not again");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
