use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::sqlite::SqliteConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sinks::sqlite::open;
use crate::state::now_ms;
use crate::worker::Ack;

/// One partition value's rows of a window, in `at` order.
struct Group {
    ids: Vec<i64>,
    frame: BytesMut,
}

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
    if cfg.window_secs == 0 {
        bail!("window_secs must be positive");
    }
    let conn = open(&cfg.path, &[])?;
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM pragma_table_info('logs') WHERE name = ?1",
        [&cfg.partition],
        |r| r.get(0),
    )?;
    if n == 0 {
        bail!("{} has no partition {}", cfg.path.display(), cfg.partition);
    }
    let conn = Arc::new(Mutex::new(conn));
    let read = format!(
        "SELECT id, \"{}\", record FROM logs WHERE emitted = 0 AND at < ?1 ORDER BY at, id",
        cfg.partition
    );
    let w = cfg.window_secs as i64 * 1000;
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<i64>>();
    let mut start: Option<i64> = None;
    let mut sent: HashSet<i64> = HashSet::new(); // rows of unacked groups
    let mut unacked = 0usize;
    loop {
        while let Ok(ids) = rx.try_recv() {
            acked(&mut sent, &mut unacked, ids);
        }
        let due = window(&conn, &read, w, &mut start, &sent).unwrap_or_else(|e| {
            tracing::warn!("sqlite source {name}: {e:#}");
            None
        });
        let Some(groups) = due else {
            tokio::select! {
                () = shutdown.cancelled() => break,
                Some(ids) = rx.recv() => acked(&mut sent, &mut unacked, ids),
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            continue;
        };
        for g in groups {
            while unacked >= 16 {
                tokio::select! {
                    () = shutdown.cancelled() => return Ok(()),
                    Some(ids) = rx.recv() => acked(&mut sent, &mut unacked, ids),
                }
            }
            sent.extend(&g.ids);
            unacked += 1;
            let ack: Arc<dyn Ack> = Arc::new(Emitted {
                conn: Arc::clone(&conn),
                ids: g.ids,
                tx: tx.clone(),
            });
            router.forward(&from, vec![g.frame], vec![ack]).await?;
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

/// The window from `start`, when due: its unemitted rows and any earlier
/// ones not yet sent, by partition value. `start` moves to the next window
/// with rows.
fn window(
    conn: &Mutex<Connection>,
    read: &str,
    w: i64,
    start: &mut Option<i64>,
    sent: &HashSet<i64>,
) -> Result<Option<Vec<Group>>> {
    let conn = conn.lock();
    let first = |from: i64| -> rusqlite::Result<Option<i64>> {
        conn.query_row(
            "SELECT min(at) FROM logs WHERE emitted = 0 AND at >= ?1",
            [from],
            |r| r.get(0),
        )
    };
    let s = match *start {
        Some(s) => s,
        None => match first(i64::MIN)? {
            Some(at) => at.div_euclid(w) * w,
            None => return Ok(None),
        },
    };
    *start = Some(s);
    let end = s + w;
    let newest: Option<i64> =
        conn.query_row("SELECT max(at) FROM logs WHERE emitted = 0", [], |r| {
            r.get(0)
        })?;
    // due when the data has moved past it, or the clock a window further
    if newest.is_none_or(|n| n < end) && now_ms() as i64 <= end + w {
        return Ok(None);
    }

    let mut groups: BTreeMap<String, Group> = BTreeMap::new();
    let mut st = conn.prepare_cached(read)?;
    let mut rows = st.query([end])?;
    while let Some(r) = rows.next()? {
        let id: i64 = r.get(0)?;
        if sent.contains(&id) {
            continue;
        }
        let key: Option<String> = r.get(1)?;
        let record: String = r.get(2)?;
        let g = groups.entry(key.unwrap_or_default()).or_insert(Group {
            ids: Vec::new(),
            frame: BytesMut::new(),
        });
        g.ids.push(id);
        g.frame.extend_from_slice(record.as_bytes());
        g.frame.extend_from_slice(b"\n");
    }
    *start = Some(first(end)?.map_or(end, |at| at.div_euclid(w) * w));
    Ok(Some(groups.into_values().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use ahash::AHashMap;
    use std::path::{Path, PathBuf};
    use tokio::task::JoinHandle;

    const T0: i64 = 4_102_444_800_000; // 2100, never due by the clock
    const W: i64 = 300_000;

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
            window_secs: (W / 1000) as u64,
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
    async fn one_group_per_partition_per_window() {
        let path = path("groups");
        insert(
            &path,
            &[
                (T0 + 1, "a"),
                (T0 + 2, "b"),
                (T0 + 3, "a"),
                (T0 + W + 1, "a"),
                (T0 + 2 * W + 1, "z"),
            ],
        );
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink);
        let got = writes(&sink, 3).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(
            got,
            [
                group(&[(T0 + 1, "a"), (T0 + 3, "a")]),
                group(&[(T0 + 2, "b")]),
                group(&[(T0 + W + 1, "a")]),
            ]
        );
    }

    #[tokio::test]
    async fn a_late_row_joins_the_next_window() {
        let path = path("late");
        insert(&path, &[(T0 + 1, "a"), (T0 + W + 1, "a")]);
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink);
        writes(&sink, 1).await;
        insert(&path, &[(T0 + 5, "a"), (T0 + 2 * W + 1, "a")]);
        let got = writes(&sink, 2).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(
            got,
            [
                group(&[(T0 + 1, "a")]),
                group(&[(T0 + 5, "a"), (T0 + W + 1, "a")]),
            ]
        );
    }

    #[tokio::test]
    async fn acked_once_unacked_again_after_restart() {
        let path = path("restart");
        insert(&path, &[(T0 + 1, "a"), (T0 + 2, "a"), (T0 + W + 1, "b")]);
        let a = group(&[(T0 + 1, "a"), (T0 + 2, "a")]);

        let down = Arc::new(RecordingSink {
            down: true,
            ..Default::default()
        });
        let (stop, h) = start(&path, &down);
        assert_eq!(writes(&down, 1).await[0], a);
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(emitted(&path), 0);

        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink);
        assert_eq!(writes(&sink, 1).await, [a], "unacked: again");
        until(|| emitted(&path) == 2).await;
        stop.cancel();
        h.await.unwrap().unwrap();

        insert(&path, &[(T0 + 2 * W + 1, "c")]);
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink);
        let got = writes(&sink, 1).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(got, [group(&[(T0 + W + 1, "b")])], "acked: not again");
    }
}
