use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, ToSql};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::sqlite::SqliteConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sinks::sqlite::{index_columns, open, quote};
use crate::state::now_ms;
use crate::worker::Ack;

/// Sets a group's rows' emitted to the time the DAG is done with them.
struct Emitted {
    conn: Arc<Mutex<Connection>>,
    table: Arc<str>,
    ids: Vec<i64>,
    tx: mpsc::UnboundedSender<Vec<i64>>,
}

#[async_trait]
impl Ack for Emitted {
    async fn ack(&self) -> Result<()> {
        let res = mark(&self.conn, &self.table, &self.ids);
        // rows that could not be marked go again
        let _ = self.tx.send(self.ids.clone());
        res
    }
}

fn mark(conn: &Mutex<Connection>, table: &str, ids: &[i64]) -> Result<()> {
    let mut conn = conn.lock();
    let tx = conn.transaction()?;
    {
        let mut st = tx.prepare_cached(&format!(
            "UPDATE \"{table}\" SET emitted = ?1 WHERE id = ?2"
        ))?;
        let now = now_ms() as i64;
        for id in ids {
            st.execute([now, *id])?;
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
    if cfg.batch_size == 0 || cfg.timeout_secs == 0 {
        bail!("batch_size and timeout_secs must be positive");
    }
    if ["pending", "at"]
        .iter()
        .any(|r| r.eq_ignore_ascii_case(&cfg.partition))
    {
        bail!("{}_{} is the table's own index", cfg.table, cfg.partition);
    }
    // the sink, maybe another process started later, adds the partition's columns and index
    let conn = Arc::new(Mutex::new(open(&cfg.path, &cfg.table, &BTreeMap::new())?));
    let table: Arc<str> = Arc::from(cfg.table.as_str());
    let mut part = None;
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<i64>>();
    let mut stored = Stored {
        id: None,
        since: Instant::now(),
    };
    let mut sent: HashSet<i64> = HashSet::new(); // rows of unacked groups
    let mut unacked = 0usize;
    loop {
        while let Ok(ids) = rx.try_recv() {
            acked(&mut sent, &mut unacked, ids);
        }
        if part.is_none() {
            part = partition(&conn.lock(), &cfg)?;
        }
        let groups = match &part {
            Some(p) => due(&conn, p, &cfg, &mut stored, &sent).unwrap_or_else(|e| {
                tracing::warn!("sqlite source {name}: {e:#}");
                Vec::new()
            }),
            None => Vec::new(),
        };
        if groups.is_empty() {
            tokio::select! {
                () = shutdown.cancelled() => break,
                Some(ids) = rx.recv() => acked(&mut sent, &mut unacked, ids),
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            continue;
        }
        for (context, ids) in groups {
            while unacked >= 16 {
                tokio::select! {
                    () = shutdown.cancelled() => return Ok(()),
                    Some(ids) = rx.recv() => acked(&mut sent, &mut unacked, ids),
                }
            }
            let frame = match frame(&conn, &table, &context, &ids) {
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
                table: Arc::clone(&table),
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

/// A sink's partition: its columns, and the reads by them.
struct Partition {
    cols: usize,
    read: String,
    before: String,
}

/// The columns of the sink's index but at; None until the sink makes it.
fn partition(conn: &Connection, cfg: &SqliteConfig) -> Result<Option<Partition>> {
    let (t, p) = (&cfg.table, &cfg.partition);
    let rows: bool =
        conn.query_row(&format!("SELECT EXISTS (SELECT 1 FROM \"{t}\")"), [], |r| {
            r.get(0)
        })?;
    let mut cols = index_columns(conn, &format!("{t}_{p}"))?;
    if cols.pop().is_none() {
        if rows {
            bail!("no index {t}_{p}: {p} is not a partition of the sqlite sink writing {t}");
        }
        return Ok(None);
    }
    let n = cols.len();
    let sel: String = cols.iter().map(|c| format!(", {}", quote(c))).collect();
    let by: String = cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{} IS ?{} AND ", quote(c), i + 1))
        .collect();
    Ok(Some(Partition {
        cols: n,
        read: format!("SELECT id, at{sel} FROM \"{t}\" WHERE emitted IS NULL ORDER BY at, id"),
        before: format!(
            "SELECT id FROM \"{t}\" INDEXED BY \"{t}_{p}\"
            WHERE {by}at <= ?{} AND emitted IS NOT NULL ORDER BY at DESC, id DESC LIMIT ?{}",
            n + 1,
            n + 2
        ),
    }))
}

/// The newest id stored, and since when.
struct Stored {
    id: Option<i64>,
    since: Instant,
}

fn acked(sent: &mut HashSet<i64>, unacked: &mut usize, ids: Vec<i64>) {
    for id in ids {
        sent.remove(&id);
    }
    *unacked -= 1;
}

/// Ids as (context, pending) per value with none in flight: its first `batch_size` pending rows,
/// or at the timeout all of them after its latest emitted rows, `batch_size` in all.
fn due(
    conn: &Mutex<Connection>,
    part: &Partition,
    cfg: &SqliteConfig,
    stored: &mut Stored,
    sent: &HashSet<i64>,
) -> Result<Vec<(Vec<i64>, Vec<i64>)>> {
    let conn = conn.lock();
    let t = &cfg.table;
    let (last, newest): (Option<i64>, Option<i64>) = conn.query_row(
        &format!("SELECT (SELECT max(id) FROM \"{t}\"), (SELECT max(at) FROM \"{t}\")"),
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if last != stored.id {
        *stored = Stored {
            id: last,
            since: Instant::now(),
        };
    }
    let idle = stored.since.elapsed() >= Duration::from_secs(cfg.timeout_secs);
    let Some(newest) = newest else {
        return Ok(Vec::new());
    };
    let old = newest - cfg.timeout_secs as i64 * 1000;

    let mut parts: BTreeMap<Vec<Option<String>>, Vec<(i64, i64)>> = BTreeMap::new(); // at, id
    let mut busy: HashSet<Vec<Option<String>>> = HashSet::new(); // values with a group in flight
    let mut st = conn.prepare_cached(&part.read)?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let id: i64 = r.get(0)?;
        let key = (2..part.cols + 2)
            .map(|i| r.get(i))
            .collect::<rusqlite::Result<Vec<Option<String>>>>()?;
        if sent.contains(&id) {
            busy.insert(key);
            continue;
        }
        parts.entry(key).or_default().push((r.get(1)?, id));
    }

    let mut groups: Vec<(i64, Vec<i64>, Vec<i64>)> = Vec::new(); // first at, context, ids
    let mut prior = conn.prepare_cached(&part.before)?;
    for (key, rows) in parts {
        if busy.contains(&key) {
            continue;
        }
        let mut context: Vec<i64> = Vec::new();
        if rows.len() < cfg.batch_size {
            if !idle && rows[0].0 > old {
                continue;
            }
            let n = cfg.batch_size - rows.len();
            let mut args: Vec<&dyn ToSql> = key.iter().map(|v| v as &dyn ToSql).collect();
            args.extend([&rows[0].0 as &dyn ToSql, &n]);
            context = prior
                .query_map(&*args, |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            context.reverse();
        }
        let ids = rows.iter().take(cfg.batch_size).map(|r| r.1).collect();
        groups.push((rows[0].0, context, ids));
    }
    groups.sort_by_key(|g| g.0);
    Ok(groups.into_iter().map(|g| (g.1, g.2)).collect())
}

/// The rows' records, those of `context` with `"_context": true`.
fn frame(conn: &Mutex<Connection>, table: &str, context: &[i64], ids: &[i64]) -> Result<BytesMut> {
    let conn = conn.lock();
    let mut st = conn.prepare_cached(&format!("SELECT record FROM \"{table}\" WHERE id = ?1"))?;
    let mut frame = BytesMut::new();
    for (k, id) in context.iter().chain(ids).enumerate() {
        // trimmed meanwhile
        let Some(mut record) = st.query_row([id], |r| r.get::<_, String>(0)).optional()? else {
            continue;
        };
        if k < context.len() {
            // only an object can carry the mark
            let Ok(Value::Object(mut m)) = serde_json::from_str(&record) else {
                continue;
            };
            m.insert("_context".into(), Value::Bool(true));
            record = Value::Object(m).to_string();
        }
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
        let actor = BTreeMap::from([("actor".to_string(), vec!["actor".to_string()])]);
        let conn = open(path, "logs", &actor).unwrap();
        for (at, actor) in rows {
            conn.execute(
                "INSERT INTO logs (at, record, actor) VALUES (?1, ?2, ?3)",
                rusqlite::params![at, row(*at, actor), actor],
            )
            .unwrap();
        }
    }

    fn emitted(path: &Path) -> i64 {
        let conn = open(path, "logs", &BTreeMap::new()).unwrap();
        conn.query_row(
            "SELECT count(*) FROM logs WHERE emitted IS NOT NULL",
            [],
            |r| r.get(0),
        )
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
            table: "logs".into(),
            partition: "actor".into(),
            batch_size: 3,
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

    fn context(rows: &[(i64, &str)]) -> String {
        rows.iter()
            .map(|(at, a)| format!("{{\"at\":{at},\"actor\":\"{a}\",\"_context\":true}}\n"))
            .collect()
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
    async fn a_quiet_value_is_topped_up_with_context() {
        let path = path("context");
        let a: Vec<(i64, &str)> = (1..=4).map(|k| (T0 + k, "a")).collect();
        let b = [(T0 + 5, "b")];
        insert(&path, &a);
        insert(&path, &b);
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 1);
        writes(&sink, 3).await;
        until(|| emitted(&path) == 5).await;
        let more = [(T0 + 6, "a"), (T0 + 7, "a"), (T0 + 8, "a")];
        insert(&path, &more);
        let got = writes(&sink, 4).await;
        until(|| emitted(&path) == 8).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(
            got,
            [
                group(&a[..3]),
                context(&a[1..3]) + &group(&a[3..]),
                group(&b),
                group(&more),
            ]
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn a_partition_of_two_fields_groups_by_both() {
        let path = path("two");
        let fields = vec!["org".to_string(), "actor".to_string()];
        let conn = open(
            &path,
            "logs",
            &BTreeMap::from([("actor".to_string(), fields)]),
        )
        .unwrap();
        let rows = [
            (T0 + 1, Some("x")),
            (T0 + 2, Some("y")),
            (T0 + 3, None),
            (T0 + 4, Some("x")),
            (T0 + 5, None),
        ];
        let put = |at: i64, org: Option<&str>| {
            conn.execute(
                "INSERT INTO logs (at, record, org, actor) VALUES (?1, ?2, ?3, 'a')",
                rusqlite::params![at, row(at, "a"), org],
            )
            .unwrap();
        };
        for (at, org) in rows {
            put(at, org);
        }
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 1);
        writes(&sink, 3).await;
        until(|| emitted(&path) == 5).await;
        put(T0 + 6, None);
        let got = writes(&sink, 4).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        assert_eq!(
            got,
            [
                group(&[(T0 + 1, "a"), (T0 + 4, "a")]),
                group(&[(T0 + 2, "a")]),
                group(&[(T0 + 3, "a"), (T0 + 5, "a")]),
                context(&[(T0 + 3, "a"), (T0 + 5, "a")]) + &group(&[(T0 + 6, "a")]),
            ]
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn no_index_is_an_error_once_rows_exist() {
        let path = path("index");
        let sink = Arc::new(RecordingSink::default());
        let (_stop, h) = start(&path, &sink, 3600);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(!h.is_finished(), "no rows: waits for the sink");
        open(&path, "logs", &BTreeMap::new())
            .unwrap()
            .execute("INSERT INTO logs (at, record) VALUES (1, '{}')", [])
            .unwrap();
        let err = h.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("no index logs_actor"), "{err}");
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

        let t = now_ms() as i64;
        let sink = Arc::new(RecordingSink::default());
        let (stop, h) = start(&path, &sink, 3600);
        assert_eq!(writes(&sink, 1).await, [group(&a)], "unacked: again");
        until(|| emitted(&path) == 3).await;
        stop.cancel();
        h.await.unwrap().unwrap();
        let (first, last): (i64, i64) = open(&path, "logs", &BTreeMap::new())
            .unwrap()
            .query_row("SELECT min(emitted), max(emitted) FROM logs", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert!(t <= first && last <= now_ms() as i64, "the ack time");

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
