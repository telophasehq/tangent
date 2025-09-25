use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::{self, Instant as TokioInstant};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixStream,
    sync::mpsc,
    task,
};
use tracing::{error, info};

use wasmtime::component::{bindgen, Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::{IoView, WasiCtx, WasiCtxBuilder, WasiView};

use crate::{BATCH_EVENTS, BATCH_LATENCY};

const BATCH_MAX_BYTES: usize = 2 << 20;
const BATCH_MAX_AGE: Duration = Duration::from_millis(5);

bindgen!({
    world: "processor",
    path: "../wit",
    async: true,
});

struct Host {
    ctx: WasiCtx,
    table: ResourceTable,
}

impl IoView for Host {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}
impl WasiView for Host {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
    }
}

struct Worker {
    rx: mpsc::Receiver<UnixStream>,
    store: Store<Host>,
    processor: Processor,
}

impl Worker {
    async fn run(mut self) {
        while let Some(stream) = self.rx.recv().await {
            if let Err(e) = handle_conn(stream, &mut self.store, &self.processor).await {
                error!("worker conn error: {e}");
            }
        }
    }
}

async fn handle_conn(
    stream: UnixStream,
    store: &mut Store<Host>,
    processor: &Processor,
) -> Result<()> {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    let mut batch: Vec<u8> = Vec::with_capacity(BATCH_MAX_BYTES);
    let mut events_in_batch: usize = 0;

    let mut deadline = TokioInstant::now() + BATCH_MAX_AGE;
    let sleeper = time::sleep_until(deadline);
    tokio::pin!(sleeper);

    async fn flush_batch(
        batch: &mut Vec<u8>,
        events_in_batch: &mut usize,
        store: &mut Store<Host>,
        processor: &Processor,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let s: &mut Store<Host> = &mut *store;

        // TODO: handle output
        let _ = processor.call_process_logs(s, &batch).await?;

        let secs = start.elapsed().as_secs_f64();
        BATCH_LATENCY.observe(secs);
        BATCH_EVENTS.inc_by(*events_in_batch as u64);

        tracing::info!(target: "sidecar", "processed batch of {} in {} µs",
                                events_in_batch, start.elapsed().as_micros());
        batch.clear();
        *events_in_batch = 0;
        Ok(())
    }

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line? {
                    Some(mut l) => {
                        if l.ends_with('\n') { l.pop(); }
                        if l.ends_with('\r') { l.pop(); }
                        if l.is_empty() { continue; }

                        if batch.is_empty() {
                            deadline = TokioInstant::now() + BATCH_MAX_AGE;
                            sleeper.as_mut().reset(deadline);
                        }

                        batch.extend_from_slice(l.as_bytes());
                        batch.push(b'\n');
                        events_in_batch += 1;

                        if batch.len() >= BATCH_MAX_BYTES {
                            let _ = flush_batch(&mut batch, &mut events_in_batch, store, processor).await?;
                            deadline = TokioInstant::now() + BATCH_MAX_AGE;
                            sleeper.as_mut().reset(deadline);
                        }
                    }
                    None => {
                        tracing::info!(target:"sidecar", "client EOF; flushing then closing");
                        let _ = flush_batch(&mut batch, &mut events_in_batch, store, processor).await?;
                        break;
                    }
                }
            }

            _ = &mut sleeper => {
                if !batch.is_empty() {
                    flush_batch(&mut batch, &mut events_in_batch, store, processor).await?;
                }
                deadline = TokioInstant::now() + BATCH_MAX_AGE;
                sleeper.as_mut().reset(deadline);
            }
        }

        tokio::task::yield_now().await;
    }

    Ok(())
}

pub struct WasmEngine {
    engine: Engine,
    component: Component,
    linker: Linker<Host>,
}

impl WasmEngine {
    pub fn new() -> Result<Self> {
        let mut cfg = Config::new();
        cfg.wasm_component_model(true)
            .async_support(true)
            .wasm_simd(true);

        let engine = Engine::new(&cfg)?;
        let path = std::env::var("WASM_COMPONENT")
            .unwrap_or_else(|_| "compiled/app.component.wasm".into());
        let component = Component::from_file(&engine, &path)?;
        let mut linker = Linker::<Host>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        Ok(Self {
            engine,
            component,
            linker,
        })
    }

    pub async fn spawn_workers(&self, n: usize) -> Vec<mpsc::Sender<UnixStream>> {
        let mut senders = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel::<UnixStream>(128);

            let mut store = Store::new(
                &self.engine,
                Host {
                    ctx: WasiCtxBuilder::new()
                        .inherit_stdout()
                        .inherit_stderr()
                        .build(),
                    table: ResourceTable::new(),
                },
            );
            let processor = Processor::instantiate_async(&mut store, &self.component, &self.linker)
                .await
                .expect("instantiate");

            let start = Instant::now();
            match processor.call_process_logs(&mut store, b"{}").await {
                Ok(_) => {
                    let secs = start.elapsed().as_secs_f64();
                    BATCH_LATENCY.observe(secs);

                    tracing::info!(target: "sidecar", "processed warmup in {} µs",
                                    start.elapsed().as_micros());
                }
                Err(e) => {
                    tracing::error!(target: "sidecar", "warmup failed after {} µs: {e}",
                                    start.elapsed().as_micros());
                }
            }

            let worker = Worker {
                rx,
                store,
                processor,
            };
            task::spawn(worker.run());

            senders.push(tx);
        }
        info!("spawned {} WASM workers", n);
        senders
    }
}
