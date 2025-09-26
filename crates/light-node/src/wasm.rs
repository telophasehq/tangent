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

bindgen!({
    world: "processor",
    path: "../../wit",
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
    batch_max_size: usize,
    batch_max_age: Duration,
}

impl Worker {
    async fn run(mut self) {
        while let Some(stream) = self.rx.recv().await {
            if let Err(e) = handle_conn(
                stream,
                &mut self.store,
                &self.processor,
                self.batch_max_size,
                self.batch_max_age,
            )
            .await
            {
                error!("worker conn error: {e}");
            }
        }
    }
}

async fn handle_conn(
    stream: UnixStream,
    store: &mut Store<Host>,
    processor: &Processor,
    batch_max_size: usize,
    batch_max_age: Duration,
) -> Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line_bytes = Vec::with_capacity(4096);

    let mut batch: Vec<u8> = Vec::with_capacity(batch_max_size);
    let mut events_in_batch: usize = 0;

    let mut deadline = TokioInstant::now() + batch_max_age;
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

    fn reset_timer(
        sleeper: &mut core::pin::Pin<&mut tokio::time::Sleep>,
        deadline: &mut TokioInstant,
        batch_max_age: Duration,
    ) {
        *deadline = TokioInstant::now() + batch_max_age;
        sleeper.as_mut().reset(*deadline);
    }

    loop {
        tokio::select! {
            n = reader.read_until(b'\n', &mut line_bytes) => {
                let n = n?;
                if n == 0 {
                    flush_batch(&mut batch, &mut events_in_batch, store, processor).await?;
                    break;
                }

                if line_bytes.last().copied() == Some(b'\n') { line_bytes.pop(); }
                if line_bytes.last().copied() == Some(b'\r') { line_bytes.pop(); }
                if line_bytes.is_empty() {
                    line_bytes.clear();
                    continue;
                }

                let need = line_bytes.len() + 1;

                if batch.is_empty() {
                    reset_timer(&mut sleeper, &mut deadline, batch_max_age);
                }

                if batch.len() + need > batch_max_size {
                    flush_batch(&mut batch, &mut events_in_batch, store, processor).await?;
                    reset_timer(&mut sleeper, &mut deadline, batch_max_age);
                }

                if need > batch_max_size && batch.is_empty() {
                    batch.extend_from_slice(&line_bytes);
                    batch.push(b'\n');
                    events_in_batch += 1;
                    flush_batch(&mut batch, &mut events_in_batch, store, processor).await?;
                    reset_timer(&mut sleeper, &mut deadline, batch_max_age);
                } else {
                    batch.extend_from_slice(&line_bytes);
                    batch.push(b'\n');
                    events_in_batch += 1;
                }

                line_bytes.clear();
            }

            _ = &mut sleeper => {
                if !batch.is_empty() {
                    flush_batch(&mut batch, &mut events_in_batch, store, processor).await?;
                }
                reset_timer(&mut sleeper, &mut deadline, batch_max_age);
            }
        }
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

    pub async fn spawn_workers(
        &self,
        n: usize,
        batch_max_size: usize,
        batch_max_age: Duration,
    ) -> Vec<mpsc::Sender<UnixStream>> {
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
                batch_max_size,
                batch_max_age,
            };
            task::spawn(worker.run());

            senders.push(tx);
        }
        info!("spawned {} WASM workers", n);
        senders
    }
}
