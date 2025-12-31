use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use bytes::BytesMut;
use tangent_shared::{dag::NodeRef, sources::wasm::WasmSourceConfig};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::{cache::CacheHandle, router::Router, wasm::engine::WasmEngine};

pub async fn run_consumer(
    name: Arc<str>,
    cfg: WasmSourceConfig,
    config_dir: PathBuf,
    cache: Arc<CacheHandle>,
    disable_remote_calls: bool,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let poll_interval = Duration::from_millis(cfg.poll_interval_ms);
    let component_path = config_dir.join(&cfg.path).canonicalize()?;

    let mut engine = WasmEngine::new(cache, disable_remote_calls)?;
    let component = engine.load_precompiled(Arc::clone(&name), &component_path, cfg.config)?;
    let mut store = engine.make_store(&name);
    let source = engine.make_source(&mut store, &component).await?;
    let guest = source.tangent_logs_source_provider();

    let meta = guest.call_metadata(&mut store).await?;
    tracing::info!(
        cfg_name = %name,
        wasm_name = meta.name,
        wasm_version = meta.version,
        path = %component_path.display(),
        "starting wasm source",
    );

    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,

            res = guest.call_next(&mut store) => {
                match res {
                    Ok(Ok(frames)) => {
                        if frames.is_empty() {
                            continue;
                        }

                        let mut payloads = Vec::with_capacity(frames.len());
                        for mut frame in frames {
                            if !frame.ends_with(b"\n") {
                                frame.push(b'\n');
                            }

                            payloads.push(BytesMut::from(frame.as_slice()));
                        }

                        if let Err(e) = router.forward(&from, payloads, Vec::new()).await {
                            tracing::error!(?e, source = %name, "push_from_wasm_source failed");
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::error!(error = e, source = %name, "wasm source returned error");
                    }
                    Err(e) => {
                        tracing::error!(?e, source = %name, "wasm source trap");
                    }
                }
            }
        }

        sleep(poll_interval).await;
    }

    Ok(())
}
