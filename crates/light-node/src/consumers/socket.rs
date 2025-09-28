// consumers/socket.rs
use anyhow::{Context, Result};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};
use tangent_shared::socket::SocketConfig;
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;

use crate::worker::{Incoming, WorkerPool};

pub async fn run_consumer(
    cfg: SocketConfig,
    pool: std::sync::Arc<WorkerPool>,
    shutdown: CancellationToken,
) -> Result<()> {
    let path = Path::new(&cfg.socket_path);
    if path.exists() {
        fs::remove_file(path)
            .with_context(|| format!("removing stale socket {}", cfg.socket_path.display()))?;
    }

    let listener = UnixListener::bind(path)
        .with_context(|| format!("binding unix socket {}", cfg.socket_path.display()))?;

    if let Ok(meta) = fs::metadata(path) {
        let mut perm = meta.permissions();
        perm.set_mode(0o666);
        let _ = fs::set_permissions(path, perm);
    }

    tracing::info!("unix socket listening at {}", cfg.socket_path.display());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("socket consumer shutdown requested");
                break;
            }
            res = listener.accept() => {
                match res {
                    Ok((us, _addr)) => {
                        pool.dispatch(Incoming::UnixStream(us)).await;
                    }
                    Err(e) => {
                        tracing::warn!("unix accept error: {e}");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }

    if let Err(e) = fs::remove_file(path) {
        tracing::debug!(
            "failed to remove socket {} on shutdown: {e}",
            cfg.socket_path.display()
        );
    }

    Ok(())
}
