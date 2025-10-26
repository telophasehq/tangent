use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    sinks::manager::{Sink, SinkWrite},
    SINK_BYTES_TOTAL, SINK_BYTES_UNCOMPRESSED_TOTAL, SINK_OBJECTS_TOTAL,
};

#[derive(Default)]
pub struct BlackholeSink;

impl BlackholeSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl Sink for BlackholeSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        SINK_OBJECTS_TOTAL.inc();
        SINK_BYTES_TOTAL.inc_by(req.payload.len() as u64);
        SINK_BYTES_UNCOMPRESSED_TOTAL.inc_by(req.payload.len() as u64);

        Ok(())
    }
}
