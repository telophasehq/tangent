use actix_web::{get, post, web, App, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};
use wasmtime::{Engine, Linker, Module, Store, TypedFunc};
use wasmtime_wasi::{add_to_linker, WasiCtxBuilder};

// Define the log entry structure to match incoming payload
#[derive(Debug, Serialize, Deserialize)]
struct LogEntry {
    host: String,
    message: String,
    source_type: String,
    timestamp: String,
}

// WASM engine state
struct WasmEngine {
    engine: Engine,
    module: Module,
}

impl WasmEngine {
    pub fn new() -> Result<Self, anyhow::Error> {
        let engine = Engine::default();

        // TODO: dynamically create modules from tangent.yaml
        match Module::from_file(
            &engine,
            "/Users/ethanblackburn/tangent/compiled/hello_world.wasm",
        ) {
            Ok(module) => Ok(WasmEngine { engine, module }),
            Err(message) => {
                println!("Error finding module: {message}");
                return Err(message);
            }
        }
    }

    pub fn process_logs(&self, logs_json: &str) -> Result<String, anyhow::Error> {
        let wasi = WasiCtxBuilder::new()
            .inherit_stdout()
            .inherit_stderr()
            .build();
        let mut store = Store::new(&self.engine, wasi);
        let mut linker = Linker::new(&self.engine);
        add_to_linker(&mut linker, |cx| cx)?;

        let instance = linker.instantiate(&mut store, &self.module)?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow::anyhow!("no `memory` export"))?;

        let alloc: TypedFunc<i32, i32> = instance.get_typed_func(&mut store, "alloc")?;
        let free: TypedFunc<(i32, i32), ()> = instance.get_typed_func(&mut store, "free")?;

        let process: TypedFunc<(i32, i32), (i32, i32)> =
            instance.get_typed_func(&mut store, "process_logs")?;

        let input = logs_json.as_bytes();
        let in_len = input.len() as i32;
        let in_ptr = alloc.call(&mut store, in_len)?;
        memory
            .write(&mut store, in_ptr as usize, input)
            .map_err(|e| anyhow::anyhow!("write to guest memory: {e}"))?;

        let (out_ptr, out_len) = process.call(&mut store, (in_ptr, in_len))?;

        let mut out = vec![0u8; out_len as usize];
        memory
            .read(&mut store, out_ptr as usize, &mut out)
            .map_err(|e| anyhow::anyhow!("read from guest memory: {e}"))?;

        free.call(&mut store, (in_ptr, in_len))?;
        free.call(&mut store, (out_ptr, out_len))?;

        let s = String::from_utf8(out)?;
        Ok(s)
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    if std::env::var("LOG_LEVEL").is_err() {
        std::env::set_var("LOG_LEVEL", "info");
    }

    tracing_subscriber::fmt::init();

    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".into())
        .parse::<u16>()
        .expect("PORT must be a valid u16");

    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    info!("Starting Actix Web HTTP server on {}", addr);

    let wasm_engine = match WasmEngine::new() {
        Ok(engine) => {
            info!("WASM engine initialized successfully");
            Arc::new(engine)
        }
        Err(e) => {
            error!("Failed to initialize WASM engine: {}", e);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ));
        }
    };

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(wasm_engine.clone()))
            .service(sink_handler)
            .service(health_handler)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}

#[post("/sink")]
async fn sink_handler(
    payload: web::Json<Value>,
    wasm_engine: web::Data<Arc<WasmEngine>>,
) -> impl Responder {
    info!("=== POST Request to /sink ===");
    let payload_value = payload.into_inner();
    info!(
        "Payload: {}",
        serde_json::to_string_pretty(&payload_value).unwrap()
    );

    let logs_json = serde_json::to_string(&payload_value).unwrap();
    match wasm_engine.process_logs(&logs_json) {
        Ok(analysis) => {
            info!("Log analysis completed");
            info!("Analysis: {}", analysis);

            match serde_json::from_str::<Value>(&analysis) {
                Ok(analysis_json) => web::Json(json!({
                    "status": "processed",
                    "analysis": analysis_json
                })),
                Err(e) => {
                    error!("Failed to parse analysis JSON: {}", e);
                    web::Json(json!({
                        "status": "error",
                        "error": "Failed to parse analysis"
                    }))
                }
            }
        }
        Err(e) => {
            error!("Failed to process logs: {}", e);
            web::Json(json!({
                "status": "error",
                "error": format!("Failed to process logs: {}", e)
            }))
        }
    }
}

#[get("/health")]
async fn health_handler() -> impl Responder {
    web::Json(json!({ "status": "healthy" }))
}
