use actix_web::{get, post, web, App, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};
use wasmtime::*;

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
    fn new() -> Result<Self, anyhow::Error> {
        let engine = Engine::default();
        
        // For now, we'll use a simple WASM module since we don't have the compiled one yet
        // In production, you'd load the compiled WASM module
        let module = Module::from_file(&engine, "examples/log-processor.wat")
            .or_else(|_| {
                // Fallback: create a simple module if file doesn't exist
                let wat = r#"
                    (module
                        (func $process_logs (param i32) (result i32)
                            i32.const 0
                        )
                        (export "process_logs" (func $process_logs))
                    )
                "#;
                Module::new(&engine, wat)
            })?;
        
        Ok(WasmEngine { engine, module })
    }
    
    fn process_logs(&self, logs_json: &str) -> Result<String, anyhow::Error> {
        // For now, return a simple analysis since we need to build the WASM module first
        let logs: Vec<LogEntry> = serde_json::from_str(logs_json)?;
        
        let mut error_count = 0;
        let mut warn_count = 0;
        let mut info_count = 0;
        let mut suspicious_patterns = Vec::new();
        let mut hosts = Vec::new();
        let mut source_types = Vec::new();
        
        for log in &logs {
            let lower_message = log.message.to_lowercase();
            
            // Count log levels based on message content
            if lower_message.contains("error") {
                error_count += 1;
            } else if lower_message.contains("warn") {
                warn_count += 1;
            } else if lower_message.contains("info") {
                info_count += 1;
            }
            
            // Check for suspicious patterns
            if lower_message.contains("password") || lower_message.contains("secret") || lower_message.contains("key") {
                suspicious_patterns.push("Potential credential leak detected".to_string());
            }
            
            if lower_message.contains("exception") || lower_message.contains("stack trace") {
                suspicious_patterns.push("Exception or stack trace detected".to_string());
            }
            
            if lower_message.contains("timeout") || lower_message.contains("deadlock") {
                suspicious_patterns.push("Performance issue detected".to_string());
            }
            
            // Collect unique hosts and source types
            if !hosts.contains(&log.host) {
                hosts.push(log.host.clone());
            }
            if !source_types.contains(&log.source_type) {
                source_types.push(log.source_type.clone());
            }
        }
        
        let analysis = json!({
            "total_logs": logs.len(),
            "error_count": error_count,
            "warn_count": warn_count,
            "info_count": info_count,
            "debug_count": 0,
            "suspicious_patterns": suspicious_patterns,
            "hosts": hosts,
            "source_types": source_types
        });
        
        Ok(analysis.to_string())
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    tracing_subscriber::fmt::init();

    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".into())
        .parse::<u16>()
        .expect("PORT must be a valid u16");

    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    info!("Starting Actix Web HTTP server on {}", addr);

    // Initialize WASM engine
    let wasm_engine = match WasmEngine::new() {
        Ok(engine) => {
            info!("WASM engine initialized successfully");
            Arc::new(engine)
        }
        Err(e) => {
            error!("Failed to initialize WASM engine: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
        }
    };

    // Start the server
    info!("Server listening on {}", addr);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(wasm_engine.clone()))
            .service(sink_handler)
            .service(health_handler)
    })
    .bind(("127.0.0.1", port))?
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
    info!("Payload: {}", serde_json::to_string_pretty(&payload_value).unwrap());
    
    // Process logs with WASM
    let logs_json = serde_json::to_string(&payload_value).unwrap();
    match wasm_engine.process_logs(&logs_json) {
        Ok(analysis) => {
            info!("Log analysis completed");
            info!("Analysis: {}", analysis);
            
            // Parse the analysis back to JSON for response
            match serde_json::from_str::<Value>(&analysis) {
                Ok(analysis_json) => {
                    web::Json(json!({
                        "status": "processed",
                        "analysis": analysis_json
                    }))
                }
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
