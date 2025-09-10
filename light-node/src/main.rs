use actix_web::{get, post, web, App, HttpServer, Responder};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

mod wasm;

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

    let wasm_engine = match wasm::WasmEngine::new() {
        Ok(engine) => {
            info!("WASM engine initialized successfully");
            Arc::new(engine)
        }
        Err(e) => {
            tracing::error!("Failed to initialize WASM engine: {:#}", e);
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
    wasm_engine: web::Data<Arc<wasm::WasmEngine>>,
) -> impl Responder {
    info!("=== POST Request to /sink ===");
    let payload_value = payload.into_inner();
    info!(
        "Payload: {}",
        serde_json::to_string_pretty(&payload_value).unwrap()
    );

    let logs_json = serde_json::to_string(&payload_value).unwrap();
    match wasm_engine.process_logs(&logs_json).await {
        Ok(()) => web::Json(json!({
            "status": "processed",
        })),
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
