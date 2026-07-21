// std modules
use std::{path::PathBuf, sync::Arc};

// external modules
use anyhow::Result;
use axum::{extract::DefaultBodyLimit, routing::get, Router};
use http::Method;
use tower_http::cors::{Any, CorsLayer};

// internal modules
use crate::web::results_api::ResultController;

/// Loki tracing label value for the results api server
///
static LOKI_TRACING_LABEL_VALUE: &str = "results_api_server";

pub struct Server;

impl Server {
    /// Starts a http service to submit and monitor searches
    /// and monitor the progress of the searches.
    ///
    /// # Arguments
    /// * `interface` - Interface to bind the service to
    /// * `port` - Port to bind the service to
    /// * `result_dir` - Directory to store the results
    ///
    pub async fn start(interface: String, port: u16, result_dir: PathBuf) -> Result<()> {
        let result_dir = Arc::new(result_dir);

        // Add CORS layer
        let cors = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST])
            .allow_headers(vec![http::header::ACCEPT, http::header::CONTENT_TYPE])
            .allow_origin(Any);

        // Build our application with route
        let app = Router::new()
            .route("/api/searches/:uuid", get(ResultController::show_search))
            .route(
                "/api/searches/:uuid/:ms_run",
                get(ResultController::show_ms_run),
            )
            .route(
                "/api/searches/:uuid/:ms_run/:spectrum_id",
                get(ResultController::show_spectrum),
            )
            .layer(DefaultBodyLimit::disable())
            .layer(cors)
            .with_state(result_dir);

        let listener = tokio::net::TcpListener::bind(format!("{}:{}", interface, port)).await?;
        tracing::info!("ready for connections, listening on {}", interface);
        axum::serve(listener, app).await.unwrap();

        Ok(())
    }

    pub fn loki_tracing_label_value() -> &'static str {
        LOKI_TRACING_LABEL_VALUE
    }
}
