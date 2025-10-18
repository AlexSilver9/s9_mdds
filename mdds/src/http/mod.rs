mod error;
mod market_data;

use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Context;
use axum::{Extension, Router};
pub use error::Error;
use std::sync::Arc;
use chrono::NaiveDate;
use tokio::sync::RwLock;
use tower::ServiceBuilder;

use crate::config::Config;
use tower_http::trace::TraceLayer;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The core type through which handler functions can access common API state.
/// This can be accessed by adding a parameter `Extension<ApiContext>` to a handler function's
/// parameters.
#[derive(Clone)]
struct ApiContext {
    config: Arc<Config>,
    filepath_cache: FilePathCache,
}

// File metadata used for caching
#[derive(Clone, Debug)]
struct FileMetadata {
    path: PathBuf,
    date: NaiveDate,
}

// Cache structure to avoid filesystem calls
type FilePathCache = Arc<RwLock<HashMap<String, Vec<FileMetadata>>>>;

pub async fn serve(config: Config) -> anyhow::Result<()> {
    let arc_config = Arc::new(config);
    let filepath_cache = Arc::new(RwLock::new(HashMap::new()));

    let app = api_router().layer(
        ServiceBuilder::new()
            .layer(Extension(ApiContext {
                config: Arc::clone(&arc_config),
                filepath_cache
            }))
            // Enables logging. Use `RUST_LOG=tower_http=debug`
            .layer(TraceLayer::new_for_http()),
    );

    let config = Arc::clone(&arc_config);

    let listener = tokio::net::TcpListener::bind(&config.server_address).await?;
    tracing::info!("Server listening on {}", listener.local_addr()?);

    axum::serve(listener, app)
        .await
        .context("error running server")
}

fn api_router() -> Router {
    // This is the order that the modules were authored in.
    market_data::router()
    // .merge(more::router())
}