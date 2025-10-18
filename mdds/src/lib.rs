mod config;

use std::env;
use anyhow::Error;
use axum::{Json, Router};
use axum::http::StatusCode;
use axum::routing::get;
use clap::Parser;
use s9_parquet::{TimestampInfo};
use serde::Serialize;
use tower_http::cors::CorsLayer;
use crate::config::Config;

#[derive(Debug, Serialize)]
struct Message {
    pub timestamp_millis: i64,
    pub timestamp_sec: i64,
    pub timestamp_sub_sec: i32,
    data: String,
}

#[derive(Debug, Serialize)]
struct ApiResponse<M> {
    messages: M,
}

pub async fn run() -> anyhow::Result<(), Error> {
    // Load .env file if it exists
    dotenv::dotenv().ok();

    // Initialize tracing/logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Load configuration from environment variables
    let config = Config::parse();

    start_server(config).await
}

async fn start_server(config: Config) -> anyhow::Result<(), Error> {
    let app = Router::new()
        .route("/api/v1/market-data", get(get_market_data))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(config.server_address).await?;
    tracing::info!("Server listening on {}", listener.local_addr()?);

    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_market_data() -> Result<Json<ApiResponse<Vec<Message>>>, StatusCode> {
    tracing::info!("Loading market data");

    // TODO: Make configurable
    let batch_size = 1024;

    let reader = s9_parquet::AsyncParquetReader::new("data/market_data/ethusdt.trade.parquet", batch_size).await
        .map_err(|err| {
            tracing::error!("Error reading parquet file: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let entries = reader.read().await
        .map_err(|err| {
            tracing::error!("Error reading entries from parquet file: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut messages: Vec<Message> = Vec::with_capacity(entries.len());
    for entry in entries {
        let timestamp_info = entry.timestamp_info;
        let data = String::from_utf8(entry.data);
        let data = match data {
            Ok(data) => data,
            Err(err) => {
                tracing::error!("Error decoding message data: {}", err);
                continue;
            }
        };
        let message = Message {
            timestamp_millis: timestamp_info.timestamp_millis,
            timestamp_sec: timestamp_info.timestamp_sec,
            timestamp_sub_sec: timestamp_info.timestamp_sub_sec,
            data: data,
        };
        messages.push(message);
    }

    Ok(Json(ApiResponse{
        messages: messages,
    }))
}