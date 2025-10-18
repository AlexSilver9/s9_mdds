mod config;
mod http;

use crate::config::Config;
use anyhow::Error;
use clap::Parser;

pub async fn run() -> anyhow::Result<(), Error> {
    // Load .env file if it exists
    dotenv::dotenv().ok();

    // Initialize tracing/logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Load configuration from environment variables
    let config = Config::parse();

    http::serve(config).await
}