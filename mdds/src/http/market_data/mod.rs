use crate::http::ApiContext;
use axum::extract::{Path, Query};
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use crate::fs::file_finder::FileFinder;
use crate::fs::TimeSlice;

pub fn router() -> Router {

    let api_path = "/api";
    let api_version = "v1";
    let market_data_path = "market-data";
    let exchange_capture_path = "{exchange}";
    let market_type_path = "{market_type}";
    let stream_capture_path = "{stream}";
    let symbol_capture_path = "{symbol}";

    let route = api_path.to_string()
        + "/" + api_version
        + "/" + market_data_path
        + "/" + exchange_capture_path
        + "/" + market_type_path
        + "/" + stream_capture_path
        + "/" + symbol_capture_path;

    // Example URLs:
    // localhost:8080/api/v1/market-data/binance/spot/trade/ethusdt?from=2025-10-15T16:21:30.160Z&to=2025-10-15T16:21:39.049Z
    // localhost:8080/api/v1/market-data/binance/spot/trade/ethusdt?from=2025-10-15T16:21:32.000Z&to=2025-10-15T16:21:32.100Z

    // Example data file paths:
    // data/market_data/binance/spot/trade/ethusdt.2019-04-05.parquet
    // data/market_data/binance/spot/trade/ethusdt.2019-04-06.parquet
    Router::new().route(route.as_str(),get(get_market_data))
}

#[derive(Deserialize)]
struct QueryParams {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

// TODO: Move this to a separate codec repo to share with adapters and s9_parquet
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

async fn get_market_data(
    ctx: Extension<ApiContext>,
    Path((exchange, market_type, stream, symbol)): Path<(String, String, String, String)>,
    Query(query): Query<QueryParams>,
) -> anyhow::Result<Json<ApiResponse<Vec<Message>>>, StatusCode>
{
    tracing::info!("loading market data for {}/{}/{}/{}", exchange, market_type, stream, symbol);

    let file_paths = if let (Some(from), Some(to)) = (query.from, query.to) {
        // Multi-file query for date range
        let file_finder = FileFinder {
            parquet_file_extension: &ctx.config.parquet_file_extension,
            base_path: &ctx.config.market_data_path,
            exchange: &exchange,
            market_type: &market_type,
            stream: &stream,
            symbol: &symbol,
            time_slice: &TimeSlice {
                from: &from,
                to: &to,
            },
        };

        file_finder.find_files().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        return Err(StatusCode::BAD_REQUEST);
    };

    let mut all_messages = Vec::new();

    if ! file_paths.is_empty() {
        for file_path in file_paths {
            let messages = read_parquet_file(&ctx, &file_path).await?;
            all_messages.extend(messages);
        }

        // Filter by exact timestamps
        if let (Some(from), Some(to)) = (query.from, query.to) {
            all_messages.retain(|msg| {
                let msg_time = DateTime::<Utc>::from_timestamp_millis(msg.timestamp_millis);
                if let Some(msg_time) = msg_time {
                    msg_time >= from && msg_time <= to
                } else {
                    false
                }
            });
        }
    }

    Ok(Json(ApiResponse{ messages: all_messages }))
}

async fn read_parquet_file(ctx: &Extension<ApiContext>, file_path: &PathBuf) -> anyhow::Result<Vec<Message>, StatusCode> {
    let batch_size = &ctx.config.parquet_reader_record_batch_size;
    let reader = s9_parquet::AsyncParquetReader::new(file_path, *batch_size).await
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
            data,
        };
        messages.push(message);
    }
    Ok(messages)
}