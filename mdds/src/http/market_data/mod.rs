use crate::http::ApiContext;
use axum::extract::{Path, Query};
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use axum::response::{IntoResponse, Response};
use axum_streams::StreamBodyAs;
use futures::{stream, Stream, StreamExt};
use crate::fs::file_finder::FileFinder;
use crate::fs::TimeSlice;

pub fn router() -> Router {

    let api_path = "/api";
    let api_version = "v1";
    let stream_path = "/stream";
    let stream_version = "v1";
    let market_data_path = "market-data";
    let exchange_capture_path = "{exchange}";
    let market_type_path = "{market_type}";
    let stream_capture_path = "{stream}";
    let symbol_capture_path = "{symbol}";

    let api_route = api_path.to_string()
        + "/" + api_version
        + "/" + market_data_path
        + "/" + exchange_capture_path
        + "/" + market_type_path
        + "/" + stream_capture_path
        + "/" + symbol_capture_path;

    let stream_route = stream_path.to_string()
        + "/" + stream_version
        + "/" + market_data_path
        + "/" + exchange_capture_path
        + "/" + market_type_path
        + "/" + stream_capture_path
        + "/" + symbol_capture_path;

    // Example URLs:
    // localhost:8080/api/v1/market-data/binance/spot/trade/ethusdt?from=2025-10-15T16:21:30.160Z&to=2025-10-15T16:21:39.049Z
    // localhost:8080/api/v1/market-data/binance/spot/trade/ethusdt?from=2025-10-15T16:21:32.000Z&to=2025-10-15T16:21:32.100Z

    // localhost:8080/stream/v1/market-data/binance/spot/trade/ethusdt?from=2025-10-15T16:21:30.160Z&to=2025-10-15T16:21:39.049Z
    // localhost:8080/stream/v1/market-data/binance/spot/trade/ethusdt?from=2025-10-15T16:21:32.000Z&to=2025-10-15T16:21:32.100Z

    // Example data file paths:
    // data/market_data/binance/spot/trade/ethusdt.2019-04-05.parquet
    // data/market_data/binance/spot/trade/ethusdt.2019-04-06.parquet
    Router::new()
        .route(api_route.as_str(),get(get_market_data))
        .route(stream_route.as_str(),get(stream_market_data))
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

async fn stream_market_data(
    ctx: Extension<ApiContext>,
    Path((exchange, market_type, stream, symbol)): Path<(String, String, String, String)>,
    Query(query): Query<QueryParams>,
) -> impl IntoResponse
{
    // Validate parameters first
    if query.from.is_none() || query.to.is_none() {
        return (StatusCode::BAD_REQUEST, "Missing from/to parameters").into_response();
    }

    let stream = s_market_data(ctx, Path((exchange, market_type, stream, symbol)), Query(query)).await;
    let stream = stream.map(|result| result.map_err(|e| crate::http::Error::Anyhow(e)));

    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .header("cache-control", "no-cache")
        .header("connection", "keep-alive")
        .header("x-accel-buffering", "no") // Disable nginx buffering if behind nginx
        .body(StreamBodyAs::json_nl_with_errors(stream))
        .unwrap().into_response()
}


async fn s_market_data(
    ctx: Extension<ApiContext>,
    Path((exchange, market_type, stream, symbol)): Path<(String, String, String, String)>,
    Query(query): Query<QueryParams>,
) -> impl Stream<Item = Result<Message, anyhow::Error>>
{
    tracing::info!("loading stream market data for {}/{}/{}/{}", exchange, market_type, stream, symbol);

    let file_paths = match query.from.zip(query.to) {
        Some((from, to)) => {
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

            match file_finder.find_files().await {
                Ok(paths) => paths,
                Err(e) => return stream::once(async move { Err(anyhow::anyhow!("Failed to find files: {}", e)) }).boxed(),
            }
        }
        None => return stream::once(async move { Err(anyhow::anyhow!("Missing from/to parameters")) }).boxed(),
    };

    let from = query.from.unwrap();
    let to = query.to.unwrap();

    // Create streams for all files and merge them
    let file_streams: Vec<_> = file_paths.into_iter().map(|file_path| {
        let batch_size = ctx.config.parquet_reader_record_batch_size;
        async move {
            match stream_parquet_file(batch_size, &file_path).await {
                Ok(entry_stream) => {
                    entry_stream
                        .map(move |result| {
                            match result {
                                Ok(entry) => {
                                    let timestamp_info = entry.timestamp_info;
                                    match String::from_utf8(entry.data) {
                                        Ok(data) => {
                                            let message = Message {
                                                timestamp_millis: timestamp_info.timestamp_millis,
                                                timestamp_sec: timestamp_info.timestamp_sec,
                                                timestamp_sub_sec: timestamp_info.timestamp_sub_sec,
                                                data,
                                            };

                                            // Filter by timestamp
                                            let msg_time = DateTime::<Utc>::from_timestamp_millis(message.timestamp_millis);
                                            if let Some(msg_time) = msg_time {
                                                if msg_time >= from && msg_time <= to {
                                                    Some(Ok(message))
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        }
                                        Err(err) => {
                                            tracing::error!("Error decoding message data: {}", err);
                                            Some(Err(anyhow::anyhow!("Error decoding message data: {}", err)))
                                        }
                                    }
                                }
                                Err(e) => Some(Err(anyhow::anyhow!("Error reading entry: {:?}", e))),
                            }
                        })
                        .filter_map(|item| async move { item })
                        .boxed()
                }
                Err(_) => stream::once(async move { Err(anyhow::anyhow!("Failed to stream parquet file")) }).boxed(),
            }
        }
    }).collect();

    // Convert the vector of futures into a stream and flatten
    stream::iter(file_streams)
        .then(|fut| fut)
        .flatten()
        .boxed()
}


async fn get_market_data(
    ctx: Extension<ApiContext>,
    Path((exchange, market_type, stream, symbol)): Path<(String, String, String, String)>,
    Query(query): Query<QueryParams>,
) -> anyhow::Result<Json<ApiResponse<Vec<Message>>>, StatusCode>
{
    tracing::info!("loading batch market data for {}/{}/{}/{}", exchange, market_type, stream, symbol);

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

async fn stream_parquet_file(
    parquet_reader_record_batch_size: usize,
    file_path: &PathBuf
) -> Result<impl Stream<Item = Result<s9_parquet::Entry, Box<dyn std::error::Error + Send + Sync>>>, StatusCode>
{
    let reader = s9_parquet::AsyncParquetReader::new(file_path, parquet_reader_record_batch_size).await
        .map_err(|err| {
            tracing::error!("Error reading parquet file: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(reader.into_entry_stream())
}