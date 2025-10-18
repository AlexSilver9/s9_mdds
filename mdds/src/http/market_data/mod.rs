use crate::http::ApiContext;
use axum::routing::get;
use axum::{Extension, Json, Router};
use http::StatusCode;
use serde::Serialize;
use std::path::PathBuf;
use axum::extract::Path;

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

    // localhost:8080/api/v1/market-data/binance/spot/trade/ethusdt?from=2019-04-05T00:02:00.000Z&to=2019-04-06T00:03:00.000Z
    // data/market_data/binance/spot/trade/ethusdt.2019-04-05.parquet
    // data/market_data/binance/spot/trade/ethusdt.2019-04-06.parquet

    // localhost:8080/api/v1/market-data/binance/spot/trade/ethusdt
    // data/market_data/binance/spot/trade/ethusdt.parquet
    Router::new().route(route.as_str(),get(get_market_data))
}

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
) -> Result<Json<ApiResponse<Vec<Message>>>, StatusCode>
{
    tracing::info!("Loading market data for {}/{}/{}/{}", exchange, market_type, stream, symbol);

    let batch_size = &ctx.config.parquet_reader_record_batch_size;
    let mut market_data_path = PathBuf::from(&ctx.config.market_data_path);

    let parquet_file_extension = &ctx.config.parquet_file_extension;
    let file_name = format!("{}.{}", symbol, parquet_file_extension);

    market_data_path.push(exchange);
    market_data_path.push(market_type);
    market_data_path.push(stream);
    market_data_path.push(file_name);

    let file_path = match market_data_path.as_path().to_str() {
        Some(path) => path,
        None => {
            tracing::error!("Invalid market data path: {}", market_data_path.display());
            return Err(StatusCode::NOT_FOUND);
        }
    };

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
            data: data,
        };
        messages.push(message);
    }

    Ok(Json(ApiResponse{
        messages: messages,
    }))
}