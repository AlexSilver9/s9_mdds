use chrono::{DateTime, NaiveDate, Utc};
use std::path::PathBuf;
use tokio::fs;

#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub path: PathBuf,
    pub date: NaiveDate,
}

#[derive(Clone, Copy, Debug)]
pub struct FindFileParams<'a> {
    pub parquet_file_extension: &'a str,
    pub base_path: &'a str,
    pub exchange: &'a str,
    pub market_type: &'a str,
    pub stream: &'a str,
    pub symbol: &'a str,
    pub time_slice: &'a TimeSlice<'a>,
}

impl FindFileParams<'_> {
    pub fn get_symbol_path(&self) -> String {
        let mut symbol_path = PathBuf::from(self.base_path);
        symbol_path.push(self.exchange);
        symbol_path.push(self.market_type);
        symbol_path.push(self.stream);
        symbol_path.to_string_lossy().to_string()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TimeSlice<'a> {
    pub from: &'a DateTime<Utc>,
    pub to: &'a DateTime<Utc>,
}

pub fn files_in_time_slice(file_metadata: &Vec<FileMetadata>, time_slice: &TimeSlice) -> Vec<PathBuf> {
    let files: Vec<PathBuf> = file_metadata
        .iter()
        .filter(|file_meta| {
            file_meta.date.is_within(time_slice)
        })
        .map(|file_meta| file_meta.path.clone())
        .collect();
    files
}

// TODO: Move to separate module
trait IsWithin {
    fn is_within(&self, time_slice: &TimeSlice) -> bool;
}

// TODO: Move to separate module
impl IsWithin for NaiveDate {
    fn is_within(&self, time_slice: &TimeSlice) -> bool {
        let from = time_slice.from.date_naive();
        let to = time_slice.to.date_naive();
        self >= &from && self <= &to
    }
}

pub async fn scan_directory_for_files(params: FindFileParams<'_>) -> anyhow::Result<Vec<FileMetadata>> {
    let symbol_path = params.get_symbol_path();

    let mut entries = fs::read_dir(&symbol_path).await?;
    let mut file_metadata = Vec::new();
    let file_extension = format!(".{}", params.parquet_file_extension);

    while let Some(entry) = entries.next_entry().await? {
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();

        if let Some(date_str) = extract_date_from_filename(&filename_str, params.symbol, &file_extension) {
            if let Ok(file_date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                let file_meta = FileMetadata {
                    path: entry.path(),
                    date: file_date,
                };
                file_metadata.push(file_meta);
            }
        }
    }

    file_metadata.sort_by(|a, b| a.date.cmp(&b.date));
    Ok(file_metadata)
}


fn extract_date_from_filename(filename: &str, symbol: &str, file_extension: &str) -> Option<String> {
    // Extract date from e.g.: ethusdt.2019-04-05.parquet
    let prefix = format!("{}.", symbol);
    if filename.starts_with(&prefix) && filename.ends_with(&file_extension) {
        let date = &filename[prefix.len()..filename.len()-file_extension.len()]; // Remove .parquet
        Some(date.to_string())
    } else {
        None
    }
}