use std::path::PathBuf;
use chrono::NaiveDate;
use tokio::fs;
use crate::fs::{FileMetadata, IsWithin, TimeSlice};

#[derive(Clone, Copy, Debug)]
pub struct FileFinder<'a> {
    pub parquet_file_extension: &'a str,
    pub base_path: &'a str,
    pub exchange: &'a str,
    pub market_type: &'a str,
    pub stream: &'a str,
    pub symbol: &'a str,
    pub time_slice: &'a TimeSlice<'a>,
}

impl FileFinder<'_> {

    pub async fn find_files(&self) -> anyhow::Result<Vec<PathBuf>> {
        // Find, filter and return matching files
        let files = self.files_for_symbol().await?;
        let files = self.files_in_time_slice(&files);
        Ok(files)
    }

    async fn files_for_symbol(&self) -> anyhow::Result<Vec<FileMetadata>> {
        let path = self.path_for_symbol();

        let mut entries = fs::read_dir(&path).await?;
        let mut file_metas = Vec::new();

        let file_prefix = format!("{}.", self.symbol);
        let file_extension = format!(".{}", self.parquet_file_extension);

        while let Some(entry) = entries.next_entry().await? {
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if let Some(date_str) = self.extract_date_from_filename(&filename_str, &file_prefix, &file_extension) {
                if let Ok(file_date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                    let file_meta = FileMetadata {
                        path: entry.path(),
                        date: file_date,
                    };
                    file_metas.push(file_meta);
                }
            }
        }

        file_metas.sort_by(|a, b| a.date.cmp(&b.date));
        Ok(file_metas)
    }

    fn path_for_symbol(&self) -> String {
        let mut path = PathBuf::from(self.base_path);
        path.push(self.exchange);
        path.push(self.market_type);
        path.push(self.stream);
        path.to_string_lossy().to_string()
    }

    fn files_in_time_slice(&self, file_metadata: &Vec<FileMetadata>) -> Vec<PathBuf> {
        let files: Vec<PathBuf> = file_metadata
            .iter()
            .filter(|file_meta| file_meta.date.is_within(self.time_slice))
            .map(|file_meta| file_meta.path.clone())
            .collect();
        files
    }

    fn extract_date_from_filename(&self, filename: &str, prefix: &str, file_extension: &str) -> Option<String> {
        // Extract date from e.g.: ethusdt.2019-04-05.parquet
        if filename.starts_with(&prefix) && filename.ends_with(&file_extension) {
            let date = &filename[prefix.len()..filename.len() - file_extension.len()];
            Some(date.to_string())
        } else {
            None
        }
    }
}

