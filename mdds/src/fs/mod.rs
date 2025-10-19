pub mod file_finder;

use chrono::{DateTime, NaiveDate, Utc};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub path: PathBuf,
    pub date: NaiveDate,
}

// TODO: Move to separate module
#[derive(Clone, Copy, Debug)]
pub struct TimeSlice<'a> {
    pub from: &'a DateTime<Utc>,
    pub to: &'a DateTime<Utc>,
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
