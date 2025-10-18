/// The configuration parameters for the application.
///
/// These can either be passed on the command line, or pulled from environment variables (preferred).
///
/// For development convenience, these can also be read from a `.env` file in the working
/// directory where the application is started. See `.env.sample` in the repository root for details.

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Config {
    // The address of the mdds http server to listen for client requests.
    #[clap(long, env = "MDDS_SERVER_ADDRESS", default_value = "0.0.0.0:8080" )]
    pub server_address: String,

    /// The base path for data files.
    #[clap(long, env = "MDDS_PARQUET_DATA_PATH", default_value = "data" )]
    pub parquet_data_path: String,

    // The base path containing the market data files.
    #[clap(long, env = "MDDS_MARKET_DATA_PATH", default_value = "data/market_data" )]
    pub market_data_path: String,

    /// The size of a record batch when reading parquet files.
    #[clap(long, env = "MDDS_PARQUET_READER_RECORD_BATCH_SIZE", default_value_t = 1024)]
    pub parquet_reader_record_batch_size: usize,

    // The base path containing the market data files.
    #[clap(long, env = "MDDS_PARQUET_FILE_EXTENSION", default_value = "parquet" )]
    pub parquet_file_extension: String,

}