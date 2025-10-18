use mdds;

// Just a thin shim that awaits a `run()` function in mdds `lib.rs`
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    mdds::run().await
}