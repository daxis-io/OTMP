//! Query a local OTMP table: `cargo run -p otmp-datafusion --example query -- TABLE 'SELECT * FROM t'`.
use datafusion::prelude::SessionContext;
use otmp::{LocalObjectStore, MetadataSelection, ReaderOptions, SnapshotSelection, Table};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: query <table-directory> [SQL over t]")?;
    let sql = args
        .next()
        .unwrap_or_else(|| "SELECT * FROM t LIMIT 20".into());
    let table = Table::new(LocalObjectStore::new(root)?);
    let provider = OtmpTableProvider::open(
        &table,
        MetadataSelection::Current,
        SnapshotSelection::Ref("main".into()),
        ReaderOptions::default(),
        ProviderOptions::default(),
    )
    .await?;
    let context = SessionContext::new();
    context.register_table("t", Arc::new(provider))?;
    context.sql(&sql).await?.show().await?;
    Ok(())
}
