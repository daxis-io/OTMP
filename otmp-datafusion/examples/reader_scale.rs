//! Controlled reader qualification; see qualification/reader-scale/README.md.
#[path = "reader_scale/fixture.rs"]
mod fixture;
#[path = "reader_scale/transport.rs"]
mod transport;
#[path = "reader_scale/worker.rs"]
mod worker;
#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), fixture::Error> {
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .try_init()?;
    }
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let command = args.first().and_then(|value| value.to_str());
    let root = args
        .get(1)
        .map(std::path::Path::new)
        .ok_or("usage: reader_scale prepare|run ROOT CONFIG.json; reader_scale tail|verify ROOT")?;
    let result = match command {
        Some("prepare") if args.len() == 3 => {
            let config = serde_json::from_slice(&std::fs::read(&args[2])?)?;
            fixture::prepare(root, config).await?
        }
        Some("run") if args.len() == 3 => {
            let config = serde_json::from_slice(&std::fs::read(&args[2])?)?;
            worker::run(root, config).await?
        }
        Some("tail") if args.len() == 2 => fixture::tail(root).await?,
        Some("verify") if args.len() == 2 => fixture::verify(root).await?,
        _ => {
            return Err(
                "usage: reader_scale prepare|run ROOT CONFIG.json; reader_scale tail|verify ROOT"
                    .into(),
            );
        }
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
