use std::path::Path;

use otmp::write_latency_qualification::worker::{self, WorkerError};

#[tokio::main]
async fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match execute(&arguments).await {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("{error}");
            println!("{}", serde_json::to_string(&error.output()).unwrap());
            std::process::exit(1);
        }
    }
}

async fn execute(arguments: &[String]) -> Result<String, WorkerError> {
    match arguments {
        [command, root, config] if command == "prepare" => {
            serde_json::to_string(&worker::prepare(Path::new(root), Path::new(config)).await?)
                .map_err(Into::into)
        }
        [command, root, config] if command == "run" => {
            serde_json::to_string(&worker::run(Path::new(root), Path::new(config)).await?)
                .map_err(Into::into)
        }
        [command, root] if command == "verify" => {
            serde_json::to_string(&worker::verify(Path::new(root)).await?).map_err(Into::into)
        }
        _ => Err(WorkerError::Config(
            "expected prepare ROOT CONFIG.json, run ROOT CONFIG.json, or verify ROOT".into(),
        )),
    }
}
