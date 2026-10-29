use relayhub::connector::{ConnectorConfig, run_connector};
use std::{path::Path, process::ExitCode};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() == 1 && args[0] == "version" {
        println!("relayhub {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    if args.len() != 3 || args[0] != "connect" || args[1] != "--config" {
        eprintln!("usage: relayhub connect --config <path> | relayhub version");
        return ExitCode::from(2);
    }
    let result = match ConnectorConfig::from_file(Path::new(&args[2])) {
        Ok(config) => run_connector(config).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}
