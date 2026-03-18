mod config;
mod grpc;
mod ingest;
mod normalize;

/// Generated protobuf types
pub mod proto {
    tonic::include_proto!("kaspa.ingestor");
}

use std::sync::Arc;

use clap::Parser;
use log::info;
use tokio::sync::broadcast;

#[derive(Parser)]
#[command(name = "kaspa-ingestor")]
#[command(about = "Un-opinionated Kaspa blockchain data ingestor with gRPC streaming")]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    let config = config::Config::from_file(&cli.config)?;

    info!("kaspa-ingestor v{}", env!("CARGO_PKG_VERSION"));
    info!("Node: {}", config.node.url);
    info!("Network: {}", config.node.network);
    info!("gRPC bind: {}", config.grpc.bind_address);
    info!(
        "Poll interval: {}ms, tip distance: {}",
        config.ingest.poll_interval_ms, config.ingest.tip_distance
    );
    if !config.ingest.checkpoint_path.is_empty() {
        info!(
            "Checkpoint: {} (every {}s)",
            config.ingest.checkpoint_path, config.ingest.checkpoint_interval_secs
        );
    } else {
        info!("Checkpointing disabled");
    }

    // Broadcast channel for events (ingest loop → gRPC subscribers)
    // Buffer 10,000 events — clients that fall behind will get a Lagged error
    let (event_tx, _) = broadcast::channel::<proto::IngestorEvent>(10_000);

    // Shared metrics
    let metrics = Arc::new(ingest::IngestorMetrics::new());

    // Start gRPC server in background
    let grpc_config = config.clone();
    let grpc_tx = event_tx.clone();
    let grpc_metrics = metrics.clone();
    tokio::spawn(async move {
        if let Err(e) = grpc::start_grpc_server(grpc_config, grpc_tx, grpc_metrics).await {
            log::error!("gRPC server error: {}", e);
        }
    });

    // Run the ingest loop (blocking — reconnects on failure)
    ingest::run_ingest_loop(config, event_tx, metrics).await;

    Ok(())
}
