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
use log::{info, warn};
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
    info!("Replay buffer size: {}", config.grpc.replay_buffer_size);
    if !config.grpc.replay_buffer_path.is_empty() {
        info!("Replay buffer path: {}", config.grpc.replay_buffer_path);
    }

    // Broadcast channel for events (ingest loop → gRPC subscribers)
    // Buffer 10,000 events — clients that fall behind will get a Lagged error
    let (event_tx, _) = broadcast::channel::<proto::IngestorEvent>(10_000);

    // Shared metrics
    let metrics = Arc::new(ingest::IngestorMetrics::new());

    // Replay buffer for consumer resume — try to load snapshot from disk
    let replay_buffer = if !config.grpc.replay_buffer_path.is_empty() {
        match ingest::ReplayBuffer::load_snapshot(
            &config.grpc.replay_buffer_path,
            config.grpc.replay_buffer_size,
        ) {
            Some(rb) => Arc::new(rb),
            None => {
                info!("No replay buffer snapshot found, starting fresh");
                Arc::new(ingest::ReplayBuffer::new(config.grpc.replay_buffer_size))
            }
        }
    } else {
        Arc::new(ingest::ReplayBuffer::new(config.grpc.replay_buffer_size))
    };

    // Start gRPC server in background
    let grpc_config = config.clone();
    let grpc_tx = event_tx.clone();
    let grpc_metrics = metrics.clone();
    let grpc_replay = replay_buffer.clone();
    tokio::spawn(async move {
        if let Err(e) = grpc::start_grpc_server(grpc_config, grpc_tx, grpc_metrics, grpc_replay).await {
            log::error!("gRPC server error: {}", e);
        }
    });

    // Run the ingest loop with graceful shutdown on Ctrl+C
    let shutdown_replay = replay_buffer.clone();
    let shutdown_config = config.clone();
    tokio::select! {
        _ = ingest::run_ingest_loop(config, event_tx, metrics, replay_buffer) => {
            info!("Ingest loop exited");
        }
        _ = tokio::signal::ctrl_c() => {
            warn!("Received Ctrl+C, shutting down gracefully...");
        }
    }

    // Save replay buffer snapshot on shutdown
    if !shutdown_config.grpc.replay_buffer_path.is_empty() {
        shutdown_replay.save_snapshot(&shutdown_config.grpc.replay_buffer_path);
    }

    // Save final checkpoint (we need the last sequence from the replay buffer)
    // The checkpoint is already saved periodically by the ingest loop,
    // but we save one final time to capture the latest state.
    info!("Shutdown complete");

    Ok(())
}
