/// Ingestion loop: connects to a rusty-kaspa node via wRPC and polls
/// the virtual chain for accepted blocks and transactions.
///
/// Supports two modes:
/// - VSPCv2 (v1.1.0+): single call with full tx data
/// - V1 fallback (v1.0.x): get_virtual_chain_from_block + get_blocks
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::RpcHash;
use kaspa_wrpc_client::client::ConnectOptions;
use kaspa_wrpc_client::prelude::*;
use kaspa_wrpc_client::KaspaRpcClient;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::config::Config;
use crate::normalize;
use crate::proto;

#[derive(Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub hash: String,
    pub sequence: u64,
    pub timestamp: u64,
}

impl Checkpoint {
    fn now_epoch() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    pub fn load(path: &str) -> Option<Self> {
        let data = std::fs::read_to_string(path).ok()?;
        let cp: Self = serde_json::from_str(&data).ok()?;
        info!("Loaded checkpoint: hash={}, seq={}", cp.hash, cp.sequence);
        Some(cp)
    }

    pub fn save(path: &str, hash: &str, sequence: u64) {
        let cp = Checkpoint {
            hash: hash.to_string(),
            sequence,
            timestamp: Self::now_epoch(),
        };
        let json = match serde_json::to_string_pretty(&cp) {
            Ok(j) => j,
            Err(e) => {
                error!("Failed to serialize checkpoint: {}", e);
                return;
            }
        };
        let tmp = format!("{}.tmp", path);
        if let Err(e) = std::fs::write(&tmp, &json) {
            error!("Failed to write checkpoint tmp file: {}", e);
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            error!("Failed to rename checkpoint file: {}", e);
            // Try to clean up the tmp file
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        info!("Checkpoint saved: hash={}, seq={}", cp.hash, cp.sequence);
    }
}

pub struct IngestorMetrics {
    pub blocks_processed: AtomicU64,
    pub txs_processed: AtomicU64,
    pub virtual_daa_score: AtomicU64,
    pub connected: std::sync::atomic::AtomicBool,
}

impl IngestorMetrics {
    pub fn new() -> Self {
        Self {
            blocks_processed: AtomicU64::new(0),
            txs_processed: AtomicU64::new(0),
            virtual_daa_score: AtomicU64::new(0),
            connected: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

pub async fn run_ingest_loop(
    config: Config,
    event_tx: broadcast::Sender<proto::IngestorEvent>,
    metrics: Arc<IngestorMetrics>,
) {
    let poll_interval = Duration::from_millis(config.ingest.poll_interval_ms);

    // Load checkpoint if available
    let checkpoint_enabled = !config.ingest.checkpoint_path.is_empty();
    let loaded = if checkpoint_enabled {
        Checkpoint::load(&config.ingest.checkpoint_path)
    } else {
        None
    };
    let mut sequence: u64 = loaded.as_ref().map(|c| c.sequence).unwrap_or(0);
    let resume_hash: Option<String> = loaded.map(|c| c.hash);

    loop {
        info!("Connecting to node at {}", config.node.url);

        match connect_and_ingest(
            &config,
            &event_tx,
            &metrics,
            &mut sequence,
            poll_interval,
            resume_hash.as_deref(),
        )
        .await
        {
            Ok(()) => {
                info!("Ingest loop exited cleanly");
                break;
            }
            Err(e) => {
                error!("Ingest loop error: {}. Reconnecting in 5s...", e);
                metrics.connected.store(false, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn connect_and_ingest(
    config: &Config,
    event_tx: &broadcast::Sender<proto::IngestorEvent>,
    metrics: &Arc<IngestorMetrics>,
    sequence: &mut u64,
    poll_interval: Duration,
    resume_hash: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let network_id = NetworkId::from_str(&config.node.network)?;

    let client = KaspaRpcClient::new_with_args(
        WrpcEncoding::Borsh,
        Some(&config.node.url),
        None,
        Some(network_id),
        None,
    )?;

    client
        .connect(Some(ConnectOptions {
            block_async_connect: true,
            strategy: ConnectStrategy::Fallback,
            url: None,
            connect_timeout: Some(Duration::from_secs(10)),
            retry_interval: None,
        }))
        .await?;

    let server_info = client.get_server_info().await?;
    info!(
        "Connected to node v{}, network: {}, synced: {}",
        server_info.server_version, server_info.network_id, server_info.is_synced
    );
    metrics.connected.store(true, Ordering::Relaxed);

    // Detect VSPCv2 support by checking server version
    let version_parts: Vec<u32> = server_info
        .server_version
        .split('.')
        .filter_map(|s| s.parse().ok())
        .collect();
    let supports_v2 = version_parts.len() >= 2 && (version_parts[0] > 1 || (version_parts[0] == 1 && version_parts[1] >= 1));

    if supports_v2 {
        info!("Node supports VSPCv2 — using single-call mode");
        ingest_vspc_v2(&client, config, event_tx, metrics, sequence, poll_interval, resume_hash).await
    } else {
        info!(
            "Node v{} does not support VSPCv2 — using v1 fallback (two-call mode)",
            server_info.server_version
        );
        ingest_v1_fallback(&client, config, event_tx, metrics, sequence, poll_interval, resume_hash).await
    }
}

/// VSPCv2 mode: single get_virtual_chain_from_block_v2 call with Full verbosity
async fn ingest_vspc_v2(
    client: &KaspaRpcClient,
    config: &Config,
    event_tx: &broadcast::Sender<proto::IngestorEvent>,
    metrics: &Arc<IngestorMetrics>,
    sequence: &mut u64,
    poll_interval: Duration,
    resume_hash: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut start_hash: RpcHash = if let Some(hash_str) = resume_hash {
        RpcHash::from_str(hash_str)?
    } else {
        let sink = client.get_sink().await?;
        sink.sink
    };
    info!("VSPCv2 starting from {}: {}", if resume_hash.is_some() { "checkpoint" } else { "sink" }, start_hash);

    let checkpoint_enabled = !config.ingest.checkpoint_path.is_empty();
    let checkpoint_interval = Duration::from_secs(config.ingest.checkpoint_interval_secs);
    let mut last_checkpoint = Instant::now();

    loop {
        match client
            .get_virtual_chain_from_block_v2(
                start_hash,
                Some(kaspa_rpc_core::RpcDataVerbosityLevel::Full),
                Some(config.ingest.tip_distance),
            )
            .await
        {
            Ok(response) => {
                let added = response.added_chain_block_hashes.len();
                let removed = response.removed_chain_block_hashes.len();

                if added > 0 || removed > 0 {
                    if let Some(last) = response.added_chain_block_hashes.last() {
                        start_hash = *last;
                    }

                    let tx_count: usize = response
                        .chain_block_accepted_transactions
                        .iter()
                        .map(|cb| cb.accepted_transactions.len())
                        .sum();

                    let events = normalize::normalize_vspc_response(&response, sequence);
                    for event in &events {
                        let _ = event_tx.send(event.clone());
                    }

                    metrics.blocks_processed.fetch_add(added as u64, Ordering::Relaxed);
                    metrics.txs_processed.fetch_add(tx_count as u64, Ordering::Relaxed);

                    if removed > 0 {
                        warn!("Reorg: {} removed, {} added, {} txs", removed, added, tx_count);
                    } else {
                        info!("Processed {} blocks, {} txs (seq: {})", added, tx_count, sequence);
                    }

                    // Periodic checkpoint
                    if checkpoint_enabled && last_checkpoint.elapsed() >= checkpoint_interval {
                        Checkpoint::save(
                            &config.ingest.checkpoint_path,
                            &start_hash.to_string(),
                            *sequence,
                        );
                        last_checkpoint = Instant::now();
                    }
                }
            }
            Err(e) => {
                error!("VSPCv2 poll error: {}", e);
                return Err(e.into());
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// V1 fallback: get_virtual_chain_from_block (tx IDs only) + get_blocks (full data)
async fn ingest_v1_fallback(
    client: &KaspaRpcClient,
    config: &Config,
    event_tx: &broadcast::Sender<proto::IngestorEvent>,
    metrics: &Arc<IngestorMetrics>,
    sequence: &mut u64,
    poll_interval: Duration,
    resume_hash: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut start_hash: RpcHash = if let Some(hash_str) = resume_hash {
        RpcHash::from_str(hash_str)?
    } else {
        let sink = client.get_sink().await?;
        sink.sink
    };
    info!("V1 fallback starting from {}: {}", if resume_hash.is_some() { "checkpoint" } else { "sink" }, start_hash);

    let checkpoint_enabled = !config.ingest.checkpoint_path.is_empty();
    let checkpoint_interval = Duration::from_secs(config.ingest.checkpoint_interval_secs);
    let mut last_checkpoint = Instant::now();

    loop {
        // Step 1: Get virtual chain changes (accepted tx IDs only)
        match client
            .get_virtual_chain_from_block(start_hash, true, None)
            .await
        {
            Ok(vc_response) => {
                let added = vc_response.added_chain_block_hashes.len();
                let removed = vc_response.removed_chain_block_hashes.len();

                if added > 0 || removed > 0 {
                    if let Some(last) = vc_response.added_chain_block_hashes.last() {
                        start_hash = *last;
                    }

                    // Handle reorgs
                    if removed > 0 {
                        let reorg_event = normalize::make_reorg_event(
                            &vc_response.removed_chain_block_hashes,
                            sequence,
                        );
                        let _ = event_tx.send(reorg_event);
                        warn!("Reorg: {} blocks removed", removed);
                    }

                    // Step 2: Fetch full block data for each added chain block
                    let mut total_txs: usize = 0;
                    for accepted in &vc_response.accepted_transaction_ids {
                        let block_hash = accepted.accepting_block_hash;

                        // Get the full block with transactions
                        match client.get_block(block_hash, true).await {
                            Ok(block) => {
                                let tx_count = block.transactions.len();
                                total_txs += tx_count;

                                let event = normalize::normalize_block_v1(
                                    &block,
                                    &accepted.accepted_transaction_ids,
                                    sequence,
                                );
                                let _ = event_tx.send(event);
                            }
                            Err(e) => {
                                warn!("Failed to get block {}: {}", block_hash, e);
                            }
                        }
                    }

                    metrics.blocks_processed.fetch_add(added as u64, Ordering::Relaxed);
                    metrics.txs_processed.fetch_add(total_txs as u64, Ordering::Relaxed);

                    info!(
                        "Processed {} blocks, {} txs (seq: {})",
                        added, total_txs, sequence
                    );

                    // Periodic checkpoint
                    if checkpoint_enabled && last_checkpoint.elapsed() >= checkpoint_interval {
                        Checkpoint::save(
                            &config.ingest.checkpoint_path,
                            &start_hash.to_string(),
                            *sequence,
                        );
                        last_checkpoint = Instant::now();
                    }
                }
            }
            Err(e) => {
                error!("V1 virtual chain poll error: {}", e);
                return Err(e.into());
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}
