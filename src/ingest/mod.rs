/// Ingestion loop: connects to a rusty-kaspa node via wRPC and polls
/// the virtual chain for accepted blocks and transactions using VSPCv2.
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::RpcHash;
use kaspa_wrpc_client::client::ConnectOptions;
use kaspa_wrpc_client::prelude::*;
use kaspa_wrpc_client::KaspaRpcClient;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use prost::Message;

use crate::config::Config;
use crate::normalize;
use crate::proto;

#[derive(Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub hash: String,
    pub sequence: u64,
    pub timestamp: u64,
    /// Hex-encoded SHA-256 of the last event (for hash chain continuity across restarts)
    #[serde(default)]
    pub prev_event_hash: String,
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

    pub fn save(path: &str, hash: &str, sequence: u64, prev_event_hash: &[u8; 32]) {
        let cp = Checkpoint {
            hash: hash.to_string(),
            sequence,
            timestamp: Self::now_epoch(),
            prev_event_hash: hex::encode(prev_event_hash),
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

/// Bounded ring buffer of recent events for consumer replay/resume.
pub struct ReplayBuffer {
    buffer: Mutex<VecDeque<proto::IngestorEvent>>,
    capacity: usize,
}

impl ReplayBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Append an event to the ring buffer, evicting the oldest if at capacity.
    pub fn push(&self, event: proto::IngestorEvent) {
        let mut buf = self.buffer.lock().unwrap();
        if buf.len() >= self.capacity {
            buf.pop_front();
        }
        buf.push_back(event);
    }

    /// Serialize the entire buffer to disk using protobuf length-delimited encoding.
    pub fn save_snapshot(&self, path: &str) {
        let buf = self.buffer.lock().unwrap();
        let mut out = Vec::new();
        for event in buf.iter() {
            event.encode_length_delimited(&mut out).ok();
        }
        let tmp = format!("{}.tmp", path);
        if let Err(e) = std::fs::write(&tmp, &out) {
            error!("Failed to write replay buffer snapshot: {}", e);
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            error!("Failed to rename replay buffer snapshot: {}", e);
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        info!("Replay buffer snapshot saved: {} events, {} bytes", buf.len(), out.len());
    }

    /// Restore the buffer from a protobuf length-delimited snapshot file.
    pub fn load_snapshot(path: &str, capacity: usize) -> Option<Self> {
        let data = std::fs::read(path).ok()?;
        let mut cursor = &data[..];
        let mut buffer = VecDeque::with_capacity(capacity);
        while !cursor.is_empty() {
            match proto::IngestorEvent::decode_length_delimited(&mut cursor) {
                Ok(event) => buffer.push_back(event),
                Err(e) => {
                    warn!("Replay buffer snapshot decode error at event {}: {}", buffer.len(), e);
                    break;
                }
            }
        }
        // Trim to capacity (keep newest)
        while buffer.len() > capacity {
            buffer.pop_front();
        }
        info!("Replay buffer snapshot loaded: {} events from {}", buffer.len(), path);
        Some(Self {
            buffer: Mutex::new(buffer),
            capacity,
        })
    }

    /// Get all events with sequence > `from_sequence`.
    /// Returns `None` if the requested sequence is too old (not in buffer).
    /// Returns `Some(vec![])` if `from_sequence` is current (no events to replay).
    pub fn events_since(&self, from_sequence: u64) -> Option<Vec<proto::IngestorEvent>> {
        let buf = self.buffer.lock().unwrap();

        if buf.is_empty() {
            // No events in buffer — if they're asking for seq 0 that's fine (no replay),
            // otherwise we can't satisfy the request.
            return if from_sequence == 0 { Some(Vec::new()) } else { None };
        }

        let oldest_seq = buf.front().unwrap().sequence;
        let newest_seq = buf.back().unwrap().sequence;

        if from_sequence >= newest_seq {
            // Client is already caught up
            return Some(Vec::new());
        }

        if from_sequence < oldest_seq.saturating_sub(1) {
            // Requested sequence is older than our buffer — can't replay
            return None;
        }

        // Find the start position: first event with sequence > from_sequence
        let events: Vec<proto::IngestorEvent> = buf
            .iter()
            .filter(|e| e.sequence > from_sequence)
            .cloned()
            .collect();

        Some(events)
    }
}

pub async fn run_ingest_loop(
    config: Config,
    event_tx: broadcast::Sender<proto::IngestorEvent>,
    metrics: Arc<IngestorMetrics>,
    replay_buffer: Arc<ReplayBuffer>,
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

    // Restore hash chain state from checkpoint (or start as zeroes)
    let mut prev_hash = [0u8; 32];
    if let Some(ref cp) = loaded {
        if !cp.prev_event_hash.is_empty() {
            if let Ok(bytes) = hex::decode(&cp.prev_event_hash) {
                if bytes.len() == 32 {
                    prev_hash.copy_from_slice(&bytes);
                    info!("Restored prev_event_hash from checkpoint");
                }
            }
        }
    }

    let resume_hash: Option<String> = loaded.map(|c| c.hash);

    loop {
        info!("Connecting to node at {}", config.node.url);

        match connect_and_ingest(
            &config,
            &event_tx,
            &metrics,
            &mut sequence,
            &mut prev_hash,
            poll_interval,
            resume_hash.as_deref(),
            &replay_buffer,
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
    prev_hash: &mut [u8; 32],
    poll_interval: Duration,
    resume_hash: Option<&str>,
    replay_buffer: &Arc<ReplayBuffer>,
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

    ingest_vspc_v2(&client, config, event_tx, metrics, sequence, prev_hash, poll_interval, resume_hash, replay_buffer).await
}

/// VSPCv2 mode: single get_virtual_chain_from_block_v2 call with Full verbosity
async fn ingest_vspc_v2(
    client: &KaspaRpcClient,
    config: &Config,
    event_tx: &broadcast::Sender<proto::IngestorEvent>,
    metrics: &Arc<IngestorMetrics>,
    sequence: &mut u64,
    prev_hash: &mut [u8; 32],
    poll_interval: Duration,
    resume_hash: Option<&str>,
    replay_buffer: &Arc<ReplayBuffer>,
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

                    let events = normalize::normalize_vspc_response(&response, sequence, prev_hash);
                    for event in &events {
                        let _ = event_tx.send(event.clone());
                        replay_buffer.push(event.clone());
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
                            prev_hash,
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
