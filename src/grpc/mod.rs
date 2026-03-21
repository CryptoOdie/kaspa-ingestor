/// gRPC streaming server.
///
/// Clients connect and call Subscribe() to receive a stream of IngestorEvents.
/// Events are broadcast from the ingest loop to all connected clients.
/// Follows the Yellowstone (Solana Geyser) pattern: single gRPC output,
/// external consumers decide what to do with the data.
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use log::{info, warn};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::config::Config;
use crate::ingest::{IngestorMetrics, ReplayBuffer};
use crate::proto;
use crate::proto::kaspa_ingestor_server::{KaspaIngestor, KaspaIngestorServer};

pub struct IngestorGrpcService {
    event_tx: broadcast::Sender<proto::IngestorEvent>,
    metrics: Arc<IngestorMetrics>,
    replay_buffer: Arc<ReplayBuffer>,
    config: Config,
}

impl IngestorGrpcService {
    pub fn new(
        event_tx: broadcast::Sender<proto::IngestorEvent>,
        metrics: Arc<IngestorMetrics>,
        replay_buffer: Arc<ReplayBuffer>,
        config: Config,
    ) -> Self {
        Self {
            event_tx,
            metrics,
            replay_buffer,
            config,
        }
    }

    pub fn into_server(self) -> KaspaIngestorServer<Self> {
        KaspaIngestorServer::new(self)
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<proto::IngestorEvent, Status>> + Send>>;

#[tonic::async_trait]
impl KaspaIngestor for IngestorGrpcService {
    type SubscribeStream = EventStream;

    async fn subscribe(
        &self,
        request: Request<Streaming<proto::SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // Read the first message from the client stream to get resume_from_sequence
        let mut inbound = request.into_inner();
        let resume_from_sequence = match inbound.message().await {
            Ok(Some(req)) => req.resume_from_sequence,
            Ok(None) => 0,
            Err(_) => 0,
        };

        // Subscribe to the broadcast channel BEFORE replaying so we don't miss
        // events between replay and live.
        let rx = self.event_tx.subscribe();
        info!(
            "New gRPC subscriber (total: {}, resume_from: {})",
            self.event_tx.receiver_count(),
            resume_from_sequence
        );

        // Build replay prefix if requested
        let replay_events = if resume_from_sequence > 0 {
            match self.replay_buffer.events_since(resume_from_sequence) {
                Some(events) => {
                    info!("Replaying {} events from sequence {}", events.len(), resume_from_sequence);
                    events
                }
                None => {
                    return Err(Status::out_of_range(format!(
                        "Sequence {} is too old and no longer in the replay buffer. Resync from scratch.",
                        resume_from_sequence
                    )));
                }
            }
        } else {
            Vec::new()
        };

        // Determine the highest sequence we replayed so we can deduplicate
        let replay_max_seq = replay_events.last().map(|e| e.sequence).unwrap_or(0);

        // Create the replay stream prefix
        let replay_stream = tokio_stream::iter(
            replay_events.into_iter().map(Ok)
        );

        // Live broadcast stream with error-on-lag
        let live_stream = BroadcastStream::new(rx);
        let dedup_seq = replay_max_seq;
        let mapped_live = async_stream::stream! {
            use tokio_stream::StreamExt;
            let mut stream = live_stream;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(event) => {
                        // Skip events already sent during replay
                        if event.sequence <= dedup_seq {
                            continue;
                        }
                        yield Ok(event);
                    }
                    Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                        warn!("Client lagged by {} events, disconnecting", n);
                        yield Err(tonic::Status::data_loss(
                            format!("Lagged by {} events. Reconnect and resume from your last sequence.", n)
                        ));
                        return;
                    }
                }
            }
        };

        // Chain replay then live
        let combined = async_stream::stream! {
            use tokio_stream::StreamExt;

            // First: replay events
            let mut replay = std::pin::pin!(replay_stream);
            while let Some(item) = replay.next().await {
                yield item;
            }

            // Then: live events
            let mut live = std::pin::pin!(mapped_live);
            while let Some(item) = live.next().await {
                yield item;
            }
        };

        Ok(Response::new(Box::pin(combined)))
    }

    async fn ping(
        &self,
        _request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        Ok(Response::new(proto::PingResponse {}))
    }

    async fn get_status(
        &self,
        _request: Request<proto::StatusRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        Ok(Response::new(proto::StatusResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            node_url: self.config.node.url.clone(),
            connected: self.metrics.connected.load(Ordering::Relaxed),
            virtual_daa_score: self.metrics.virtual_daa_score.load(Ordering::Relaxed),
            blocks_processed: self.metrics.blocks_processed.load(Ordering::Relaxed),
            txs_processed: self.metrics.txs_processed.load(Ordering::Relaxed),
            connected_clients: self.event_tx.receiver_count() as u64,
        }))
    }
}

/// Start the gRPC server
pub async fn start_grpc_server(
    config: Config,
    event_tx: broadcast::Sender<proto::IngestorEvent>,
    metrics: Arc<IngestorMetrics>,
    replay_buffer: Arc<ReplayBuffer>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = config.grpc.bind_address.parse()?;

    let service = IngestorGrpcService::new(event_tx, metrics, replay_buffer, config);

    info!("gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
