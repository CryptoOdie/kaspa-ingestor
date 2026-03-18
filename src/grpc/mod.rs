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
use crate::ingest::IngestorMetrics;
use crate::proto;
use crate::proto::kaspa_ingestor_server::{KaspaIngestor, KaspaIngestorServer};

pub struct IngestorGrpcService {
    event_tx: broadcast::Sender<proto::IngestorEvent>,
    metrics: Arc<IngestorMetrics>,
    config: Config,
}

impl IngestorGrpcService {
    pub fn new(
        event_tx: broadcast::Sender<proto::IngestorEvent>,
        metrics: Arc<IngestorMetrics>,
        config: Config,
    ) -> Self {
        Self {
            event_tx,
            metrics,
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
        _request: Request<Streaming<proto::SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let rx = self.event_tx.subscribe();
        info!(
            "New gRPC subscriber (total: {})",
            self.event_tx.receiver_count()
        );

        let stream = BroadcastStream::new(rx);
        let mapped = tokio_stream::StreamExt::filter_map(stream, |result| match result {
            Ok(event) => Some(Ok(event)),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                warn!("Client lagged, skipped {} events", n);
                None
            }
        });

        Ok(Response::new(Box::pin(mapped)))
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = config.grpc.bind_address.parse()?;

    let service = IngestorGrpcService::new(event_tx, metrics, config);

    info!("gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
