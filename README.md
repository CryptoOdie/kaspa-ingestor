# kaspa-ingestor

Un-opinionated Kaspa blockchain data ingestor. Connects to a rusty-kaspa node via wRPC, follows the virtual selected parent chain (VSPC), and streams accepted blocks + transactions over gRPC. No database, no indexing opinions — just a clean data firehose for downstream consumers.

## Architecture

```
┌──────────────┐  wRPC (Borsh)   ┌──────────────────┐  gRPC stream   ┌──────────────┐
│  rusty-kaspa │ ──────────────► │  kaspa-ingestor   │ ─────────────► │  consumer(s) │
│    node      │                 │                   │                │  indexer,    │
└──────────────┘                 │  • VSPCv2 poller  │                │  analytics,  │
                                 │  • checkpoint     │                │  wallet...   │
                                 │  • replay buffer  │                └──────────────┘
                                 │  • hash chain     │
                                 │  • gRPC server    │
                                 └──────────────────┘
```

The ingestor polls the node's virtual chain at a configurable interval. Each poll returns newly accepted chain blocks with full transaction data. These are normalized into protobuf events and broadcast to all connected gRPC clients.

## Build

```bash
cargo build --release
```

The binary will be at `target/release/kaspa-ingestor`.

## Configure

All configuration lives in `config.toml`:

```toml
[node]
# wRPC URL of a rusty-kaspa node
url = "ws://127.0.0.1:17110"
# Network: "mainnet", "testnet-10", "testnet-11"
network = "mainnet"

[grpc]
# Address for the gRPC streaming server
bind_address = "0.0.0.0:50051"
# Number of recent events kept in memory for consumer resume (~80 min at 10 BPS)
replay_buffer_size = 50000
# Path to persist replay buffer on shutdown (for resume across restarts)
replay_buffer_path = "replay_buffer.bin"

[ingest]
# VSPCv2 polling interval in milliseconds
poll_interval_ms = 100
# Tip distance (confirmation safety margin)
tip_distance = 5
# Checkpoint file path (set to "" to disable)
checkpoint_path = "checkpoint.json"
# How often to save checkpoint (seconds)
checkpoint_interval_secs = 60
```

## Run

```bash
# Default config path: config.toml
./target/release/kaspa-ingestor

# Custom config
./target/release/kaspa-ingestor --config /path/to/config.toml

# Adjust log level
RUST_LOG=debug ./target/release/kaspa-ingestor
```

## Checkpointing

The ingestor writes a `checkpoint.json` file every 60 seconds (configurable) with the last processed block hash and sequence number. On restart, it resumes from the checkpoint instead of the current sink, so blocks produced during downtime are not missed.

The checkpoint write is atomic (write to `.tmp`, then rename) to prevent corruption from mid-write crashes.

To disable checkpointing, set `checkpoint_path = ""` in your config.

## Replay Buffer

The ingestor maintains a bounded ring buffer of recent events in memory (default 50,000 events, roughly 80 minutes at 10 BPS). This allows gRPC consumers to resume from their last seen sequence number without a full resync.

- On shutdown (Ctrl+C), the buffer is serialized to disk as `replay_buffer.bin` using protobuf length-delimited encoding.
- On startup, the snapshot is restored so consumers can resume even across ingestor restarts.
- If a consumer requests a sequence older than the buffer, the server returns an `OUT_OF_RANGE` error.

To disable replay buffer persistence, set `replay_buffer_path = ""` in your config.

## Hash Chain

Every `IngestorEvent` includes a `prev_event_hash` field — the SHA-256 hash of the previous event's serialized protobuf bytes. This forms a tamper-evident hash chain that consumers can verify to ensure no events were dropped or modified. The first event uses a zero hash. The hash chain state is persisted in the checkpoint file for continuity across restarts.

## gRPC API

The protobuf schema is in `proto/kaspa_ingestor.proto`. Three RPCs are available:

### `Subscribe` (bidirectional stream)

Streams `IngestorEvent` messages to the client. Each event contains a monotonic sequence number, a `prev_event_hash` for hash chain verification, and one of:

- **`ChainBlockEvent`** — A newly accepted chain block with its header and full transaction data (inputs, outputs, UTXO entries when available).
- **`ReorgEvent`** — Block hashes removed from the virtual selected parent chain during a reorg.
- **`StatusEvent`** — Periodic heartbeat with DAA score, block/tx counts.

Consumers can send a `SubscribeRequest` with `resume_from_sequence` to replay buffered events before switching to the live stream. The handoff is seamless with automatic deduplication. If a consumer falls behind the broadcast buffer, they receive a `DATA_LOSS` error and should reconnect with their last sequence.

### `Ping` (unary)

Health check. Returns an empty response.

### `GetStatus` (unary)

Returns current ingestor state: version, node URL, connection status, DAA score, block/tx counts, and number of connected gRPC clients.

## Connect as a Consumer

Using `grpcurl`:

```bash
# Check status
grpcurl -plaintext localhost:50051 kaspa.ingestor.KaspaIngestor/GetStatus

# Ping
grpcurl -plaintext localhost:50051 kaspa.ingestor.KaspaIngestor/Ping

# Subscribe to events (streams indefinitely)
grpcurl -plaintext -d '{}' localhost:50051 kaspa.ingestor.KaspaIngestor/Subscribe

# Resume from a specific sequence number
grpcurl -plaintext -d '{"resume_from_sequence": 12345}' localhost:50051 kaspa.ingestor.KaspaIngestor/Subscribe
```

Or use any gRPC client with the proto file at `proto/kaspa_ingestor.proto`.

## License

MIT
