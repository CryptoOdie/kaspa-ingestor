# kaspa-ingestor

Un-opinionated Kaspa blockchain data ingestor. Connects to a rusty-kaspa node via wRPC, follows the virtual selected parent chain (VSPC), and streams accepted blocks + transactions over gRPC. No database, no indexing opinions — just a clean data firehose for downstream consumers.

## Architecture

```
┌──────────────┐  wRPC (Borsh)   ┌──────────────────┐  gRPC stream   ┌──────────────┐
│  rusty-kaspa │ ──────────────► │  kaspa-ingestor   │ ─────────────► │  consumer(s) │
│    node      │                 │                   │                │  indexer,    │
└──────────────┘                 │  • VSPCv2 poller  │                │  analytics,  │
                                 │  • checkpoint     │                │  wallet...   │
                                 │  • gRPC server    │                └──────────────┘
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
url = "wss://wrpc.kasia.fyi"
# Network: "mainnet", "testnet-10", "testnet-11"
network = "mainnet"

[grpc]
# Address for the gRPC streaming server
bind_address = "0.0.0.0:50051"

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

## gRPC API

The protobuf schema is in `proto/kaspa_ingestor.proto`. Three RPCs are available:

### `Subscribe` (bidirectional stream)

Streams `IngestorEvent` messages to the client. Each event contains a monotonic sequence number for gap detection and one of:

- **`ChainBlockEvent`** — A newly accepted chain block with its header and full transaction data (inputs, outputs, UTXO entries when available).
- **`ReorgEvent`** — Block hashes removed from the virtual selected parent chain during a reorg.
- **`StatusEvent`** — Periodic heartbeat with DAA score, block/tx counts.

### `Ping` (unary)

Health check. Returns an empty response.

### `GetStatus` (unary)

Returns current ingestor state: version, node URL, connection status, DAA score, block/tx counts, and number of connected gRPC clients.

## VSPCv2 vs V1 Fallback

The ingestor auto-detects the node version on connect:

- **VSPCv2** (rusty-kaspa v1.1.0+): Uses `get_virtual_chain_from_block_v2` which returns accepted chain blocks with full transaction data and resolved UTXO entries in a single call. This is the preferred mode.
- **V1 fallback** (rusty-kaspa v1.0.x): Uses `get_virtual_chain_from_block` (returns only transaction IDs) followed by `get_block` for each accepted chain block. No UTXO entry data is available in this mode.

The consumer-facing gRPC schema is identical in both modes. The only difference is that `utxo_entry` fields on transaction inputs will be empty in V1 mode.

## Connect as a Consumer

Using `grpcurl`:

```bash
# Check status
grpcurl -plaintext localhost:50051 kaspa.ingestor.KaspaIngestor/GetStatus

# Ping
grpcurl -plaintext localhost:50051 kaspa.ingestor.KaspaIngestor/Ping

# Subscribe to events (streams indefinitely)
grpcurl -plaintext -d '{}' localhost:50051 kaspa.ingestor.KaspaIngestor/Subscribe
```

Or use any gRPC client with the proto file at `proto/kaspa_ingestor.proto`.

## License

MIT
