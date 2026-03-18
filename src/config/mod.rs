use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub node: NodeConfig,
    pub grpc: GrpcConfig,
    pub ingest: IngestConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    /// wRPC URL of the rusty-kaspa node (e.g., "ws://127.0.0.1:17110")
    pub url: String,
    /// Network ID (e.g., "mainnet", "testnet-10", "testnet-11")
    pub network: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GrpcConfig {
    /// Address to bind the gRPC server (e.g., "0.0.0.0:50051")
    pub bind_address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    /// Polling interval in milliseconds for VSPCv2
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,

    /// Tip distance for VSPCv2 (confirmation safety margin)
    #[serde(default = "default_tip_distance")]
    pub tip_distance: u64,

    /// Path to the checkpoint file (set to empty string to disable)
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,

    /// How often to write the checkpoint file, in seconds
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_secs: u64,
}

fn default_poll_interval() -> u64 {
    100
}

fn default_tip_distance() -> u64 {
    5
}

fn default_checkpoint_path() -> String {
    "checkpoint.json".to_string()
}

fn default_checkpoint_interval() -> u64 {
    60
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
