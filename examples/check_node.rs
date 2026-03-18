use kaspa_wrpc_client::prelude::*;
use kaspa_wrpc_client::KaspaRpcClient;
use kaspa_wrpc_client::client::ConnectOptions;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args().nth(1).unwrap_or_else(|| "wss://wrpc.kasia.fyi".to_string());
    println!("Connecting to {}...", url);

    let client = KaspaRpcClient::new_with_args(
        WrpcEncoding::Borsh,
        Some(&url),
        None,
        Some(NetworkId::new(NetworkType::Mainnet)),
        None,
    )?;

    client.connect(Some(ConnectOptions {
        block_async_connect: true,
        strategy: ConnectStrategy::Fallback,
        url: None,
        connect_timeout: Some(Duration::from_secs(10)),
        retry_interval: None,
    })).await?;

    println!("Connected!");

    let info = client.get_server_info().await?;
    println!("Server version: {}", info.server_version);
    println!("Network: {}", info.network_id);
    println!("Synced: {}", info.is_synced);
    println!("Virtual DAA score: {}", info.virtual_daa_score);

    // Check if VSPCv2 is available by trying the call
    let sink = client.get_sink().await?;
    println!("Sink: {}", sink.sink);

    match client.get_virtual_chain_from_block_v2(
        sink.sink,
        Some(kaspa_rpc_core::RpcDataVerbosityLevel::Full),
        Some(5),
    ).await {
        Ok(resp) => {
            println!("VSPCv2 SUPPORTED!");
            println!("  Added blocks: {}", resp.added_chain_block_hashes.len());
            println!("  Removed blocks: {}", resp.removed_chain_block_hashes.len());
            println!("  Chain block txs: {}", resp.chain_block_accepted_transactions.len());
        }
        Err(e) => {
            println!("VSPCv2 NOT SUPPORTED: {}", e);
        }
    }

    client.disconnect().await?;
    Ok(())
}
