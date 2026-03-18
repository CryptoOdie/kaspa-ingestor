/// Converts rusty-kaspa RPC types into our canonical protobuf event types.
use kaspa_rpc_core::model::message::GetVirtualChainFromBlockV2Response;

use crate::proto;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Convert a VSPCv2 response into a sequence of IngestorEvents.
pub fn normalize_vspc_response(
    response: &GetVirtualChainFromBlockV2Response,
    sequence: &mut u64,
) -> Vec<proto::IngestorEvent> {
    let mut events = Vec::new();
    let now_ms = now_ms();

    // Emit reorg event if any blocks were removed
    if !response.removed_chain_block_hashes.is_empty() {
        let reorg = proto::ReorgEvent {
            removed_block_hashes: response
                .removed_chain_block_hashes
                .iter()
                .map(|h| h.as_bytes().to_vec())
                .collect(),
        };
        *sequence += 1;
        events.push(proto::IngestorEvent {
            sequence: *sequence,
            timestamp_ms: now_ms,
            event: Some(proto::ingestor_event::Event::Reorg(reorg)),
        });
    }

    // Emit a ChainBlockEvent for each accepted chain block
    for chain_block in response.chain_block_accepted_transactions.iter() {
        let header = convert_optional_header(&chain_block.chain_block_header);
        let accepted_txs: Vec<proto::AcceptedTransaction> = chain_block
            .accepted_transactions
            .iter()
            .map(convert_optional_transaction)
            .collect();

        let block_hash = chain_block
            .chain_block_header
            .hash
            .map(|h| h.as_bytes().to_vec())
            .unwrap_or_default();

        let event = proto::ChainBlockEvent {
            block_hash,
            header: Some(header),
            accepted_transactions: accepted_txs,
        };

        *sequence += 1;
        events.push(proto::IngestorEvent {
            sequence: *sequence,
            timestamp_ms: now_ms,
            event: Some(proto::ingestor_event::Event::ChainBlock(event)),
        });
    }

    events
}

fn convert_optional_header(h: &kaspa_rpc_core::RpcOptionalHeader) -> proto::BlockHeader {
    // CompressedParents uses .get(level_index) to access each level
    let parents = if let Some(ref cp) = h.parents_by_level {
        let mut levels = Vec::new();
        let mut i = 0;
        while let Some(hashes) = cp.get(i) {
            levels.push(proto::ParentLevel {
                parent_hashes: hashes.iter().map(|p| p.as_bytes().to_vec()).collect(),
            });
            i += 1;
        }
        levels
    } else {
        Vec::new()
    };

    proto::BlockHeader {
        hash: h.hash.map(|v| v.as_bytes().to_vec()).unwrap_or_default(),
        version: h.version.unwrap_or(0) as u32,
        parents_by_level: parents,
        hash_merkle_root: h.hash_merkle_root.map(|v| v.as_bytes().to_vec()).unwrap_or_default(),
        accepted_id_merkle_root: h
            .accepted_id_merkle_root
            .map(|v| v.as_bytes().to_vec())
            .unwrap_or_default(),
        utxo_commitment: h.utxo_commitment.map(|v| v.as_bytes().to_vec()).unwrap_or_default(),
        timestamp_ms: h.timestamp.unwrap_or(0),
        bits: h.bits.unwrap_or(0),
        nonce: h.nonce.unwrap_or(0),
        daa_score: h.daa_score.unwrap_or(0),
        blue_work: h
            .blue_work
            .map(|v| v.to_be_bytes().to_vec())
            .unwrap_or_default(),
        blue_score: h.blue_score.unwrap_or(0),
        pruning_point: h.pruning_point.map(|v| v.as_bytes().to_vec()).unwrap_or_default(),
    }
}

fn convert_optional_transaction(
    tx: &kaspa_rpc_core::RpcOptionalTransaction,
) -> proto::AcceptedTransaction {
    let verbose = tx.verbose_data.as_ref();

    proto::AcceptedTransaction {
        transaction_id: verbose
            .and_then(|v| v.transaction_id)
            .map(|id| id.as_bytes().to_vec())
            .unwrap_or_default(),
        hash: verbose
            .and_then(|v| v.hash)
            .map(|h| h.as_bytes().to_vec())
            .unwrap_or_default(),
        version: tx.version.unwrap_or(0) as u32,
        inputs: tx
            .inputs
            .iter()
            .enumerate()
            .map(|(i, input)| convert_optional_input(i, input))
            .collect(),
        outputs: tx
            .outputs
            .iter()
            .enumerate()
            .map(|(i, output)| convert_optional_output(i, output))
            .collect(),
        lock_time: tx.lock_time.unwrap_or(0),
        subnetwork_id: tx
            .subnetwork_id
            .as_ref()
            .map(|s| {
                let bytes: &[u8; 20] = s.as_ref();
                bytes.to_vec()
            })
            .unwrap_or_default(),
        gas: tx.gas.unwrap_or(0),
        payload: tx.payload.clone().unwrap_or_default(),
        mass: tx.mass.unwrap_or(0),
        block_time: verbose.and_then(|v| v.block_time).unwrap_or(0),
    }
}

fn convert_optional_input(
    index: usize,
    input: &kaspa_rpc_core::RpcOptionalTransactionInput,
) -> proto::TransactionInput {
    let outpoint = input.previous_outpoint.as_ref();
    let utxo = input
        .verbose_data
        .as_ref()
        .and_then(|v| v.utxo_entry.as_ref());

    proto::TransactionInput {
        index: index as u32,
        previous_outpoint_hash: outpoint
            .and_then(|o| o.transaction_id)
            .map(|id| id.as_bytes().to_vec())
            .unwrap_or_default(),
        previous_outpoint_index: outpoint.and_then(|o| o.index).unwrap_or(0),
        signature_script: input.signature_script.clone().unwrap_or_default(),
        sequence: input.sequence.unwrap_or(0),
        sig_op_count: input.sig_op_count.unwrap_or(0) as u32,
        utxo_entry: utxo.map(|u| proto::UtxoEntry {
            amount: u.amount.unwrap_or(0),
            script_public_key: u
                .script_public_key
                .as_ref()
                .map(|s| s.script().to_vec())
                .unwrap_or_default(),
            block_daa_score: u.block_daa_score.unwrap_or(0),
            is_coinbase: u.is_coinbase.unwrap_or(false),
            address: u
                .verbose_data
                .as_ref()
                .and_then(|v| v.script_public_key_address.as_ref())
                .map(|a| a.to_string())
                .unwrap_or_default(),
        }),
    }
}

fn convert_optional_output(
    index: usize,
    output: &kaspa_rpc_core::RpcOptionalTransactionOutput,
) -> proto::TransactionOutput {
    let verbose = output.verbose_data.as_ref();

    proto::TransactionOutput {
        index: index as u32,
        amount: output.value.unwrap_or(0),
        script_public_key: output
            .script_public_key
            .as_ref()
            .map(|s| s.script().to_vec())
            .unwrap_or_default(),
        script_public_key_version: output
            .script_public_key
            .as_ref()
            .map(|s| s.version() as u32)
            .unwrap_or(0),
        address: verbose
            .and_then(|v| v.script_public_key_address.as_ref())
            .map(|a| a.to_string())
            .unwrap_or_default(),
    }
}
