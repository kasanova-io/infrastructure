//! Private, transactional history import. Uses the live indexer's parser and
//! partition types; never emits push notifications or changes chain cursors.
use super::export::ExportApi;
use anyhow::{Context, Result, bail, ensure};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use fjall::{PartitionCreateOptions, TxKeyspace};
use indexer_db::{
    AddressPayload,
    messages::{
        contextual_message::*, group_control::*, group_message::*, handshake::*, payment::*,
        self_stash::*,
    },
    processing::tx_id_to_acceptance::*,
};
use kaspa_rpc_core::RpcAddress;
use protocol::operation::{SealedOperation, deserializer::parse_sealed_operation};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HistoryTx {
    pub tx_id: String,
    pub payload: String,
    pub sender: String,
    pub receiver: String,
    pub amount: u64,
    pub block_hash: String,
    pub block_time: u64,
    pub accepting_block: Option<String>,
    pub accepting_daa_score: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ImportResult {
    pub imported: usize,
    pub duplicates: usize,
}

fn hex(s: &str) -> Result<Vec<u8>> {
    ensure!(s.len() % 2 == 0, "odd hex length");
    let mut out = vec![0; s.len() / 2];
    faster_hex::hex_decode(s.as_bytes(), &mut out)?;
    Ok(out)
}
fn fixed<const N: usize>(s: &str) -> Result<[u8; N]> {
    hex(s)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid fixed hex length"))
}
fn address(s: &str) -> Result<AddressPayload> {
    ensure!(
        s.starts_with("kaspa:"),
        "history import requires a mainnet address"
    );
    AddressPayload::try_from(&RpcAddress::try_from(s.to_owned())?)
}

pub async fn import_history(
    State(state): State<ExportApi>,
    Json(txs): Json<Vec<HistoryTx>>,
) -> impl IntoResponse {
    if std::env::var("KASANOVA_HISTORY_IMPORT_ENABLED").as_deref() != Ok("true")
        || std::env::var("NETWORK_TYPE").as_deref() != Ok("mainnet")
    {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "history import is disabled"})),
        )
            .into_response();
    }
    let result = tokio::task::spawn_blocking(move || import_batch(&state.tx_keyspace, &txs)).await;
    match result {
        Ok(Ok(result)) => {
            (StatusCode::OK, Json(serde_json::to_value(result).unwrap())).into_response()
        }
        Ok(Err(error)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "history task failed"})),
        )
            .into_response(),
    }
}

pub fn import_batch(db: &TxKeyspace, txs: &[HistoryTx]) -> Result<ImportResult> {
    ensure!(txs.len() <= 200, "maximum batch size is 200");
    let ledger = db.open_partition("kasanova_history_import", PartitionCreateOptions::default())?;
    let mut wtx = db.write_tx()?;
    let mut result = ImportResult {
        imported: 0,
        duplicates: 0,
    };
    for t in txs {
        let tx_id = fixed::<32>(&t.tx_id).context("invalid transaction ID")?;
        let payload = hex(&t.payload).context("invalid transaction payload")?;
        let provenance = serde_json::to_vec(t)?;
        if let Some(previous) = wtx.get(&ledger, tx_id)? {
            ensure!(
                previous.as_ref() == provenance.as_slice(),
                "conflicting history record for {}",
                t.tx_id
            );
            result.duplicates += 1;
            continue;
        }
        let sender = address(&t.sender)?;
        let receiver = address(&t.receiver)?;
        let block_hash = fixed::<32>(&t.block_hash)?;
        ensure!(t.block_time > 0, "missing block time");
        let op = parse_sealed_operation(&payload).context("unsupported chat operation")?;
        // Compare any existing ciphertext before adding missing historical indexes.
        let (part_name, sealed) = match &op {
            SealedOperation::SealedMessageOrSealedHandshakeVNone(v) => {
                ("tx-id-to-handshake", v.sealed_hex)
            }
            SealedOperation::SealedHandshakeV2(v) => ("tx-id-to-handshake", v.sealed_hex),
            SealedOperation::ContextualMessageV1(v) => {
                ("tx-id-to-contextual-message", v.sealed_hex)
            }
            SealedOperation::PaymentV1(v) => ("tx_id_to_payment", v.sealed_hex),
            SealedOperation::SelfStashV1(v) => ("tx-id-to-self-stash", v.sealed_hex),
            SealedOperation::GroupMessageV1(v) => ("tx-id-to-group-message", v.sealed_hex),
            SealedOperation::GroupControlV1(v) => ("tx-id-to-group-control", v.encrypted_payload),
        };
        ensure!(!sealed.is_empty(), "empty encrypted payload");
        let part = db.open_partition(part_name, PartitionCreateOptions::default())?;
        if let Some(old) = wtx.get(&part, tx_id)? {
            let expected = if part_name == "tx_id_to_payment" {
                [t.amount.to_le_bytes().as_slice(), sealed].concat()
            } else {
                sealed.to_vec()
            };
            ensure!(
                old.as_ref() == expected.as_slice(),
                "existing ciphertext differs for {}",
                t.tx_id
            );
        }
        match op {
            SealedOperation::SealedMessageOrSealedHandshakeVNone(_)
            | SealedOperation::SealedHandshakeV2(_) => {
                let version = if matches!(op, SealedOperation::SealedHandshakeV2(_)) {
                    2
                } else {
                    0
                };
                TxIdToHandshakePartition::new(db)?.insert_wtx(&mut wtx, &tx_id, sealed);
                HandshakeByReceiverPartition::new(db)?.insert_wtx(
                    &mut wtx,
                    &HandshakeKeyByReceiver {
                        receiver,
                        block_time: t.block_time.into(),
                        block_hash,
                        version,
                        tx_id,
                    },
                    Some(sender),
                )?;
                HandshakeBySenderPartition::new(db)?.insert_wtx(
                    &mut wtx,
                    &HandshakeKeyBySender {
                        sender,
                        block_time: t.block_time.into(),
                        block_hash,
                        receiver,
                        version,
                        tx_id,
                    },
                );
            }
            SealedOperation::ContextualMessageV1(v) => {
                // Match the native live processor's 16-byte alias key. The full
                // original envelope remains in the transaction import ledger.
                let mut alias = [0; 16];
                let length = v.alias.len().min(16);
                alias[..length].copy_from_slice(&v.alias[..length]);
                TxIdToContextualMessagePartition::new(db)?.insert_wtx(&mut wtx, &tx_id, sealed);
                ContextualMessageBySenderPartition::new(db)?.insert(
                    &mut wtx,
                    &ContextualMessageBySenderKey {
                        sender,
                        alias,
                        block_time: t.block_time.into(),
                        block_hash,
                        receiver,
                        version: 1,
                        tx_id,
                    },
                );
            }
            SealedOperation::PaymentV1(_) => {
                TxIdToPaymentPartition::new(db)?.insert_wtx(&mut wtx, &tx_id, t.amount, sealed)?;
                PaymentByReceiverPartition::new(db)?.insert_wtx(
                    &mut wtx,
                    &PaymentKeyByReceiver {
                        receiver,
                        block_time: t.block_time.into(),
                        block_hash,
                        version: 0,
                        tx_id,
                    },
                    Some(sender),
                )?;
                PaymentBySenderPartition::new(db)?.insert_wtx(
                    &mut wtx,
                    &PaymentKeyBySender {
                        sender,
                        block_time: t.block_time.into(),
                        block_hash,
                        receiver,
                        version: 0,
                        tx_id,
                    },
                );
            }
            SealedOperation::SelfStashV1(v) => {
                ensure!(
                    v.key.unwrap_or_default().len() <= SCOPE_LEN,
                    "unsupported scope length"
                );
                TxIdToSelfStashPartition::new(db)?.insert_wtx(&mut wtx, &tx_id, sealed);
                SelfStashByOwnerPartition::new(db)?.insert_wtx(
                    &mut wtx,
                    &SelfStashKeyByOwner {
                        owner: sender,
                        scope: SelfStashScope::from(v.key.unwrap_or_default()),
                        block_time: t.block_time.into(),
                        block_hash,
                        version: 1,
                        tx_id,
                    },
                );
            }
            SealedOperation::GroupMessageV1(v) => {
                let blinded_group_id = fixed::<32>(std::str::from_utf8(v.blinded_group_id)?)?;
                let sender_pubkey = fixed::<32>(std::str::from_utf8(v.sender_pub)?)?;
                ensure!(
                    sender.matches_xonly_pubkey(&sender_pubkey),
                    "group sender mismatch"
                );
                ensure!(
                    GroupSenderBindingPartition::new(db)?.check_or_bind_wtx(
                        &mut wtx,
                        &blinded_group_id,
                        &sender_pubkey
                    )?,
                    "group binding conflict"
                );
                TxIdToGroupMessagePartition::new(db)?.insert_wtx(&mut wtx, &tx_id, sealed);
                GroupMessageByBlindedGroupIdPartition::new(db)?.insert_wtx(
                    &mut wtx,
                    &GroupMessageKeyByBlindedGroupId {
                        blinded_group_id,
                        block_time: t.block_time.into(),
                        block_hash,
                        version: 1,
                        tx_id,
                    },
                    Some(sender),
                )?;
            }
            SealedOperation::GroupControlV1(v) => {
                let recipient = v
                    .recipient_pubkey
                    .map(|s| fixed::<32>(std::str::from_utf8(s)?))
                    .transpose()?
                    .map(AddressPayload::from_xonly_pubkey);
                TxIdToGroupControlPartition::new(db)?.insert_wtx(&mut wtx, &tx_id, sealed);
                GroupControlBySenderPartition::new(db)?.insert_wtx(
                    &mut wtx,
                    &GroupControlKeyBySender {
                        sender,
                        block_time: t.block_time.into(),
                        block_hash,
                        version: 1,
                        tx_id,
                        recipient: recipient.unwrap_or_default(),
                    },
                );
                if let Some(recipient) = recipient {
                    GroupControlByRecipientPartition::new(db)?.insert_wtx(
                        &mut wtx,
                        &GroupControlKeyByRecipient {
                            recipient,
                            block_time: t.block_time.into(),
                            block_hash,
                            version: 1,
                            tx_id,
                        },
                        Some(sender),
                    )?;
                }
            }
        }
        let accept = TxIDToAcceptancePartition::new(db)?;
        if let Some(existing) = accept.key_by_tx_id(&tx_id)? {
            ensure!(
                existing.receiver == receiver,
                "existing acceptance receiver differs for {}",
                t.tx_id
            );
        }
        let key = AcceptanceKey { tx_id, receiver };
        accept.insert_wtx(&mut wtx, &key, &[])?;
        match (&t.accepting_block, t.accepting_daa_score) {
            (Some(hash), Some(daa)) => {
                accept.update_acceptance_wtx(&mut wtx, &key, fixed::<32>(hash)?, daa)?;
            }
            (None, None) => {}
            _ => bail!("incomplete acceptance metadata"),
        }
        wtx.insert(&ledger, tx_id, provenance);
        result.imported += 1;
    }
    ensure!(
        wtx.commit()?.is_ok(),
        "history transaction conflicted; retry the batch"
    );
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(n: u8, payload: &[u8]) -> HistoryTx {
        let addr = kaspa_addresses::Address::new(
            kaspa_addresses::Prefix::Mainnet,
            kaspa_addresses::Version::PubKey,
            &[7; 32],
        )
        .to_string();
        HistoryTx {
            tx_id: faster_hex::hex_string(&[n; 32]),
            payload: faster_hex::hex_string(payload),
            sender: addr.clone(),
            receiver: addr,
            amount: 123,
            block_hash: "ab".repeat(32),
            block_time: 123456,
            accepting_block: Some("cd".repeat(32)),
            accepting_daa_score: Some(55),
        }
    }
    #[test]
    fn both_prefixes_all_personal_types_and_restart() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let db = fjall::Config::new(temp.path()).open_transactional()?;
        let mut txs = vec![];
        for prefix in ["ciph_msg", "kchat"] {
            for body in [
                "1:handshake:deadbeef",
                "1:comm:001122:aGVsbG8=",
                "1:pay:00ff",
                "1:payment:00ee",
                "1:self_stash:saved_handshake:00aa",
                "1:gctl:00cc",
            ] {
                txs.push(fixture(
                    txs.len() as u8,
                    format!("{prefix}:{body}").as_bytes(),
                ));
            }
        }
        assert_eq!(import_batch(&db, &txs)?.imported, 12);
        assert_eq!(import_batch(&db, &txs)?.duplicates, 12);
        let rtx = db.read_tx();
        let accepted = TxIDToAcceptancePartition::new(&db)?
            .acceptance_by_tx_id_rtx(&rtx, &[0; 32])?
            .unwrap();
        assert_eq!(u64::from(accepted.header.accepting_daa), 55);
        assert_eq!(
            TxIdToHandshakePartition::new(&db)?
                .get_rtx(&rtx, &[0; 32])?
                .unwrap()
                .as_ref(),
            b"deadbeef"
        );
        drop(accepted);
        drop(rtx);
        db.persist(fjall::PersistMode::SyncAll)?;
        drop(db);
        let db = fjall::Config::new(temp.path()).open_transactional()?;
        assert_eq!(import_batch(&db, &txs)?.duplicates, 12);
        Ok(())
    }
    #[test]
    fn native_long_and_empty_alias_keys_remain_readable() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let db = fjall::Config::new(temp.path()).open_transactional()?;
        let long = fixture(
            40,
            b"kchat:1:comm:abcdefghijklmnopqrstuvwxyz0123456789_suffix:sealed",
        );
        let empty = fixture(41, b"ciph_msg:1:comm::sealed");
        let sender = address(&long.sender)?;
        assert_eq!(import_batch(&db, &[long, empty])?.imported, 2);
        let partition = ContextualMessageBySenderPartition::new(&db)?;
        let rtx = db.read_tx();
        let rows = partition
            .get_by_sender_alias_from_block_time(&rtx, &sender, b"abcdefghijklmnop", 0)
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_id, [40; 32]);
        let rows = partition
            .get_by_sender_alias_from_block_time(&rtx, &sender, &[0; 16], 0)
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_id, [41; 32]);
        Ok(())
    }

    #[test]
    fn malformed_batch_is_atomic_and_conflicts_are_rejected() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let db = fjall::Config::new(temp.path()).open_transactional()?;
        let good = fixture(1, b"kchat:1:handshake:\xff:\x00");
        let mut bad = fixture(2, b"kchat:1:comm:ab:00");
        bad.tx_id = "bad".into();
        assert!(import_batch(&db, &[good.clone(), bad]).is_err());
        assert!(
            TxIdToHandshakePartition::new(&db)?
                .get_rtx(&db.read_tx(), &[1; 32])?
                .is_none()
        );
        assert_eq!(import_batch(&db, &[good.clone()])?.imported, 1);
        let mut conflict = good;
        conflict.payload = faster_hex::hex_string(b"kchat:1:handshake:different");
        assert!(import_batch(&db, &[conflict]).is_err());
        Ok(())
    }
}
