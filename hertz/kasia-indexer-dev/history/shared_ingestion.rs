//! Shared-journal transport through the existing private history-import route.
//! Uses native message partitions/parser, monotonic revisions and durable acks.
//! No pushes are emitted here; the existing notification path is integrated separately.
use super::export::ExportApi;
use super::history::{HistoryTx, address_on_network, configured_prefix, import_batch_wtx};
use anyhow::{Result, ensure};
use axum::{
    Json,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use fjall::{PartitionCreateOptions, TxKeyspace};
use indexer_db::processing::tx_id_to_acceptance::{
    AcceptanceKey, LookupOutput, TxIDToAcceptancePartition,
};
use kaspa_addresses::Prefix;
use kaspa_wrpc_client::prelude::{NetworkId, NetworkType};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JournalTx {
    pub revision: u64,
    pub transaction: HistoryTx,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalBatch {
    pub schema_version: u16,
    pub source_id: String,
    pub network: String,
    pub transactions: Vec<JournalTx>,
}

#[derive(Debug, Default, Serialize)]
pub struct JournalResult {
    pub applied: usize,
    pub duplicates: usize,
    pub stale: usize,
}

fn immutable(mut transaction: HistoryTx) -> HistoryTx {
    transaction.accepting_block = None;
    transaction.accepting_daa_score = None;
    transaction
}

fn same_body(left: &HistoryTx, right: &HistoryTx) -> bool {
    let mut left = immutable(left.clone());
    let mut right = immutable(right.clone());
    left.block_hash.clear();
    right.block_hash.clear();
    left.block_time = 0;
    right.block_time = 0;
    left == right
}

fn exact_network(network: &str) -> bool {
    NetworkId::from_str(network).is_ok_and(|id| {
        matches!(
            id.network_type(),
            NetworkType::Mainnet | NetworkType::Testnet
        ) && id.to_string() == network
    })
}

fn network_matches(configured_type: &str, full_network: &str) -> bool {
    exact_network(full_network)
        && (configured_type == full_network
            || (configured_type == "testnet" && full_network.starts_with("testnet-")))
}

fn journal_id(value: &str) -> bool {
    value.len() == 36
        && value != "00000000-0000-0000-0000-000000000000"
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}

fn authorized(headers: &HeaderMap, secret: &str) -> bool {
    if secret.len() < 32 {
        return false;
    }
    let Some(supplied) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let expected = format!("Bearer {secret}");
    let expected = ring::digest::digest(&ring::digest::SHA256, expected.as_bytes());
    let supplied = ring::digest::digest(&ring::digest::SHA256, supplied.as_bytes());
    #[allow(deprecated)]
    ring::constant_time::verify_slices_are_equal(expected.as_ref(), supplied.as_ref()).is_ok()
}

pub async fn import_journal(state: ExportApi, headers: HeaderMap, batch: JournalBatch) -> Response {
    let configured = std::env::var("KASANOVA_SHARED_NETWORK").unwrap_or_default();
    let native_network = std::env::var("NETWORK_TYPE").unwrap_or_default();
    let secret = std::env::var("KASANOVA_SHARED_INGEST_TOKEN").unwrap_or_default();
    if std::env::var("KASANOVA_SHARED_INGESTION_ENABLED").as_deref() != Ok("true")
        || !exact_network(&configured)
        || !network_matches(&native_network, &configured)
        || !authorized(&headers, &secret)
    {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error":"shared ingestion is disabled or unauthorized"})),
        )
            .into_response();
    }
    let result =
        tokio::task::spawn_blocking(move || apply_batch(&state.tx_keyspace, &configured, &batch))
            .await;
    match result {
        Ok(Ok(result)) => {
            (StatusCode::OK, Json(serde_json::to_value(result).unwrap())).into_response()
        }
        Ok(Err(error)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":"shared ingestion task failed"})),
        )
            .into_response(),
    }
}

/// The canonical journal owns message writes in shared mode. Middleware runs
/// before body extraction, so legacy bulk import/purge cannot bypass its ledger
/// or buffer an unneeded large body. Push configuration remains independent.
pub(super) async fn reject_alternate_writer(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    reject_alternate_writer_for_mode(
        std::env::var("KASANOVA_SHARED_INGESTION_ENABLED").as_deref() == Ok("true"),
        request,
        next,
    )
    .await
}

async fn reject_alternate_writer_for_mode(
    shared: bool,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if shared {
        return (
            StatusCode::FORBIDDEN,
            "message storage is owned by the canonical journal",
        )
            .into_response();
    }
    next.run(request).await
}

/// A successful inbox probe advertises DM ingestion support to KaChat clients.
/// Standalone scanners retain their old grammar, so they must not advertise it.
pub(super) async fn require_shared_inbox(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if std::env::var("KASANOVA_SHARED_INGESTION_ENABLED").as_deref() != Ok("true") {
        return StatusCode::NOT_FOUND.into_response();
    }
    next.run(request).await
}

#[cfg(test)]
#[path = "shared_mutator_tests.rs"]
mod shared_mutator_tests;

pub fn apply_batch(
    db: &TxKeyspace,
    configured: &str,
    batch: &JournalBatch,
) -> Result<JournalResult> {
    ensure!(
        batch.schema_version == 1,
        "unsupported shared ingestion schema"
    );
    ensure!(
        journal_id(&batch.source_id),
        "explicit journal source identity required"
    );
    ensure!(
        exact_network(configured) && batch.network == configured,
        "shared ingestion network mismatch"
    );
    ensure!(batch.transactions.len() <= 200, "maximum batch size is 200");
    let prefix = configured_prefix(configured)?;
    let ledger = db.open_partition(
        "kasanova_shared_ingestion",
        PartitionCreateOptions::default(),
    )?;
    let config = db.open_partition(
        "kasanova_shared_ingestion_config",
        PartitionCreateOptions::default(),
    )?;
    let mut wtx = db.write_tx()?;
    if let Some(old) = wtx.get(&config, b"network")? {
        ensure!(
            old.as_ref() == configured.as_bytes(),
            "shared ingestion database network binding mismatch"
        );
    } else {
        wtx.insert(&config, b"network", configured.as_bytes());
    }
    if let Some(old) = wtx.get(&config, b"source_id")? {
        ensure!(
            old.as_ref() == batch.source_id.as_bytes(),
            "shared ingestion journal identity changed; reconcile before migration"
        );
    } else {
        wtx.insert(&config, b"source_id", batch.source_id.as_bytes());
    }
    let mut result = JournalResult::default();
    for incoming in &batch.transactions {
        ensure!(incoming.revision > 0, "positive journal revision required");
        let t = &incoming.transaction;
        ensure!(
            native_chat_payload(&t.payload),
            "journal payload is not a supported private Chat operation"
        );
        let tx_id: [u8; 32] = decode_fixed(&t.tx_id)?;
        let receiver = address_on_network(&t.receiver, prefix)?;
        address_on_network(&t.sender, prefix)?;
        let accepted = match (&t.accepting_block, t.accepting_daa_score) {
            (Some(hash), Some(daa)) => {
                let block = decode_fixed::<32>(hash)?;
                ensure!(block != [0; 32], "accepted block cannot be zero");
                Some((block, daa))
            }
            (None, None) => None,
            _ => anyhow::bail!("incomplete journal acceptance metadata"),
        };
        if let Some(previous) = wtx.get(&ledger, tx_id)? {
            let old: JournalTx = serde_json::from_slice(previous.as_ref())?;
            ensure!(
                same_body(&old.transaction, t),
                "journal transaction body conflict"
            );
            if old.revision == incoming.revision {
                ensure!(old == *incoming, "conflicting duplicate journal revision");
                result.duplicates += 1;
                continue;
            }
            if old.revision > incoming.revision {
                result.stale += 1;
                continue;
            }
        }
        // Exactly the same native parser and insertion functions as legacy
        // backfill. Dynamic acceptance is applied separately in this transaction.
        import_batch_wtx(db, &[immutable(t.clone())], prefix, true, &mut wtx)?;
        let key = AcceptanceKey { tx_id, receiver };
        let (block, daa) = accepted.unwrap_or(([0; 32], 0));
        let updated = TxIDToAcceptancePartition::new(db)?
            .update_acceptance_wtx(&mut wtx, &key, block, daa)?;
        ensure!(
            !matches!(updated, LookupOutput::KeyDoesNotExist),
            "missing native acceptance row"
        );
        wtx.insert(&ledger, tx_id, serde_json::to_vec(incoming)?);
        result.applied += 1;
    }
    ensure!(
        wtx.commit()?.is_ok(),
        "shared ingestion transaction conflicted; retry the batch"
    );
    // Persist before acknowledging, including a retry of an already committed
    // batch whose previous fsync/HTTP response failed.
    db.persist(fjall::PersistMode::SyncAll)?;
    Ok(result)
}

fn decode_fixed<const N: usize>(value: &str) -> Result<[u8; N]> {
    ensure!(value.len() == N * 2, "invalid fixed hex length");
    let mut result = [0; N];
    faster_hex::hex_decode(value.as_bytes(), &mut result)?;
    Ok(result)
}

fn native_chat_payload(value: &str) -> bool {
    if value.len() % 2 != 0 {
        return false;
    }
    let mut payload = vec![0; value.len() / 2];
    if faster_hex::hex_decode(value.as_bytes(), &mut payload).is_err() {
        return false;
    }
    let Some(rest) = payload
        .strip_prefix(b"kchat:")
        .or_else(|| payload.strip_prefix(b"ciph_msg:"))
    else {
        return false;
    };
    if rest.starts_with(b"1:") {
        protocol::operation::deserializer::parse_shared_operation(&payload).is_some()
    } else {
        // The old decoder accepts arbitrary unknown versions as a legacy
        // handshake. Shared dispatch permits only the actual hex legacy envelope.
        !rest.is_empty() && rest.len() % 2 == 0 && rest.iter().all(u8::is_ascii_hexdigit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_db::messages::{
        handshake::TxIdToHandshakePartition, payment::TxIdToPaymentPartition,
    };

    pub(super) fn fixture(n: u8, prefix: Prefix, payload: &[u8]) -> JournalTx {
        let address =
            kaspa_addresses::Address::new(prefix, kaspa_addresses::Version::PubKey, &[7; 32])
                .to_string();
        JournalTx {
            revision: 1,
            transaction: HistoryTx {
                tx_id: faster_hex::hex_string(&[n; 32]),
                payload: faster_hex::hex_string(payload),
                sender: address.clone(),
                receiver: address,
                amount: 123,
                block_hash: "ab".repeat(32),
                block_time: 123456,
                accepting_block: Some("cd".repeat(32)),
                accepting_daa_score: Some(55),
            },
        }
    }
    pub(super) fn batch(network: &str, transactions: Vec<JournalTx>) -> JournalBatch {
        JournalBatch {
            schema_version: 1,
            source_id: "a0a0a0a0-1111-4444-8888-012345678901".into(),
            network: network.into(),
            transactions,
        }
    }

    #[test]
    fn shared_dm_native_storage_is_atomic_and_revision_safe_on_both_networks() -> Result<()> {
        use indexer_db::messages::contextual_message::*;
        for (network, prefix) in [
            ("mainnet", Prefix::Mainnet),
            ("testnet-10", Prefix::Testnet),
        ] {
            let temp = tempfile::tempdir()?;
            let db = fjall::Config::new(temp.path()).open_transactional()?;
            let mut wire = format!("kchat:1:dm:{}:alias:", "ab".repeat(16)).into_bytes();
            let binary = (0..=255u8).collect::<Vec<_>>();
            wire.extend_from_slice(&binary);
            let dm = fixture(61, prefix, &wire);
            assert!(native_chat_payload(&dm.transaction.payload));
            assert!(
                protocol::operation::deserializer::parse_sealed_operation(&wire).is_none(),
                "standalone grammar unchanged"
            );
            assert!(
                super::super::history::import_batch_for_network(
                    &db,
                    &[dm.transaction.clone()],
                    prefix
                )
                .is_err(),
                "legacy writer never partially files DM"
            );
            let comm = fixture(62, prefix, b"ciph_msg:1:comm:alias:\xff\x00:binary");
            assert_eq!(
                apply_batch(&db, network, &batch(network, vec![dm.clone(), comm]))?.applied,
                2
            );
            let inbox = ContextualMessageByInboxPartition::new(&db)?;
            let payload = TxIdToContextualMessagePartition::new(&db)?;
            assert_eq!(
                payload.get_rtx(&db.read_tx(), &[61; 32])?.unwrap().as_ref(),
                binary.as_slice()
            );
            let rows = inbox
                .iter_by_tag_from_block_time(&db.read_tx(), &[0xab; 16], 0)
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(rows.len(), 1, "classic comm creates no inbox row");
            assert_eq!(rows[0].0.tx_id, [61; 32]);
            assert_eq!(rows[0].0.block_time.get(), 123456);
            assert_eq!(
                rows[0].1.as_ref(),
                &address_on_network(&dm.transaction.sender, prefix)?
            );
            let by_sender = ContextualMessageBySenderPartition::new(&db)?;
            assert_eq!(by_sender.len()?, 2);
            let mut revoked = dm.clone();
            revoked.revision = 2;
            revoked.transaction.accepting_block = None;
            revoked.transaction.accepting_daa_score = None;
            apply_batch(&db, network, &batch(network, vec![revoked]))?;
            assert!(
                TxIDToAcceptancePartition::new(&db)?
                    .acceptance_by_tx_id_rtx(&db.read_tx(), &[61; 32])?
                    .is_none()
            );
            assert_eq!(
                apply_batch(&db, network, &batch(network, vec![dm.clone()]))?.stale,
                1
            );
            let mut accepted = dm.clone();
            accepted.revision = 3;
            accepted.transaction.block_time += 100;
            accepted.transaction.block_hash = "ef".repeat(32);
            accepted.transaction.accepting_block = Some("ef".repeat(32));
            apply_batch(&db, network, &batch(network, vec![accepted.clone()]))?;
            assert_eq!(
                apply_batch(&db, network, &batch(network, vec![accepted.clone()]))?.duplicates,
                1
            );
            let mut new = dm.clone();
            new.transaction.tx_id = "3f".repeat(32);
            let malformed = fixture(64, prefix, b"kchat:1:dm:BAD:alias:sealed");
            assert!(apply_batch(&db, network, &batch(network, vec![new, malformed])).is_err());
            assert!(
                payload.get_rtx(&db.read_tx(), &[63; 32])?.is_none(),
                "valid sibling rollback includes payload and inbox"
            );
            assert_eq!(
                inbox
                    .iter_by_tag_from_block_time(&db.read_tx(), &[0xab; 16], 0)
                    .count(),
                1
            );
            for wire in [
                b"ciph_msg:1:dm:abababababababababababababababab:alias:sealed".as_slice(),
                b"kchat:2:dm:abababababababababababababababab:alias:sealed",
            ] {
                assert!(!native_chat_payload(&faster_hex::hex_string(wire)));
            }
            drop(rows);
            drop(payload);
            drop(inbox);
            drop(by_sender);
            drop(db);
            let db = fjall::Config::new(temp.path()).open_transactional()?;
            assert_eq!(
                apply_batch(&db, network, &batch(network, vec![accepted]))?.duplicates,
                1
            );
            let part = ContextualMessageByInboxPartition::new(&db)?;
            let rows = part
                .iter_by_tag_from_block_time(&db.read_tx(), &[0xab; 16], 0)
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].0.block_time.get(),
                123456,
                "original native cursor retained across reaccept/restart"
            );
        }
        Ok(())
    }

    #[test]
    fn both_networks_native_storage_and_revision_replay_after_restart() -> Result<()> {
        for (network, prefix) in [
            ("mainnet", Prefix::Mainnet),
            ("testnet-10", Prefix::Testnet),
        ] {
            let temp = tempfile::tempdir()?;
            let db = fjall::Config::new(temp.path()).open_transactional()?;
            let one = fixture(1, prefix, b"kchat:1:handshake:\xff\x00:binary");
            let two = fixture(2, prefix, b"ciph_msg:1:payment:sealed");
            let b = batch(network, vec![one, two]);
            assert_eq!(apply_batch(&db, network, &b)?.applied, 2);
            assert_eq!(apply_batch(&db, network, &b)?.duplicates, 2);
            assert_eq!(
                TxIdToHandshakePartition::new(&db)?
                    .get_rtx(&db.read_tx(), &[1; 32])?
                    .unwrap()
                    .as_ref(),
                b"\xff\x00:binary"
            );
            assert!(
                TxIdToPaymentPartition::new(&db)?
                    .get_rtx(&db.read_tx(), &[2; 32])?
                    .is_some()
            );
            drop(db);
            let db = fjall::Config::new(temp.path()).open_transactional()?;
            assert_eq!(apply_batch(&db, network, &b)?.duplicates, 2);
        }
        Ok(())
    }

    #[test]
    fn revocation_and_stale_backfill_cannot_resurrect_acceptance() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let db = fjall::Config::new(temp.path()).open_transactional()?;
        let original = fixture(3, Prefix::Testnet, b"ciph_msg:1:handshake:sealed");
        apply_batch(
            &db,
            "testnet-10",
            &batch("testnet-10", vec![original.clone()]),
        )?;
        let mut revoked = original.clone();
        revoked.revision = 2;
        revoked.transaction.accepting_block = None;
        revoked.transaction.accepting_daa_score = None;
        assert_eq!(
            apply_batch(&db, "testnet-10", &batch("testnet-10", vec![revoked]))?.applied,
            1
        );
        assert_eq!(
            apply_batch(
                &db,
                "testnet-10",
                &batch("testnet-10", vec![original.clone()])
            )?
            .stale,
            1
        );
        assert!(
            TxIDToAcceptancePartition::new(&db)?
                .acceptance_by_tx_id_rtx(&db.read_tx(), &[3; 32])?
                .is_none()
        );
        assert!(
            TxIdToHandshakePartition::new(&db)?
                .get_rtx(&db.read_tx(), &[3; 32])?
                .is_some()
        );
        let mut reaccepted = original;
        reaccepted.revision = 3;
        reaccepted.transaction.accepting_block = Some("ef".repeat(32));
        reaccepted.transaction.block_hash = "ef".repeat(32);
        reaccepted.transaction.block_time += 100;
        apply_batch(&db, "testnet-10", &batch("testnet-10", vec![reaccepted]))?;
        assert_eq!(
            TxIDToAcceptancePartition::new(&db)?
                .acceptance_by_tx_id_rtx(&db.read_tx(), &[3; 32])?
                .unwrap()
                .header
                .accepting_block_hash,
            [0xef; 32]
        );
        let sender = address_on_network(
            &fixture(3, Prefix::Testnet, b"unused").transaction.sender,
            Prefix::Testnet,
        )?;
        let part = indexer_db::messages::handshake::HandshakeBySenderPartition::new(&db)?;
        let rtx = db.read_tx();
        let keys = part
            .iter_by_sender_from_block_time_rtx(&rtx, sender, 0)
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(keys.len(), 1);
        assert_eq!(u64::from(keys[0].block_time), 123456);
        assert_eq!(keys[0].block_hash, [0xab; 32]);
        Ok(())
    }

    #[test]
    fn wrong_network_conflicting_body_or_duplicate_revision_roll_back_batch() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let db = fjall::Config::new(temp.path()).open_transactional()?;
        let original = fixture(4, Prefix::Testnet, b"kchat:1:handshake:sealed");
        let initial = batch("testnet-10", vec![original.clone()]);
        apply_batch(&db, "testnet-10", &initial)?;
        let mut changed_source = initial.clone();
        changed_source.source_id = "b0b0b0b0-1111-4444-8888-012345678901".into();
        assert!(apply_batch(&db, "testnet-10", &changed_source).is_err());
        assert!(apply_batch(&db, "testnet-11", &initial).is_err());
        assert!(
            apply_batch(
                &db,
                "testnet-11",
                &batch("testnet-11", vec![original.clone()])
            )
            .is_err()
        );
        let mut conflict = original.clone();
        conflict.transaction.accepting_daa_score = Some(56);
        assert!(
            apply_batch(
                &db,
                "testnet-10",
                &batch(
                    "testnet-10",
                    vec![
                        fixture(5, Prefix::Testnet, b"kchat:1:handshake:other"),
                        conflict
                    ]
                )
            )
            .is_err()
        );
        assert!(
            TxIdToHandshakePartition::new(&db)?
                .get_rtx(&db.read_tx(), &[5; 32])?
                .is_none()
        );
        let mut changed = original;
        changed.revision = 3;
        changed.transaction.amount += 1;
        assert!(apply_batch(&db, "testnet-10", &batch("testnet-10", vec![changed])).is_err());
        Ok(())
    }

    #[test]
    fn internal_authorization_fails_closed() {
        let secret = "01234567890123456789012345678901";
        let mut headers = HeaderMap::new();
        assert!(!authorized(&headers, secret));
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {secret}").parse().unwrap(),
        );
        assert!(authorized(&headers, secret));
        assert!(!authorized(&headers, "short"));
        assert!(!authorized(&headers, "11234567890123456789012345678901"));
        assert!(network_matches("testnet", "testnet-10"));
        assert!(network_matches("testnet-10", "testnet-10"));
        assert!(!network_matches("testnet-11", "testnet-10"));
        assert!(!network_matches("mainnet", "testnet-10"));
        assert!(!network_matches("testnet", "testnet-010"));
    }

    #[test]
    fn public_posts_and_unknown_versions_never_enter_private_chat_storage() {
        for wire in [
            b"kchat:1:post:key:sig:body".as_slice(),
            b"kchat:2:comm:alias:body",
            b"k:1:post:key:sig:body",
            b"ciph_msg:1:tip:key:sig:post:author:aa",
            b"kchat:1:bcast:room:body",
        ] {
            assert!(!native_chat_payload(&faster_hex::hex_string(wire)));
        }
        assert!(native_chat_payload(&faster_hex::hex_string(
            b"kchat:1:handshake:\xff\x00:binary"
        )));
        assert!(native_chat_payload(&faster_hex::hex_string(
            b"ciph_msg:aaff"
        )));
    }
}
