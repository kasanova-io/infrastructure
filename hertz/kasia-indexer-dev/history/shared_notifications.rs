//! Live-only admission to the existing PushDispatcher. Imports remain quiet.
use super::history::{address_on_network, configured_prefix};
use super::shared_ingestion::{JournalBatch, JournalResult, JournalTx};
use anyhow::{Context, Result, ensure};
use fjall::{PartitionCreateOptions, TxKeyspace};
use indexer_actors::push::{PushEvent, PushEventKind, parse_self_stash_alias};
use indexer_db::AddressPayload;
use protocol::operation::{SealedOperation, deserializer::parse_shared_operation};

fn fixed<const N: usize>(value: &[u8]) -> Result<[u8; N]> {
    ensure!(value.len() == 2 * N, "invalid fixed hex length");
    let mut bytes = [0; N];
    faster_hex::hex_decode(value, &mut bytes)?;
    Ok(bytes)
}

fn event(incoming: &JournalTx, network: &str) -> Result<Option<PushEvent>> {
    let t = &incoming.transaction;
    let prefix = configured_prefix(network)?;
    let sender = address_on_network(&t.sender, prefix)?;
    let receiver = address_on_network(&t.receiver, prefix)?;
    let mut payload = vec![0; t.payload.len() / 2];
    ensure!(t.payload.len() % 2 == 0, "invalid payload hex");
    faster_hex::hex_decode(t.payload.as_bytes(), &mut payload)?;
    let operation = parse_shared_operation(&payload).context("unsupported Chat operation")?;
    let mut result = PushEvent {
        kind: PushEventKind::Handshake,
        watched_address: receiver,
        sender,
        receiver,
        alias: None,
        tx_id: fixed(t.tx_id.as_bytes())?,
        amount: None,
        payload: None,
        timestamp: t.block_time,
        daa_score: t.accepting_daa_score.context("notification requires acceptance")?,
        blinded_group_id: None,
        group_control_recipient: None,
    };
    let sealed = match operation {
        SealedOperation::SealedMessageOrSealedHandshakeVNone(v) => v.sealed_hex,
        SealedOperation::SealedHandshakeV2(v) => v.sealed_hex,
        SealedOperation::ContextualMessageV1(v) => {
            result.kind = PushEventKind::Contextual;
            result.watched_address = sender;
            // Match live BlockProcessor push matching. Storage's 16-byte alias
            // key is a separate contract and must not narrow subscriptions.
            result.alias = Some(String::from_utf8_lossy(v.alias).into_owned());
            v.sealed_hex
        }
        SealedOperation::PaymentV1(v) => {
            if sender == receiver {
                return Ok(None);
            }
            result.kind = PushEventKind::Payment;
            result.amount = Some(t.amount);
            // Push payload uses the original wire representation. The native
            // database's tip hex decoding is intentionally a separate contract.
            result.payload = Some(String::from_utf8_lossy(v.sealed_hex).into_owned());
            return Ok(Some(result));
        }
        SealedOperation::SelfStashV1(v) => {
            result.kind = PushEventKind::SelfStash;
            result.watched_address = sender;
            result.receiver = AddressPayload::default();
            result.alias = parse_self_stash_alias(v.key.unwrap_or_default());
            v.sealed_hex
        }
        SealedOperation::GroupMessageV1(v) => {
            let sender_pubkey = fixed::<32>(v.sender_pub)?;
            ensure!(sender.matches_xonly_pubkey(&sender_pubkey), "group sender mismatch");
            result.kind = PushEventKind::GroupMessage;
            result.watched_address = AddressPayload::default();
            result.receiver = AddressPayload::default();
            result.blinded_group_id = Some(fixed(v.blinded_group_id)?);
            v.sealed_hex
        }
        SealedOperation::GroupControlV1(v) => {
            result.kind = PushEventKind::GroupControl;
            result.watched_address = sender;
            result.group_control_recipient = v.recipient_pubkey
                .map(fixed::<32>).transpose()?.map(AddressPayload::from_xonly_pubkey);
            result.receiver = result.group_control_recipient.unwrap_or_default();
            v.encrypted_payload
        }
    };
    result.payload = Some(String::from_utf8_lossy(sealed).into_owned());
    Ok(Some(result))
}

/// A configured private source, never a caller-supplied URL or binding.
pub(super) struct Source {
    pub(super) client: postgres::Client,
    pub(super) binding: serde_json::Value,
}

impl Source {
    pub(super) fn configured() -> Result<Self> {
        use std::str::FromStr;
        let binding_path = std::env::var("PROTOCOL_SOURCE_BINDING")
            .unwrap_or_else(|_| "/run/protocol-indexer/source-binding.json".into());
        let binding: serde_json::Value = serde_json::from_slice(
            &std::fs::read(binding_path).context("notification source binding unavailable")?,
        ).context("invalid notification source binding")?;
        let url = std::env::var("PROTOCOL_DATABASE_URL")
            .context("notification source configuration unavailable")?;
        let mut config = postgres::Config::from_str(&url)
            .map_err(|_| anyhow::anyhow!("invalid notification source configuration"))?;
        let expected = &binding["database"];
        let port = u16::try_from(expected["port"].as_u64().context("missing source port")?)
            .context("invalid source port")?;
        ensure!(port != 0, "invalid source port");
        ensure!(config.get_hosts().len() == 1
            && matches!(&config.get_hosts()[0], postgres::config::Host::Tcp(host)
                if Some(host.as_str()) == expected["host"].as_str())
            && config.get_dbname() == expected["name"].as_str()
            && config.get_ports().len() <= 1
            && config.get_ports().first().copied().unwrap_or(5432) == port,
            "notification source endpoint differs from binding");
        config.connect_timeout(std::time::Duration::from_secs(3));
        let client = config.connect(postgres::NoTls)
            .map_err(|_| anyhow::anyhow!("notification source unavailable"))?;
        Ok(Self { client, binding })
    }
}

/// Called under ExportApi's shared import/notification mutex. This handler owns
/// its SQL fence itself: a delayed or disconnected HTTP caller cannot leave it
/// authorizing against a stale native ledger. Queue/delivery has the existing
/// dispatcher's best-effort semantics, not crash-exactly-once external delivery.
pub(super) fn notify_batch(
    db: &TxKeyspace,
    network: &str,
    batch: &JournalBatch,
    sender: Option<&flume::Sender<PushEvent>>,
) -> Result<JournalResult> {
    notify_with_source(db, network, batch, sender, &mut Source::configured()?)
}

fn notify_with_source(
    db: &TxKeyspace,
    network: &str,
    batch: &JournalBatch,
    sender: Option<&flume::Sender<PushEvent>>,
    source: &mut Source,
) -> Result<JournalResult> {
    ensure!(batch.notification && batch.schema_version == 1, "invalid notification request");
    ensure!(batch.network == network, "notification network mismatch");
    ensure!(batch.transactions.len() == 1, "one notification transaction required");
    let config = db.open_partition("kasanova_shared_ingestion_config", PartitionCreateOptions::default())?;
    let ledger = db.open_partition("kasanova_shared_ingestion", PartitionCreateOptions::default())?;
    let rtx = db.read_tx();
    ensure!(rtx.get(&config, b"network")?.is_some_and(|v| v.as_ref() == network.as_bytes()),
        "notification network is not bound");
    ensure!(rtx.get(&config, b"source_id")?.is_some_and(|v| v.as_ref() == batch.source_id.as_bytes()),
        "notification journal identity mismatch");
    let incoming = &batch.transactions[0];
    let tx_id = fixed::<32>(incoming.transaction.tx_id.as_bytes())?.to_vec();
    let old = rtx.get(&ledger, &tx_id)?.context("notification transaction is not imported")?;
    let imported: JournalTx = serde_json::from_slice(old.as_ref())?;
    ensure!(imported == *incoming, "notification differs from current imported revision");
    let accepted = incoming.transaction.accepting_block.as_deref()
        .context("notification transaction is revoked")?;
    ensure!(incoming.transaction.accepting_daa_score.is_some(), "notification transaction is revoked");

    let tx = source.client.transaction()
        .map_err(|_| anyhow::anyhow!("notification source unavailable"))?;
    admit(tx, &source.binding, network, batch, incoming, &tx_id, accepted, |push| {
            sender.context("existing push dispatcher unavailable")?.try_send(push)
                .map_err(|_| anyhow::anyhow!("existing push dispatcher queue unavailable"))
        })
        // SQL/connection errors must never return a configured DSN or credential.
        .map_err(|_| anyhow::anyhow!("notification source admission unavailable"))
}

/// Validate the configured SQL instance before using any notification data.
pub(super) fn source_identity(
    sql: &mut postgres::Transaction<'_>, binding: &serde_json::Value, network: &str,
) -> Result<String> {
    let identity = sql.query_one("SELECT c.network,c.journal_id::text,c.source_revision,
        system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),current_database()
        FROM protocol_indexer.config c CROSS JOIN pg_control_system()", &[])?;
    let expected = &binding["database"];
    ensure!(binding["schema_version"] == 1 && binding["network"].as_str() == Some(network)
        && identity.get::<_, String>(0) == network
        && Some(identity.get::<_, String>(2).as_str()) == binding["source_revision"].as_str()
        && Some(identity.get::<_, String>(3).as_str()) == expected["system_identifier"].as_str()
        && Some(identity.get::<_, i64>(4)) == expected["oid"].as_i64()
        && Some(identity.get::<_, String>(5).as_str()) == expected["name"].as_str(),
        "notification SQL identity differs from bound source");
    Ok(identity.get(1))
}

fn admit(
    mut sql: postgres::Transaction<'_>,
    binding: &serde_json::Value,
    network: &str,
    batch: &JournalBatch,
    incoming: &JournalTx,
    tx_id: &Vec<u8>,
    accepted: &str,
    dispatch: impl FnOnce(PushEvent) -> Result<()>,
) -> Result<JournalResult> {
    sql.batch_execute("SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='500ms'; SET LOCAL synchronous_commit=on")?;
    ensure!(source_identity(&mut sql, binding, network)? == batch.source_id,
        "notification journal identity mismatch");
    // SKIP LOCKED never waits in the opposite order to a collector/capture
    // transaction. The real acceptance and canonical guards survive queue entry.
    let current = sql.query_opt("SELECT c.revision,encode(c.accepting_block,'hex'),c.source_transaction->>'payload'
        FROM protocol_indexer.canonical c JOIN public.transactions_acceptances a
          ON a.transaction_id=c.transaction_id AND a.block_hash=c.accepting_block AND NOT a.is_archive_seed
        WHERE c.transaction_id=$1 AND NOT EXISTS (SELECT 1 FROM protocol_indexer.pending p
            WHERE p.transaction_id=c.transaction_id AND p.acceptance_changed)
        FOR SHARE OF c,a SKIP LOCKED", &[tx_id])?.context("notification acceptance pending")?;
    ensure!(current.get::<_, i64>(0) == i64::try_from(incoming.revision)?
        && current.get::<_, String>(1) == accepted
        && current.get::<_, String>(2) == format!("\\x{}", incoming.transaction.payload),
        "notification canonical revision differs");
    let eligible = sql.query_opt("SELECT e.event_id FROM protocol_indexer.events e
        JOIN protocol_indexer.notification_consumers n ON n.consumer='native-chat-push-v1'
        WHERE e.transaction_id=$1 AND e.notify_allowed
          AND e.recorded_at >= to_timestamp(n.activated_at_ms / 1000.0)
          AND (e.source_transaction->>'block_time')::bigint >= n.activated_at_ms
        ORDER BY e.event_id LIMIT 1 FOR UPDATE OF e SKIP LOCKED", &[tx_id])?
        .context("notification is historical or ineligible")?;
    let event_id: i64 = eligible.get(0);
    if sql.query_opt("SELECT 1 FROM protocol_indexer.deliveries WHERE consumer='native-chat-push-v1' AND event_id=$1", &[&event_id])?.is_some() {
        sql.commit()?;
        return Ok(JournalResult { applied: 0, duplicates: 1, stale: 0 });
    }
    if let Some(push) = event(incoming, network)? {
        dispatch(push)?;
    }
    sql.execute("INSERT INTO protocol_indexer.deliveries(consumer,event_id) VALUES ('native-chat-push-v1',$1)", &[&event_id])?;
    sql.commit()?;
    Ok(JournalResult { applied: 1, duplicates: 0, stale: 0 })
}

#[cfg(test)]
#[path = "shared_notification_tests.rs"]
mod tests;
