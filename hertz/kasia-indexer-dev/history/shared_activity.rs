//! Ordinary payments from the one shared raw source to the existing dispatcher.
//! No chain subscription, public ingestion route or historical wallet alert scan.
use super::history::{address_on_network, configured_prefix};
use super::shared_ingestion::{JournalBatch, apply_batch};
use super::shared_notifications::{Source, source_identity};
use anyhow::{Context, Result, ensure};
use fjall::TxKeyspace;
use indexer_actors::push::{FundsPushEvent, funds_event, funds_transaction_id};
use indexer_db::processing::tx_id_to_acceptance::TxIDToAcceptancePartition;
use kaspa_rpc_core::{RpcBlock, RpcTransaction, api::rpc::RpcApi};
use kaspa_wrpc_client::KaspaRpcClient;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CONSUMER: &str = "native-address-push-v1";

pub struct Worker {
    stop: flume::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    pub fn start(
        db: TxKeyspace,
        rpc: KaspaRpcClient,
        sender: flume::Sender<FundsPushEvent>,
        import_lock: Arc<Mutex<()>>,
    ) -> Self {
        let (stop, stopped) = flume::bounded(1);
        let runtime = tokio::runtime::Handle::current();
        let thread = std::thread::spawn(move || {
            let mut current = None;
            loop {
                if stopped.try_recv().is_ok() || stopped.is_disconnected() {
                    break;
                }
                let result = (|| -> Result<bool> {
                    if current.is_none() {
                        current = Some(Activity::configured(&db, &import_lock)?);
                    }
                    let activity = current.as_mut().unwrap();
                    activity.run_one(&rpc, &runtime, &sender)
                })();
                let delay = match result {
                    Ok(true) => Duration::ZERO,
                    Ok(false) => Duration::from_millis(250),
                    Err(_) => {
                        // SQL/RPC errors may contain credentials or user data.
                        tracing::warn!(
                            "Shared address activity deferred; source or queue unavailable"
                        );
                        current = None;
                        Duration::from_secs(1)
                    }
                };
                match stopped.recv_timeout(delay) {
                    Ok(()) | Err(flume::RecvTimeoutError::Disconnected) => break,
                    Err(flume::RecvTimeoutError::Timeout) => {}
                }
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.stop.try_send(());
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                tracing::error!("Shared address activity worker panicked");
            }
        }
    }
}

struct Activity {
    source: Source,
    network: String,
    journal: String,
    native: TxKeyspace,
    acceptance: TxIDToAcceptancePartition,
}

#[derive(Clone)]
struct Candidate {
    id: Vec<u8>,
    body: Value,
    accepting: Vec<u8>,
}

fn bytes(value: &Value) -> Result<Vec<u8>> {
    let text = value.as_str().context("missing raw bytea")?;
    let hex = text.strip_prefix("\\x").context("invalid raw bytea")?;
    ensure!(hex.len() % 2 == 0, "invalid raw bytea length");
    let mut result = vec![0; hex.len() / 2];
    faster_hex::hex_decode(hex.as_bytes(), &mut result)?;
    Ok(result)
}

fn watched(body: &Value, network: &str) -> Result<bool> {
    if indexer_actors::watch_only_is_empty() {
        return Ok(false);
    }
    let prefix = configured_prefix(network)?;
    Ok(body["outputs"]
        .as_array()
        .context("missing raw outputs")?
        .iter()
        .any(|output| {
            let Some(text) = output["script_public_key_address"].as_str() else {
                return false;
            };
            let address = if text.contains(':') {
                text.to_owned()
            } else {
                format!("{prefix}:{text}")
            };
            address_on_network(&address, prefix)
                .is_ok_and(|a| indexer_actors::watch_only_contains(&a))
        }))
}

/// Raw rows omit script versions. Recompute the fetched transaction's SDK ID,
/// then check every retained field used for the alert against the same body.
fn proof<'a>(candidate: &Candidate, block: &'a RpcBlock) -> Result<&'a RpcTransaction> {
    ensure!(
        block.header.hash.as_bytes().as_slice() == bytes(&candidate.body["block_hash"])?,
        "containing block differs"
    );
    ensure!(
        Some(block.header.timestamp) == candidate.body["block_time"].as_u64(),
        "containing time differs"
    );
    let tx = block
        .transactions
        .iter()
        .find(|t| funds_transaction_id(t).is_ok_and(|id| id.as_slice() == candidate.id))
        .context("transaction absent from containing block")?;
    let payload = if candidate.body["payload"].is_null() {
        Vec::new()
    } else {
        bytes(&candidate.body["payload"])?
    };
    ensure!(tx.payload == payload, "raw payload differs");
    let outputs = candidate.body["outputs"]
        .as_array()
        .context("missing raw outputs")?;
    ensure!(
        outputs.len() == tx.outputs.len(),
        "raw output count differs"
    );
    for (index, output) in tx.outputs.iter().enumerate() {
        let raw = outputs
            .iter()
            .find(|r| r["index"].as_u64() == Some(index as u64))
            .context("missing raw output index")?;
        ensure!(
            raw["amount"].as_u64() == Some(output.value)
                && bytes(&raw["script_public_key"])? == output.script_public_key.script(),
            "raw output differs"
        );
    }
    let empty = Vec::new();
    let inputs = candidate.body["inputs"].as_array().unwrap_or(&empty);
    ensure!(inputs.len() == tx.inputs.len(), "raw input count differs");
    for (index, input) in tx.inputs.iter().enumerate() {
        let raw = inputs
            .iter()
            .find(|r| r["index"].as_u64() == Some(index as u64))
            .context("missing raw input index")?;
        ensure!(
            bytes(&raw["previous_outpoint_hash"])?
                == input.previous_outpoint.transaction_id.as_bytes()
                && raw["previous_outpoint_index"].as_u64()
                    == Some(input.previous_outpoint.index as u64),
            "raw input differs"
        );
    }
    Ok(tx)
}

impl Activity {
    fn configured(db: &TxKeyspace, import_lock: &Mutex<()>) -> Result<Self> {
        let mut source = Source::configured()?;
        let network = std::env::var("KASANOVA_SHARED_NETWORK")?;
        let mut sql = source.client.transaction()?;
        sql.batch_execute(
            "SET LOCAL statement_timeout='3s';
            SELECT observed_at,handled FROM protocol_indexer.activity_transactions LIMIT 0;
            SELECT queued_at,available_at FROM protocol_indexer.activity_pending LIMIT 0",
        )?;
        let journal = source_identity(&mut sql, &source.binding, &network)?;
        sql.commit()?;
        {
            let _guard = import_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("shared import unavailable"))?;
            // Same native binding and optimistic transaction as quiet imports.
            apply_batch(
                db,
                &network,
                &JournalBatch {
                    schema_version: 1,
                    source_id: journal.clone(),
                    network: network.clone(),
                    transactions: vec![],
                    notification: false,
                },
            )?;
        }
        let mut sql = source.client.transaction()?;
        sql.batch_execute("SET LOCAL statement_timeout='3s'")?;
        ensure!(
            source_identity(&mut sql, &source.binding, &network)? == journal,
            "activity source changed"
        );
        // First actual native activation, not the earlier schema installation.
        sql.execute(
            "INSERT INTO protocol_indexer.notification_consumers(consumer,activated_at_ms)
            VALUES($1,(extract(epoch FROM clock_timestamp())*1000)::bigint) ON CONFLICT DO NOTHING",
            &[&CONSUMER],
        )?;
        sql.commit()?;
        Ok(Self {
            source,
            network,
            journal,
            native: db.clone(),
            acceptance: TxIDToAcceptancePartition::new(db)?,
        })
    }

    fn run_one(
        &mut self,
        rpc: &KaspaRpcClient,
        runtime: &tokio::runtime::Handle,
        sender: &flume::Sender<FundsPushEvent>,
    ) -> Result<bool> {
        self.run_one_with_fetch(
            |candidate| {
                let hash =
                    faster_hex::hex_string(&bytes(&candidate.body["block_hash"])?).parse()?;
                Ok(runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(10), rpc.get_block(hash, true)).await
                })??)
            },
            sender,
        )
    }

    fn run_one_with_fetch(
        &mut self,
        fetch: impl FnOnce(&Candidate) -> Result<RpcBlock>,
        sender: &flume::Sender<FundsPushEvent>,
    ) -> Result<bool> {
        // Fair claim is committed before RPC. Failed work remains pending and
        // cannot permanently sit ahead of other payments.
        let mut sql = self.source.client.transaction()?;
        sql.batch_execute("SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='500ms'")?;
        ensure!(
            source_identity(&mut sql, &self.source.binding, &self.network)? == self.journal,
            "activity source changed"
        );
        native_binding(&self.native, &self.network, &self.journal)?;
        let epoch: i64 = sql
            .query_one(
                "SELECT activated_at_ms FROM protocol_indexer.notification_consumers
            WHERE consumer=$1 FOR SHARE",
                &[&CONSUMER],
            )?
            .get(0);
        let network = self
            .network
            .parse::<kaspa_wrpc_client::prelude::NetworkId>()?
            .network_type();
        let addresses = indexer_actors::watch_only_snapshot()
            .iter()
            .filter_map(|a| crate::api::to_rpc_address(a, network).ok().flatten())
            .flat_map(|a| {
                let full = a.to_string();
                let bare = full.split_once(':').unwrap().1.to_owned();
                [full, bare]
            })
            .collect::<Vec<_>>();
        // Bulk quiet ACKs keep ordinary chain traffic out of per-payment RPC
        // and multi-roundtrip work. The same watch union is checked again by
        // the native factory and precise per-device dispatcher for real hits.
        let quiet = sql.execute("WITH quiet AS MATERIALIZED (
            SELECT p.transaction_id FROM protocol_indexer.activity_pending p
            JOIN public.transactions t USING(transaction_id)
            JOIN protocol_indexer.activity_transactions s USING(transaction_id)
            WHERE p.available_at<=clock_timestamp() AND (s.handled
                OR s.observed_at<to_timestamp($1::bigint/1000.0) OR p.queued_at<to_timestamp($1::bigint/1000.0)
                OR t.block_time IS NULL OR t.block_time<$1::bigint
                OR NOT EXISTS(SELECT 1 FROM unnest(t.outputs) o WHERE o.script_public_key_address=ANY($2::text[])))
            ORDER BY p.available_at,p.transaction_id LIMIT 5000
            FOR UPDATE OF p,s SKIP LOCKED FOR SHARE OF t SKIP LOCKED),
            marked AS (UPDATE protocol_indexer.activity_transactions s SET handled=true FROM quiet q
                WHERE s.transaction_id=q.transaction_id RETURNING s.transaction_id)
            DELETE FROM protocol_indexer.activity_pending p USING marked m WHERE p.transaction_id=m.transaction_id",
            &[&epoch,&addresses])?;
        let row = sql.query_opt("UPDATE protocol_indexer.activity_pending SET available_at=clock_timestamp()+interval '1 second'
            WHERE transaction_id=(SELECT transaction_id FROM protocol_indexer.activity_pending
                WHERE available_at<=clock_timestamp() ORDER BY available_at,transaction_id LIMIT 1 FOR UPDATE SKIP LOCKED)
            RETURNING transaction_id", &[])?;
        sql.commit()?;
        let Some(row) = row else {
            return Ok(quiet > 0);
        };
        let id: Vec<u8> = row.get(0);
        let candidate = {
            let mut sql = self.source.client.transaction()?;
            let candidate = fence(
                &mut sql,
                &self.source.binding,
                &self.network,
                &self.journal,
                &self.native,
                &id,
            )?;
            sql.commit()?;
            candidate
        };
        let Some(candidate) = candidate else {
            return Ok(true);
        };
        let block = fetch(&candidate)?;
        let tx = proof(&candidate, &block)?;
        let mut sql = self.source.client.transaction()?;
        let current = fence(
            &mut sql,
            &self.source.binding,
            &self.network,
            &self.journal,
            &self.native,
            &id,
        )?;
        if let Some(current) = current {
            if current.body != candidate.body || current.accepting != candidate.accepting {
                sql.commit()?;
                return Ok(true);
            }
            let id: [u8; 32] = id.as_slice().try_into()?;
            if let Some(event) = funds_event(tx, id, &block.header, || {
                tx.inputs.first().and_then(|input| {
                    self.acceptance
                        .key_by_tx_id(input.previous_outpoint.transaction_id.as_ref())
                        .ok()
                        .flatten()
                        .map(|k| k.receiver)
                })
            }) {
                sender
                    .try_send(event)
                    .map_err(|_| anyhow::anyhow!("funds dispatcher queue unavailable"))?;
            }
            acknowledge(&mut sql, &id.to_vec())?;
        }
        // Existing best-effort external delivery: queue precedes SQL commit.
        sql.commit()?;
        Ok(true)
    }
}

fn acknowledge(sql: &mut postgres::Transaction<'_>, id: &Vec<u8>) -> Result<()> {
    sql.execute(
        "UPDATE protocol_indexer.activity_transactions SET handled=true WHERE transaction_id=$1",
        &[id],
    )?;
    sql.execute(
        "DELETE FROM protocol_indexer.activity_pending WHERE transaction_id=$1",
        &[id],
    )?;
    Ok(())
}

fn native_binding(native: &TxKeyspace, network: &str, journal: &str) -> Result<()> {
    let config = native.open_partition(
        "kasanova_shared_ingestion_config",
        fjall::PartitionCreateOptions::default(),
    )?;
    let read = native.read_tx();
    ensure!(
        read.get(&config, b"network")?
            .is_some_and(|v| v.as_ref() == network.as_bytes())
            && read
                .get(&config, b"source_id")?
                .is_some_and(|v| v.as_ref() == journal.as_bytes()),
        "activity native binding differs"
    );
    Ok(())
}

/// This transaction owns all guards through queue admission and acknowledgement.
fn fence(
    sql: &mut postgres::Transaction<'_>,
    binding: &Value,
    network: &str,
    journal: &str,
    native: &TxKeyspace,
    id: &Vec<u8>,
) -> Result<Option<Candidate>> {
    sql.batch_execute("SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='500ms'; SET LOCAL synchronous_commit=on")?;
    ensure!(
        source_identity(sql, binding, network)? == journal,
        "activity source changed"
    );
    native_binding(native, network, journal)?;
    let epoch: i64 = sql
        .query_one(
            "SELECT activated_at_ms FROM protocol_indexer.notification_consumers
        WHERE consumer=$1 FOR SHARE",
            &[&CONSUMER],
        )?
        .get(0);
    let pending = sql.query_opt(
        "SELECT (extract(epoch FROM queued_at)*1000)::bigint FROM protocol_indexer.activity_pending
        WHERE transaction_id=$1 FOR UPDATE SKIP LOCKED",
        &[id],
    )?;
    let Some(pending) = pending else {
        return Ok(None);
    };
    let raw = sql.query_opt("SELECT to_jsonb(t)::text FROM public.transactions t WHERE transaction_id=$1 FOR SHARE SKIP LOCKED", &[id])?;
    let Some(raw) = raw else {
        if !sql
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM public.transactions WHERE transaction_id=$1)",
                &[id],
            )?
            .get::<_, bool>(0)
        {
            // An uncommitted later body always enqueues its own wakeup.
            sql.execute(
                "DELETE FROM protocol_indexer.activity_pending WHERE transaction_id=$1",
                &[id],
            )?;
        }
        return Ok(None);
    };
    let body: Value = serde_json::from_str(&raw.get::<_, String>(0))?;
    let state = sql.query_opt("SELECT handled,(extract(epoch FROM observed_at)*1000)::bigint FROM protocol_indexer.activity_transactions
        WHERE transaction_id=$1 FOR UPDATE SKIP LOCKED", &[id])?;
    let Some(state) = state else {
        if !sql.query_one("SELECT EXISTS(SELECT 1 FROM protocol_indexer.activity_transactions WHERE transaction_id=$1)", &[id])?.get::<_,bool>(0) {
            // Retained pre-install raw rows never become historical alerts.
            sql.execute("DELETE FROM protocol_indexer.activity_pending WHERE transaction_id=$1", &[id])?;
        }
        return Ok(None);
    };
    if state.get::<_, bool>(0)
        || state.get::<_, i64>(1) < epoch
        || pending.get::<_, i64>(0) < epoch
        || body["block_time"].as_i64().is_none_or(|t| t < epoch)
    {
        acknowledge(sql, id)?;
        return Ok(None);
    }
    // Match the bulk path: a currently unwatched body is a quiet observation,
    // regardless of when its acceptance arrives. Only an alert needs proof.
    if !watched(&body, network)? {
        acknowledge(sql, id)?;
        return Ok(None);
    }
    let accepted = sql.query_opt(
        "SELECT block_hash,is_archive_seed FROM public.transactions_acceptances
        WHERE transaction_id=$1 FOR SHARE SKIP LOCKED",
        &[id],
    )?;
    let Some(accepted) = accepted else {
        return Ok(None);
    };
    if accepted.get::<_, bool>(1) {
        acknowledge(sql, id)?;
        return Ok(None);
    }
    Ok(Some(Candidate {
        id: id.clone(),
        body,
        accepting: accepted.get(0),
    }))
}

#[cfg(test)]
#[path = "shared_activity_tests.rs"]
mod tests;
