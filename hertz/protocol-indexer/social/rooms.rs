//! Public-room format and pure self-send rule follow KaChat e2f4621 (ISC).
//! Sender proof uses the existing canonical/evidence path, not a new RPC client.
use crate::{
    database::Transaction,
    tip_evidence::TipOutputEvidence,
    tip_projection::TipReconcile,
    transaction_evidence::{CheckedTransaction, unsigned},
};
use anyhow::{Context, Result, ensure};
use kaspa_consensus_core::network::{NetworkId, NetworkType};
use kaspa_txscript::extract_script_pub_key_address;
use kaspa_wallet_core::prelude::AddressPrefix;
use serde_json::Value;
use sqlx::{Postgres, Row};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub(crate) struct RoomEnvelope {
    transaction: Transaction,
    channel: String,
    content: String,
}
impl RoomEnvelope {
    pub(crate) fn parse(transaction: &Transaction) -> Option<Self> {
        if transaction.transaction_id.len() != 64
            || hex::decode(&transaction.transaction_id).is_err()
        {
            return None;
        }
        let payload = hex::decode(transaction.payload.as_ref()?).ok()?;
        let raw = std::str::from_utf8(&payload).ok()?;
        let rest = raw
            .strip_prefix("kchat:1:bcast:")
            .or_else(|| raw.strip_prefix("ciph_msg:1:bcast:"))?;
        let (channel, content) = rest.split_once(':')?;
        let channel = channel.trim().to_lowercase();
        // PostgreSQL text cannot contain NUL. Check after Unicode normalization
        // (which can expand), without modifying the supported content bytes.
        if channel.is_empty()
            || channel.chars().count() > 36
            || channel.contains('\0')
            || content.contains('\0')
        {
            return None;
        }
        Some(Self {
            transaction: transaction.clone(),
            channel,
            content: content.into(),
        })
    }
    pub(crate) fn subject(&self) -> (String, &'static str) {
        (
            format!("kachat_broadcasts:{}", self.transaction.transaction_id),
            "kachat_broadcasts",
        )
    }
}

// The upstream judge's pure rule. Both addresses below come from SDK-decoded
// scripts, never a payload's claimed author or an archive address string.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    Verified(String),
    Forged { input: String, output: String },
    Unknown,
}
pub(crate) fn judge(input_address: Option<&str>, output_address: Option<&str>) -> Verdict {
    let clean = |a: Option<&str>| a.map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty());
    match (clean(input_address), clean(output_address)) {
        (Some(i), Some(o)) if i == o => Verdict::Verified(i),
        (Some(input), Some(output)) => Verdict::Forged { input, output },
        _ => Verdict::Unknown,
    }
}
fn bytes(value: &Value) -> Result<Vec<u8>> {
    Ok(hex::decode(
        value.as_str().context("missing receipt bytes")?,
    )?)
}
fn canonical_bytes(value: &Value) -> Result<Vec<u8>> {
    Ok(hex::decode(
        value
            .as_str()
            .and_then(|s| s.strip_prefix("\\x"))
            .context("missing canonical bytea")?,
    )?)
}

pub(crate) fn sender(network: &str, id: &[u8], body: &Value, receipt: &Value) -> Result<Verdict> {
    // Includes SDK current-ID binding, all output amounts/scripts/indexes and
    // payload/network/ID consistency. This same proof protects native tips.
    let current = TipOutputEvidence::checked(network, id, body, receipt)?;
    let tx = current.transaction();
    let inputs = body["inputs"]
        .as_array()
        .context("canonical room inputs pending")?;
    ensure!(
        !inputs.is_empty() && inputs.len() == tx.inputs.len(),
        "room input count mismatch"
    );
    let mut ordered = inputs
        .iter()
        .map(|v| Ok((unsigned(&v["index"])?, v)))
        .collect::<Result<Vec<_>>>()?;
    ordered.sort_by_key(|v| v.0);
    for (index, ((source_index, source), input)) in ordered.iter().zip(&tx.inputs).enumerate() {
        ensure!(*source_index == index as u64, "room input indexes mismatch");
        ensure!(
            canonical_bytes(&source["previous_outpoint_hash"])?
                == input.previous_outpoint.transaction_id.as_bytes()
                && unsigned(&source["previous_outpoint_index"])?
                    == u64::from(input.previous_outpoint.index),
            "room input outpoint mismatch"
        );
        if !source["signature_script"].is_null() {
            ensure!(
                canonical_bytes(&source["signature_script"])? == input.signature_script,
                "room input signature bytes mismatch"
            );
        }
    }
    let input = &tx.inputs[0];
    let previous = &receipt["input0"]["previous_transaction"];
    let previous_id = input.previous_outpoint.transaction_id.to_string();
    ensure!(
        previous["schema_version"].as_u64() == Some(1)
            && previous["network"].as_str() == Some(network)
            && previous["transaction_id"].as_str() == Some(previous_id.as_str()),
        "room previous receipt identity/network mismatch"
    );
    // The prior SDK ID, not its verbose/address fields, binds the spent output.
    let previous = CheckedTransaction::checked(&previous["rpc_transaction"], &previous_id)?;
    let previous_output = previous
        .transaction()
        .outputs
        .get(input.previous_outpoint.index as usize)
        .context("room previous output missing")?;
    let proof = &receipt["input0"];
    ensure!(
        bytes(&proof["script"])? == previous_output.script_public_key.script()
            && unsigned(&proof["amount"])? == previous_output.value,
        "room previous output proof mismatch"
    );
    let source = ordered[0].1;
    if !source["previous_outpoint_script"].is_null() {
        ensure!(
            canonical_bytes(&source["previous_outpoint_script"])?
                == previous_output.script_public_key.script(),
            "resolved canonical input script mismatch"
        );
    }
    if !source["previous_outpoint_amount"].is_null() {
        ensure!(
            unsigned(&source["previous_outpoint_amount"])? == previous_output.value,
            "resolved canonical input amount mismatch"
        );
    }
    let network = NetworkId::from_str(network)?;
    let prefix = match network.network_type() {
        NetworkType::Mainnet => AddressPrefix::Mainnet,
        NetworkType::Testnet => AddressPrefix::Testnet,
        _ => anyhow::bail!("unsupported configured room network"),
    };
    let output = tx.outputs.first().context("room output0 missing")?;
    let input_address = extract_script_pub_key_address(&previous_output.script_public_key, prefix)
        .ok()
        .map(|v| v.to_string());
    let output_address = extract_script_pub_key_address(&output.script_public_key, prefix)
        .ok()
        .map(|v| v.to_string());
    if !proof["address"].is_null() {
        ensure!(
            proof["address"].as_str().is_some() && proof["address"].as_str() == input_address.as_deref(),
            "claimed room input address mismatch"
        );
    }
    Ok(judge(input_address.as_deref(), output_address.as_deref()))
}

pub(crate) async fn reconcile(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    network: &str,
    id: &[u8],
    revision: i64,
    accepted: bool,
    body: &Value,
    room: &RoomEnvelope,
) -> Result<TipReconcile> {
    sqlx::query("SELECT set_config('kasanova.social_notify_allowed','false',true)")
        .execute(&mut **tx)
        .await?;
    if !accepted {
        sqlx::query("DELETE FROM kachat_broadcasts WHERE transaction_id=$1")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        return Ok(TipReconcile::Applied);
    }
    let receipt: Option<(String,String)> = sqlx::query_as(
        "SELECT receipt::text,encode(sha256(convert_to(receipt::text,'UTF8')),'hex') FROM protocol_indexer.evidence WHERE transaction_id=$1",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((receipt, fingerprint)) = receipt else {
        return Ok(TipReconcile::SenderProofPending);
    };
    let receipt: Value = match serde_json::from_str(&receipt) {
        Ok(v) => v,
        Err(_) => return Ok(TipReconcile::InvalidSenderProofPending(fingerprint)),
    };
    let verdict = match sender(network, id, body, &receipt) {
        Ok(v) => v,
        Err(_) => return Ok(TipReconcile::InvalidSenderProofPending(fingerprint)),
    };
    let Verdict::Verified(sender) = verdict else {
        // Complete SDK-bound mismatched/non-addressable scripts cannot gain
        // attribution through more archive retries. Raw protocol stays retained.
        sqlx::query("DELETE FROM kachat_broadcasts WHERE transaction_id=$1")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM social_shared.room_facts WHERE transaction_id=$1")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        return Ok(TipReconcile::Applied);
    };
    let action=sqlx::query("SELECT original_time,snapshot::text AS snapshot FROM social_shared.actions WHERE transaction_id=$1").bind(id).fetch_one(&mut **tx).await?;
    let inserted=sqlx::query("INSERT INTO kachat_broadcasts(transaction_id,block_time,channel,sender_address,content) VALUES($1,$2,$3,$4,$5) ON CONFLICT(transaction_id) DO NOTHING")
        .bind(id).bind(action.get::<i64,_>("original_time")).bind(&room.channel).bind(&sender).bind(&room.content).execute(&mut **tx).await?.rows_affected();
    if inserted > 0 {
        crate::shared_social::restore_snapshot(
            tx,
            "kachat_broadcasts",
            id,
            action.get::<Option<String>, _>("snapshot").as_deref(),
        )
        .await?;
        crate::shared_social::capture_snapshot(tx, "kachat_broadcasts", id).await?;
    }
    sqlx::query("INSERT INTO social_shared.room_facts VALUES($1,$2,$3) ON CONFLICT(transaction_id) DO UPDATE SET revision=EXCLUDED.revision,sender_address=EXCLUDED.sender_address")
        .bind(id).bind(revision).bind(sender).execute(&mut **tx).await?;
    sqlx::query("UPDATE social_shared.actions SET status='verified',notification_consumed=true WHERE transaction_id=$1").bind(id).execute(&mut **tx).await?;
    Ok(TipReconcile::Applied)
}
