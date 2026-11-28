//! Public-room format and pure self-send rule follow KaChat e2f4621 (ISC).
//! Sender proof uses the existing canonical/evidence path, not a new RPC client.
use crate::{
    database::Transaction,
    tip_evidence::TipOutputEvidence,
    tip_projection::TipReconcile,
    transaction_evidence::{CheckedTransaction, unsigned},
};
use anyhow::{Context, Result, ensure};
use kaspa_consensus_core::{
    hashing::sighash::SigHashReusedValuesUnsync,
    network::{NetworkId, NetworkType},
    tx::{PopulatedTransaction, ScriptPublicKey, UtxoEntry},
};
use kaspa_txscript::{
    EngineCtx, EngineFlags, TxScriptEngine, caches::Cache, extract_script_pub_key_address,
};
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

fn checked_current_input<'a>(
    network: &str,
    id: &[u8],
    body: &'a Value,
    receipt: &Value,
) -> Result<(TipOutputEvidence, &'a Value)> {
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
    Ok((current, ordered[0].1))
}

pub(crate) fn sender(network: &str, id: &[u8], body: &Value, receipt: &Value) -> Result<Verdict> {
    let (current, source) = checked_current_input(network, id, body, receipt)?;
    let tx = current.transaction();
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

pub(crate) fn sender_if_complete(
    network: &str,
    id: &[u8],
    body: &Value,
    receipt: &Value,
) -> Result<Option<Verdict>> {
    if receipt.get("input0").is_none() {
        // Ordinary archive admission stores the current transaction first.
        // Request funding enrichment only after checking that current proof;
        // malformed current/input bindings must retain fingerprinted rejection.
        return signed_author_if_complete(network, id, body, receipt);
    }
    // A present but malformed/rejected funding proof still requires a fresh
    // receipt, including explicit null; never downgrade it to missing evidence.
    sender(network, id, body, receipt).map(Some)
}

fn signed_author_if_complete(
    network: &str,
    id: &[u8],
    body: &Value,
    receipt: &Value,
) -> Result<Option<Verdict>> {
    let (current, source) = checked_current_input(network, id, body, receipt)?;
    let tx = current.transaction();
    // An independently verified author signature can outlive the funding
    // transaction's archive retention. Keep other transaction/script shapes on
    // the existing full-parent path. This is public-room author authorization,
    // never a replacement parent receipt or payment/UTXO ownership proof.
    if tx.version != 0 || tx.inputs.len() != 1 || !source["covenant_id"].is_null() {
        return Ok(None);
    }
    let input = &tx.inputs[0];
    if source["signature_script"].is_null()
        || source["previous_outpoint_script"].is_null()
        || source["previous_outpoint_amount"].is_null()
        || source["sig_op_count"].is_null()
    {
        return Ok(None);
    }
    ensure!(
        canonical_bytes(&source["signature_script"])? == input.signature_script,
        "public room author signature differs from canonical input"
    );
    let script = canonical_bytes(&source["previous_outpoint_script"])?;
    if script.len() != 34
        || script[0] != 0x20
        || script[33] != 0xac
        || input.signature_script.len() != 66
        || input.signature_script[0] != 0x41
        || input.signature_script[65] != 0x01
        || unsigned(&source["sig_op_count"])? != 1
        || input.compute_commit.sig_op_count() != Some(1)
    {
        return Ok(None);
    }
    let amount = unsigned(&source["previous_outpoint_amount"])?;
    ensure!(amount > 0, "public room author amount must be positive");
    // Version zero is an explicit standard P2PK signing template. Simply does
    // not retain the original prevout's script version. Executing this template
    // proves the key holder authorized the exact current payload, outpoint,
    // amount and outputs; it makes no claim about that historical version.
    let template = ScriptPublicKey::from_vec(0, script);
    let populated = PopulatedTransaction::new(
        tx,
        vec![UtxoEntry::new(amount, template.clone(), 0, false, None)],
    );
    let cache = Cache::new(0);
    let reused = SigHashReusedValuesUnsync::new();
    let ctx = EngineCtx::new(&cache).with_reused(&reused);
    TxScriptEngine::from_transaction_input(
        &populated,
        input,
        0,
        &populated.entries[0],
        ctx,
        EngineFlags::default(),
    )
    .execute()
    .context("invalid public room author signature")?;
    let prefix = match NetworkId::from_str(network)?.network_type() {
        NetworkType::Mainnet => AddressPrefix::Mainnet,
        NetworkType::Testnet => AddressPrefix::Testnet,
        _ => anyhow::bail!("unsupported configured room network"),
    };
    let author = extract_script_pub_key_address(&template, prefix)?.to_string();
    let output = tx.outputs.first().context("room output0 missing")?;
    let recipient = extract_script_pub_key_address(&output.script_public_key, prefix)
        .ok()
        .map(|value| value.to_string());
    Ok(Some(judge(Some(&author), recipient.as_deref())))
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
    let verdict = match sender_if_complete(network, id, body, &receipt) {
        Ok(Some(v)) => v,
        Ok(None) => return Ok(TipReconcile::SenderProofPending),
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
    // The installer holds the same table lease before replacing this capture.
    // No network I/O runs here; checked sender proof and canonical acceptance
    // above remain prerequisites for every room insertion.
    sqlx::query("LOCK TABLE public.kachat_broadcasts IN ROW EXCLUSIVE MODE")
        .execute(&mut **tx).await?;
    let capture_hash:Option<String>=sqlx::query_scalar("SELECT encode(sha256(convert_to(prosrc,'UTF8')),'hex') FROM pg_proc WHERE oid=to_regprocedure('social_notifications.capture_public_room()')")
        .fetch_optional(&mut **tx).await?;
    if let Some(hash)=&capture_hash {
        ensure!(hash==include_str!("channel_notification_capture.sha256").trim(),
            "Public room notification capture must preserve live-only admission");
    }
    let capture_ready:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.kachat_broadcasts'::regclass AND tgname='kasanova_notification_room' AND NOT tgisinternal AND tgenabled='O' AND tgfoid=to_regprocedure('social_notifications.capture_public_room()'))")
        .fetch_one(&mut **tx).await?;
    let notify:bool=if capture_ready {
        sqlx::query_scalar("SELECT NOT a.notification_consumed AND a.original_time >= (SELECT since_ms FROM social_shared.decoder_epochs WHERE kind='kchat:1:room-notifications') AND EXISTS(SELECT 1 FROM protocol_indexer.events e WHERE e.transaction_id=a.transaction_id AND e.revision<=$2 AND e.notify_allowed) FROM social_shared.actions a WHERE a.transaction_id=$1 AND a.revision=$2")
            .bind(id).bind(revision).fetch_one(&mut **tx).await?
    } else {false};
    sqlx::query("SELECT set_config('kasanova.social_notify_allowed',$1,true)")
        .bind(if notify {"true"} else {"false"}).execute(&mut **tx).await?;
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
