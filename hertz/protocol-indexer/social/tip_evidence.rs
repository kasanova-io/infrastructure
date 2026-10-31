//! Bind full output evidence to the canonical immutable transaction. Acceptance
//! is fenced separately by the shared consumer; this is not consensus proof.
use crate::k_protocol::VerifiedTip;
use crate::tips::{TxOutput, amount_paid_to, xonly_bytes};
use anyhow::{Context, Result, ensure};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct TipOutputEvidence {
    transaction_id: String,
    payload: Vec<u8>,
    outputs: Vec<TxOutput>,
    transaction: crate::transaction_evidence::CheckedTransaction,
}

fn integer(value: &Value) -> Result<u64> {
    if let Some(number) = value.as_u64() {
        return Ok(number);
    }
    let string = value.as_str().context("missing output integer")?;
    ensure!(
        !string.is_empty() && string.bytes().all(|c| c.is_ascii_digit()),
        "invalid output integer"
    );
    Ok(string.parse()?)
}

fn hex_bytes(value: &Value) -> Result<Vec<u8>> {
    Ok(hex::decode(
        value.as_str().context("missing transaction bytes")?,
    )?)
}

fn source_bytes(value: &Value) -> Result<Vec<u8>> {
    let raw = value
        .as_str()
        .and_then(|s| s.strip_prefix("\\x"))
        .context("missing canonical bytea")?;
    Ok(hex::decode(raw)?)
}

impl TipOutputEvidence {
    pub fn checked(
        network: &str,
        transaction_id: &[u8],
        body: &Value,
        receipt: &Value,
    ) -> Result<Self> {
        let id = hex::encode(transaction_id);
        ensure!(
            transaction_id.len() == 32
                && receipt["schema_version"].as_u64() == Some(1)
                && receipt["network"].as_str() == Some(network)
                && receipt["transaction_id"].as_str() == Some(id.as_str()),
            "output receipt source/network/ID mismatch"
        );
        ensure!(
            source_bytes(&body["transaction_id"])? == transaction_id,
            "canonical output ID mismatch"
        );
        let payload = source_bytes(&body["payload"])?;
        let raw = &receipt["rpc_transaction"];
        // Simply23 omits script versions. SDK identity binds the full RPC
        // output/version to the canonical ID, beyond a reported archive ID.
        let transaction = crate::transaction_evidence::CheckedTransaction::checked(raw, &id)?;
        let detail = &receipt["archive_transaction"];
        ensure!(
            raw["verboseData"]["transactionId"].as_str() == Some(id.as_str())
                && detail["transaction_id"].as_str() == Some(id.as_str())
                && hex_bytes(&raw["payload"])? == payload
                && hex_bytes(&detail["payload"])? == payload
                && hex_bytes(&receipt["payload"])? == payload,
            "output receipt transaction differs from canonical payload/ID"
        );
        let canonical = body["outputs"]
            .as_array()
            .context("canonical outputs pending")?;
        let archive = detail["outputs"]
            .as_array()
            .context("archive outputs pending")?;
        let rpc = raw["outputs"].as_array().context("RPC outputs pending")?;
        ensure!(
            !canonical.is_empty()
                && canonical.len() == archive.len()
                && canonical.len() == rpc.len(),
            "output count mismatch"
        );
        // Ordering is explicit; duplicate/gapped/bool indexes cannot authorize
        // a different output pairing. Values may be integer or decimal text.
        let mut canonical = canonical
            .iter()
            .map(|v| Ok((integer(&v["index"])?, v)))
            .collect::<Result<Vec<_>>>()?;
        let mut archive = archive
            .iter()
            .map(|v| Ok((integer(&v["index"])?, v)))
            .collect::<Result<Vec<_>>>()?;
        canonical.sort_by_key(|v| v.0);
        archive.sort_by_key(|v| v.0);
        let mut outputs = Vec::with_capacity(rpc.len());
        for (index, ((source_index, source), (detail_index, detail))) in
            canonical.iter().zip(&archive).enumerate()
        {
            ensure!(
                *source_index == index as u64 && *detail_index == index as u64,
                "output index mismatch"
            );
            let output = &rpc[index];
            let script = &output["scriptPublicKey"];
            let amount = integer(&output["amount"])?;
            let version = u16::try_from(integer(&script["version"])?)?;
            let bytes = hex_bytes(&script["scriptPublicKey"])?;
            ensure!(
                integer(&source["amount"])? == amount
                    && integer(&detail["amount"])? == amount
                    && source_bytes(&source["script_public_key"])? == bytes
                    && hex_bytes(&detail["script_public_key"])? == bytes,
                "output evidence differs from canonical amount/script"
            );
            outputs.push((amount, version, bytes));
        }
        Ok(Self {
            transaction_id: id,
            payload,
            outputs,
            transaction,
        })
    }

    /// No caller-supplied amount or recipient: both are derived from this owned
    /// proof and the exact opaque, signature-verified tip envelope.
    pub fn payment_to_author(&self, verified: &VerifiedTip) -> Result<u64> {
        let transaction = verified.transaction();
        ensure!(
            transaction.transaction_id == self.transaction_id
                && hex::decode(
                    transaction
                        .payload
                        .as_ref()
                        .context("verified payload missing")?
                )? == self.payload,
            "tip output evidence belongs to another verified transaction"
        );
        let author = xonly_bytes(&verified.tip().author_pubkey).context("invalid author key")?;
        Ok(amount_paid_to(&self.outputs, &author))
    }

    pub(crate) fn first_output(&self) -> Option<&TxOutput> {
        self.outputs.first()
    }

    pub(crate) fn transaction(&self) -> &kaspa_consensus_core::tx::Transaction {
        self.transaction.transaction()
    }
}
