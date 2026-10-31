//! SDK identity binding for immutable archived RPC bodies. This proves identity,
//! not acceptance, UTXO existence or independent consensus; the journal owns those.
use anyhow::{Context, Result, ensure};
use kaspa_consensus_core::tx::{ScriptPublicKey, Transaction};
use kaspa_rpc_core::RpcTransaction;
use serde_json::Value;

pub(crate) fn unsigned(value: &Value) -> Result<u64> {
    if let Some(number) = value.as_u64() {
        return Ok(number);
    }
    let text = value
        .as_str()
        .context("missing unsigned transaction integer")?;
    ensure!(
        !text.is_empty() && text.bytes().all(|v| v.is_ascii_digit()),
        "invalid transaction integer"
    );
    Ok(text.parse()?)
}

fn normalized_rpc(raw: &Value) -> Result<Value> {
    let mut value = raw.clone();
    ensure!(value.is_object(), "missing RPC transaction");
    // Verbose metadata is not part of transaction identity. Never use its
    // reported hash, address or storage mass as a substitute for SDK hashing.
    value["verboseData"] = Value::Null;
    if value["payload"].is_null() {
        value["payload"] = Value::String(String::new());
    }
    for key in ["version", "lockTime", "gas", "mass", "storageMass"] {
        if !value[key].is_null() {
            value[key] = unsigned(&value[key])?.into();
        }
    }
    for input in value["inputs"]
        .as_array_mut()
        .context("missing RPC inputs")?
    {
        ensure!(
            input.is_object() && input["previousOutpoint"].is_object(),
            "invalid RPC input/outpoint shape"
        );
        input["verboseData"] = Value::Null;
        for key in ["sequence", "sigOpCount", "computeBudget"] {
            if !input[key].is_null() {
                input[key] = unsigned(&input[key])?.into();
            }
        }
        input["previousOutpoint"]["index"] = unsigned(&input["previousOutpoint"]["index"])?.into();
    }
    for output in value["outputs"]
        .as_array_mut()
        .context("missing RPC outputs")?
    {
        ensure!(output.is_object(), "invalid RPC output shape");
        output["verboseData"] = Value::Null;
        // Explorer's legacy RPC JSON calls the SDK's `value` field `amount`.
        // Normalize that representation without tolerating conflicting claims.
        let amount = if output["amount"].is_null() {
            unsigned(&output["value"])?
        } else {
            unsigned(&output["amount"])?
        };
        if !output["value"].is_null() {
            ensure!(
                unsigned(&output["value"])? == amount,
                "RPC amount/value conflict"
            );
        }
        output["value"] = amount.into();
        let script = &output["scriptPublicKey"];
        let version = u16::try_from(unsigned(&script["version"])?)?;
        let bytes = hex::decode(
            script["scriptPublicKey"]
                .as_str()
                .context("missing RPC script")?,
        )?;
        output["scriptPublicKey"] =
            serde_json::to_value(ScriptPublicKey::from_vec(version, bytes))?;
    }
    Ok(value)
}

fn transaction(raw: &Value) -> Result<Transaction> {
    let rpc: RpcTransaction =
        serde_json::from_value(normalized_rpc(raw)?).context("invalid SDK RPC transaction")?;
    Ok(rpc.try_into()?)
}

#[derive(Debug, Clone)]
pub(crate) struct CheckedTransaction {
    transaction: Transaction,
}
impl CheckedTransaction {
    pub(crate) fn checked(raw: &Value, expected: &str) -> Result<Self> {
        let transaction = transaction(raw)?;
        ensure!(
            transaction.id().to_string() == expected,
            "RPC transaction SDK ID mismatch"
        );
        Ok(Self { transaction })
    }
    pub(crate) fn transaction(&self) -> &Transaction {
        &self.transaction
    }
}

#[cfg(test)]
pub(crate) fn fixture_transaction_id(raw: &Value) -> Result<String> {
    Ok(transaction(raw)?.id().to_string())
}
