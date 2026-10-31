"""Archive evidence for the same six scan/backfill families.

This verifies consistency of an archive's accepted transaction and full containing
block, not independent consensus or a Social signature. Keep this receipt outside
the canonical live journal until consumer reconciliation checks current acceptance.
"""

import hashlib
import re

from .routing import route

HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def require(value, reason):
    if not value:
        raise ValueError(reason)


def integer(value, maximum=2**64 - 1):
    require(not isinstance(value, bool), "boolean is not a transaction integer")
    require(isinstance(value, (int, str)), "missing transaction integer")
    if isinstance(value, str):
        require(value.isascii() and value.isdigit(), "invalid transaction integer")
    result = int(value)
    require(0 <= result <= maximum, "transaction integer out of range")
    return result


def raw_hex(value):
    require(isinstance(value, str), "missing transaction bytes")
    require(len(value) % 2 == 0 and re.fullmatch(r"[0-9a-fA-F]*", value), "invalid transaction hex")
    return bytes.fromhex(value)


def check_network(network, info):
    require(network == "mainnet" or re.fullmatch(r"testnet-[0-9]+", network), "explicit network required")
    require(info.get("networkName") == "kaspa-" + network, "archive network does not match configuration")


def verify_archive(txid, network, network_info, detail, accepting, containing):
    """Preserve all raw fields and script versions; never UTF-8 decode ciphertext."""
    check_network(network, network_info)
    require(bool(HEX64.fullmatch(txid)), "invalid transaction ID")
    require(detail.get("transaction_id") == txid and detail.get("is_accepted") is True,
            "archive transaction ID mismatch or not accepted")
    payload = raw_hex(detail.get("payload"))
    classified = route(payload)
    require(classified is not None, "unsupported protocol root/version/action")
    accepted_hash = detail.get("accepting_block_hash", "")
    require(bool(HEX64.fullmatch(accepted_hash)), "missing accepting block")
    require(accepting.get("verboseData", {}).get("hash") == accepted_hash, "accepting block mismatch")
    hashes = detail.get("block_hash")
    require(isinstance(hashes, list) and hashes and all(isinstance(h, str) and HEX64.fullmatch(h) for h in hashes),
            "invalid containing blocks")
    containing_hash = min(hashes)
    require(containing.get("verboseData", {}).get("hash") == containing_hash, "containing block mismatch")
    matches = [t for t in containing.get("transactions", [])
               if t.get("verboseData", {}).get("transactionId") == txid]
    require(len(matches) == 1, "transaction must appear exactly once in containing block")
    tx = matches[0]
    require(raw_hex(tx.get("payload")) == payload, "containing block payload mismatch")
    outputs, rpc_outputs = detail.get("outputs"), tx.get("outputs")
    inputs, rpc_inputs = detail.get("inputs"), tx.get("inputs")
    require(isinstance(outputs, list) and isinstance(rpc_outputs, list) and outputs and len(outputs) == len(rpc_outputs),
            "missing or inconsistent transaction outputs")
    require(isinstance(inputs, list) and isinstance(rpc_inputs, list) and inputs and len(inputs) == len(rpc_inputs),
            "missing or inconsistent transaction inputs")
    require(sorted(integer(o.get("index")) for o in outputs) == list(range(len(outputs))), "invalid output indexes")
    require(sorted(integer(i.get("index")) for i in inputs) == list(range(len(inputs))), "invalid input indexes")
    outputs = sorted(outputs, key=lambda o: integer(o["index"]))
    inputs = sorted(inputs, key=lambda i: integer(i["index"]))
    prefix = "kaspa:" if network == "mainnet" else "kaspatest:"
    for i, (out, rpc) in enumerate(zip(outputs, rpc_outputs)):
        require(integer(out.get("amount")) == integer(rpc.get("amount")), "output amount differs from containing block")
        script = rpc.get("scriptPublicKey") or {}
        integer(script.get("version"), 65535)
        require(raw_hex(out.get("script_public_key")) == raw_hex(script.get("scriptPublicKey")),
                "output script differs from containing block")
        address = out.get("script_public_key_address")
        rpc_address = (rpc.get("verboseData") or {}).get("scriptPublicKeyAddress")
        require(not address or address.startswith(prefix), "output address network mismatch")
        require(not rpc_address or rpc_address.startswith(prefix), "RPC output address network mismatch")
        require(not address or not rpc_address or address == rpc_address, "output address mismatch")
    for inp, rpc in zip(inputs, rpc_inputs):
        previous = rpc.get("previousOutpoint") or {}
        require(HEX64.fullmatch(inp.get("previous_outpoint_hash", "")) is not None,
                "invalid input outpoint hash")
        require(previous.get("transactionId") == inp["previous_outpoint_hash"] and
                integer(previous.get("index")) == integer(inp.get("previous_outpoint_index")), "input outpoint mismatch")
        require(raw_hex(inp.get("signature_script")) == raw_hex(rpc.get("signatureScript")), "input signature script mismatch")
        address = inp.get("previous_outpoint_address")
        require(not address or address.startswith(prefix), "input address network mismatch")
    return {
        "schema_version": 1, "network": network, "transaction_id": txid,
        "family": classified.family.value, "root": classified.root,
        "version": classified.version, "kind": classified.action,
        "payload": payload.hex(), "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "block_time": integer(containing.get("header", {}).get("timestamp")),
        "containing_block": containing_hash, "accepting_block": accepted_hash,
        "accepting_daa_score": integer(accepting.get("header", {}).get("daaScore")),
        "accepting_time": integer(accepting.get("header", {}).get("timestamp")),
        "archive_transaction": detail, "rpc_transaction": tx,
        "notify_allowed": False,
        "verification": "archive-consistency; consensus/signature/payment/current-acceptance reconciliation pending",
    }
