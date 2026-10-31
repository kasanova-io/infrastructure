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


def verify_transaction_evidence(txid, network, network_info, detail, accepting, containing, *, require_protocol=False, require_accepted=True):
    """Preserve all raw fields and script versions; never UTF-8 decode ciphertext."""
    check_network(network, network_info)
    require(bool(HEX64.fullmatch(txid)), "invalid transaction ID")
    require(detail.get("transaction_id") == txid and (not require_accepted or detail.get("is_accepted") is True),
            "archive transaction ID mismatch or not accepted")
    encoded_payload = detail.get("payload")
    if encoded_payload is None and not require_protocol:
        encoded_payload = ""
    payload = raw_hex(encoded_payload)
    classified = route(payload)
    require(not require_protocol or classified is not None, "unsupported protocol root/version/action")
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
    rpc_payload = tx.get("payload")
    if rpc_payload is None and not require_protocol:
        rpc_payload = ""
    require(raw_hex(rpc_payload) == payload, "containing block payload mismatch")
    outputs, rpc_outputs = detail.get("outputs"), tx.get("outputs")
    inputs, rpc_inputs = detail.get("inputs"), tx.get("inputs")
    require(isinstance(outputs, list) and isinstance(rpc_outputs, list) and outputs and len(outputs) == len(rpc_outputs),
            "missing or inconsistent transaction outputs")
    require(isinstance(inputs, list) and isinstance(rpc_inputs, list) and (inputs or not require_protocol) and len(inputs) == len(rpc_inputs),
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
        "family": classified.family.value if classified else None, "root": classified.root if classified else None,
        "version": classified.version if classified else None, "kind": classified.action if classified else None,
        "payload": payload.hex(), "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "block_time": integer(containing.get("header", {}).get("timestamp")),
        "containing_block": containing_hash, "accepting_block": accepted_hash,
        "accepting_daa_score": integer(accepting.get("header", {}).get("daaScore")),
        "accepting_time": integer(accepting.get("header", {}).get("timestamp")),
        "archive_transaction": detail, "rpc_transaction": tx,
        "notify_allowed": False,
        "verification": "archive-consistency; consensus/signature/payment/current-acceptance reconciliation pending",
    }


def verify_archive(txid, network, network_info, detail, accepting, containing):
    return verify_transaction_evidence(txid, network, network_info, detail, accepting, containing, require_protocol=True)


def input0_evidence(receipt, previous):
    """Bind sender metadata to the exact previous output, not an address claim."""
    require(receipt["network"] == previous["network"], "previous transaction network mismatch")
    inputs = sorted(receipt["archive_transaction"]["inputs"], key=lambda r: integer(r["index"]))
    first = inputs[0]
    require(first["previous_outpoint_hash"] == previous["transaction_id"], "previous transaction ID mismatch")
    index = integer(first["previous_outpoint_index"])
    outputs = previous["rpc_transaction"]["outputs"]
    require(index < len(outputs), "previous output does not exist")
    output = outputs[index]
    require(integer(output["scriptPublicKey"]["version"], 65535) == 0, "native sender script version unsupported")
    address = (output.get("verboseData") or {}).get("scriptPublicKeyAddress")
    require(bool(address), "previous output address unresolved")
    require(not first.get("previous_outpoint_address") or first["previous_outpoint_address"] == address,
            "claimed sender differs from previous output")
    return {"address": address, "script": raw_hex(output["scriptPublicKey"]["scriptPublicKey"]).hex(),
            "amount": integer(output["amount"]), "previous_transaction": previous}


def checked_receipt(receipt, txid, network, *, with_input=True):
    """Recheck stored proof structure, including optional prior-output enrichment."""
    require(isinstance(receipt, dict) and receipt.get("transaction_id") == txid and receipt.get("network") == network,
            "cached receipt identity/network mismatch")
    accepting = {"verboseData": {"hash": receipt["accepting_block"]},
                 "header": {"daaScore": receipt["accepting_daa_score"], "timestamp": receipt["accepting_time"]}}
    containing = {"verboseData": {"hash": receipt["containing_block"]}, "header": {"timestamp": receipt["block_time"]},
                  "transactions": [receipt["rpc_transaction"]]}
    checked = verify_transaction_evidence(txid, network, {"networkName": "kaspa-" + network},
        receipt["archive_transaction"], accepting, containing, require_accepted=False)
    require(checked["payload"] == receipt["payload"], "cached payload mismatch")
    if with_input and "input0" in receipt:
        first = min(checked["archive_transaction"]["inputs"], key=lambda r: integer(r["index"]))
        previous_id = first["previous_outpoint_hash"]
        previous = checked_receipt(receipt["input0"]["previous_transaction"], previous_id, network, with_input=False)
        resolved = input0_evidence(checked, previous)
        require(all(receipt["input0"][k] == resolved[k] for k in ("address", "script", "amount")), "cached sender proof mismatch")
        checked["input0"] = resolved
    return checked


def bind_output_evidence(row, receipt):
    """Bind immutable outputs to the journal; acceptance stays canonical.

    The archive receipt must already have been checked for the configured network.
    A public tip does not require an addressable funding input or the sender's key.
    Preserve full RPC script versions for the native SDK payment verifier.
    """
    body = row['source_transaction']
    require(row['transaction_id'].hex() == receipt['transaction_id'], 'output proof transaction mismatch')
    require(body['payload'] == '\\x' + receipt['payload'], 'output proof payload mismatch')
    outputs, rpc = body.get('outputs'), receipt['rpc_transaction']['outputs']
    require(isinstance(outputs, list) and outputs and len(outputs) == len(rpc), 'output proof count mismatch')
    require(sorted(integer(out.get('index')) for out in outputs) == list(range(len(outputs))),
            'output proof indexes mismatch')
    prefix = 'kaspa:' if receipt['network'] == 'mainnet' else 'kaspatest:'
    for source, full in zip(sorted(outputs, key=lambda out: integer(out['index'])), rpc):
        require(integer(source.get('amount')) == integer(full.get('amount')), 'output proof amount mismatch')
        script = full['scriptPublicKey']
        integer(script.get('version'), 65535)
        require(source.get('script_public_key') == '\\x' + raw_hex(script.get('scriptPublicKey')).hex(),
                'output proof script mismatch')
        claimed = source.get('script_public_key_address')
        address = (full.get('verboseData') or {}).get('scriptPublicKeyAddress')
        require(not claimed or (claimed if ':' in claimed else prefix + claimed).startswith(prefix),
                'source output address network mismatch')
        require(not claimed or not address or (claimed if ':' in claimed else prefix + claimed) == address,
                'output proof address mismatch')
    return receipt
