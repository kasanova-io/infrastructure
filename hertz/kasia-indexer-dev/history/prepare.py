#!/usr/bin/env python3
"""Validate archived transactions and prepare native history-import batches."""
import argparse
import concurrent.futures
import gzip
import json
import os
from pathlib import Path
from collections import Counter, defaultdict
from collect import atomic_json, request


def decode(payload):
    raw = bytes.fromhex(payload)
    root, rest = raw.split(b":", 1)
    if root not in (b"ciph_msg", b"kchat"):
        raise ValueError("not a chat protocol")
    if not rest.startswith(b"1:"):
        return root.decode(), "handshake", rest, {}
    kind, body = rest[2:].split(b":", 1)
    if kind == b"handshake":
        return root.decode(), "handshake", body, {}
    if kind == b"comm":
        alias, sealed = body.split(b":", 1)
        return root.decode(), "comm", sealed, {"alias": alias[:16].hex(), "wire_alias": alias.hex()}
    if kind in (b"pay", b"payment"):
        return root.decode(), "payment", body, {}
    if kind == b"self_stash":
        scope, delimiter, sealed = body.partition(b":")
        if not delimiter:
            scope, sealed = b"", body
        return root.decode(), "self_stash", sealed, {"scope": scope.hex()}
    if kind == b"gcomm":
        return root.decode(), "gcomm", body, {"blinded_group_id": body.split(b":", 1)[0].decode()}
    if kind == b"gctl":
        recipient, delimiter, sealed = body.partition(b":")
        if not delimiter:
            recipient, sealed = b"", body
        return root.decode(), "gctl", sealed, {"recipient_pubkey": recipient.decode()}
    return root.decode(), "non_chat", b"", {}


def load(path):
    with gzip.open(path, "rt") as stream:
        return json.load(stream)


def cached(root, category, key, url):
    directory = root / category
    directory.mkdir(exist_ok=True)
    path = directory / (key + ".json.gz")
    if path.exists():
        return load(path)
    value, _ = request(url)
    atomic_json(path, value)
    return value


def prepare_one(root, tx, observations):
    txid = tx["transaction_id"]
    target = root / "prepared" / (txid + ".json.gz")
    prefix, kind, sealed, fields = decode(tx["payload"])
    if kind == "non_chat":
        return "non_chat"
    # Compare the encrypted bytes independently reported by both source indexers.
    for observation in observations:
        if bytes.fromhex(observation["row"].get("message_payload", observation["row"].get("message", observation["row"].get("stashed_data")))) != sealed:
            raise ValueError("source ciphertext differs from archival transaction " + txid)
    if target.exists():
        return "cached"
    if tx.get("is_accepted") is not True:
        raise ValueError("source transaction not confirmed accepted " + txid)
    inputs = sorted(tx.get("inputs", []), key=lambda row: row["index"])
    outputs = sorted(tx.get("outputs", []), key=lambda row: row["index"])
    sender = inputs[0].get("previous_outpoint_address") if inputs else None
    if not sender:
        detail = cached(root, "transactions", txid, "https://api.kaspa.org/transactions/" + txid + "?resolve_previous_outpoints=light")
        assert detail["payload"] == tx["payload"], "archive payload changed"
        sender = sorted(detail["inputs"], key=lambda row: row["index"])[0].get("previous_outpoint_address")
    if not sender or not sender.startswith("kaspa:"):
        raise ValueError("missing mainnet sender " + txid)
    outputs = [row for row in outputs if (row.get("script_public_key_address") or "").startswith("kaspa:")]
    if not outputs:
        raise ValueError("no standard recipient " + txid)
    output = next((row for row in outputs if row["script_public_key_address"] != sender), outputs[0])
    accepting = tx.get("accepting_block_hash")
    if not accepting:
        raise ValueError("missing acceptance block " + txid)
    accepted_scores = {int(o["row"]["accepting_daa_score"]) for o in observations
                       if o["row"].get("accepting_block") == accepting and o["row"].get("accepting_daa_score") is not None}
    if len(accepted_scores) > 1:
        raise ValueError("source acceptance score conflict " + txid)
    if accepted_scores:
        daa = accepted_scores.pop()
    else:
        block = cached(root, "blocks", accepting, "https://api.kaspa.org/blocks/" + accepting + "?includeTransactions=false")
        daa = int(block["header"]["daaScore"])
        if block.get("verboseData", {}).get("hash", accepting) != accepting:
            raise ValueError("acceptance block hash mismatch")
    if not tx.get("block_hash"):
        raise ValueError("missing containing block " + txid)
    # Pin one containing block deterministically. Native read paths dedup by tx ID.
    containing = min(tx["block_hash"])
    transaction = {
        "tx_id": txid, "payload": tx["payload"], "sender": sender,
        "receiver": output["script_public_key_address"], "amount": int(output["amount"]),
        "block_hash": containing, "block_time": int(tx["block_time"]),
        "accepting_block": accepting, "accepting_daa_score": daa,
    }
    atomic_json(target, {"transaction": transaction, "wire_prefix": prefix, "kind": kind,
                         "sealed_hex": sealed.hex(), **fields})
    return "prepared"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--partial", action="store_true", help="Defer source-only transactions while address scans are still running")
    args = parser.parse_args()
    os.umask(0o077)
    root = args.directory
    (root / "prepared").mkdir(exist_ok=True)
    txs = {}
    observations = defaultdict(list)
    for path in (root / "results").glob("*.json.gz"):
        result = load(path)
        if result["job"][0] == "explorer":
            for tx in result["rows"]:
                txid = tx["transaction_id"]
                if txid in txs and txs[txid]["payload"] != tx["payload"]:
                    raise ValueError("conflicting archive payload " + txid)
                txs[txid] = tx
        else:
            for row in result["rows"]:
                observations[row["tx_id"]].append({"source": result["job"][0], "row": row})
    # Do not omit records reported by an indexer but absent from the address scan.
    missing_errors = []
    source_pending = observations.keys() - txs.keys()
    print(json.dumps({"archive_transactions":len(txs),"source_only_pending":len(source_pending),"partial":args.partial}),flush=True)
    for txid in source_pending:
        if args.partial and not (root / "transactions" / (txid + ".json.gz")).exists():
            continue
        try:
            txs[txid] = cached(root, "transactions", txid, "https://api.kaspa.org/transactions/" + txid + "?resolve_previous_outpoints=light")
        except Exception as error:
            missing_errors.append({"tx_id": txid, "error": str(error)})
    counts = Counter()
    errors = list(missing_errors)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(prepare_one, root, tx, observations[txid]): txid for txid, tx in txs.items()}
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                counts[future.result()] += 1
            except Exception as error:
                failure = {"tx_id": futures[future], "error": str(error)}
                errors.append(failure)
                with (root / "preparation-errors.jsonl").open("a") as stream:
                    stream.write(json.dumps(failure) + "\n")
            if i % 500 == 0:
                print(json.dumps({"done": i, "total": len(futures), "counts": dict(counts), "errors": len(errors)}), flush=True)
    atomic_json(root / "preparation-errors.json.gz", errors)
    summary = {"archive_transactions": len(txs), "indexer_transactions": len(observations),
               "source_observations": sum(map(len, observations.values())), "counts": dict(counts), "errors": len(errors), "source_only_deferred": len(observations.keys() - txs.keys()) if args.partial else 0, "partial": args.partial}
    root.joinpath("preparation-status.json").write_text(json.dumps(summary))
    print(json.dumps(summary), flush=True)
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
