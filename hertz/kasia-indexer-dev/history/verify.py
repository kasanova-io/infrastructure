#!/usr/bin/env python3
"""Read every imported record through the same REST routes clients use."""
import argparse
from collections import defaultdict, Counter
import concurrent.futures
import json
from pathlib import Path
import urllib.parse
import urllib.request
from collect import atomic_json, request
from prepare import load, decode, cached


def query_for(row):
    tx = row["transaction"]
    kind = row["kind"]
    if kind in ("handshake", "payment"):
        stem = "handshakes" if kind == "handshake" else "payments"
        return [("/"+stem+"/by-sender", {"address": tx["sender"]}),
                ("/"+stem+"/by-receiver", {"address": tx["receiver"]})]
    if kind == "comm":
        return [("/contextual-messages/by-sender", {"address": tx["sender"], "alias": row["alias"]})]
    if kind == "self_stash":
        # Use original bytes; a scope need not be UTF-8.
        body = bytes.fromhex(tx["payload"]).split(b":self_stash:",1)[1]
        scope = body.partition(b":")[0] if b":" in body else b""
        return [("/self-stash/by-owner", {"owner":tx["sender"],"scope":scope.hex()})]
    if kind == "gcomm":
        return [("/group-messages/by-blinded-group-id", {"blinded_group_id":row["blinded_group_id"]})]
    if kind == "gctl":
        return [("/group-control/by-sender", {"sender":tx["sender"]})]
    raise ValueError("unhandled kind")


def export_block_observations(base):
    # Read-only decoding of the pinned upstream's fixed-size index keys.
    # This provides candidate hashes, never proof by itself: any alternate
    # timestamp must also be verified against a full block from the archive.
    offsets = {"handshake_by_sender": (34, -32, 141), "payment_by_sender": (34, -32, 141),
               "contextual_message_by_sender": (50, -32, 157),
               "self_stash_by_owner": (289, -32, 362),
               "group_message_by_blinded_group_id": (32, -32, 105),
               "group_control_by_sender": (34, 75, 141)}
    result = defaultdict(list)
    with urllib.request.urlopen(base + "/export", timeout=90) as response:
        for line in response:
            part, key_hex, _ = line.decode().rstrip("\n").split(" ")
            if part not in offsets:
                continue
            time_offset, id_offset, size = offsets[part]
            key = bytes.fromhex(key_hex)
            if len(key) != size:
                raise ValueError("unexpected native index key size")
            if id_offset < 0:
                id_offset += len(key)
            txid = key[id_offset:id_offset+32].hex()
            stamp = int.from_bytes(key[time_offset:time_offset+8], "big")
            block_hash = key[time_offset+8:time_offset+40].hex()
            result[txid].append((stamp, block_hash))
    return result


def containing_block_has_time(root, tx, timestamp, observations):
    txid = tx["tx_id"]
    archive = cached(root, "transactions", txid, "https://api.kaspa.org/transactions/" + txid + "?resolve_previous_outpoints=light")
    if archive["payload"] != tx["payload"]:
        raise ValueError("archival payload changed " + txid)
    for block_hash in archive.get("block_hash", []):
        block = cached(root, "blocks", block_hash, "https://api.kaspa.org/blocks/" + block_hash + "?includeTransactions=false")
        if block.get("verboseData", {}).get("hash", block_hash) != block_hash:
            raise ValueError("containing block hash mismatch")
        if int(block["header"]["timestamp"]) == int(timestamp):
            return True
    # The archive's transaction lookup need not enumerate every containing
    # block. Cross-check additional hashes observed by our live node.
    for stamp, block_hash in observations.get(txid, []):
        if stamp != int(timestamp):
            continue
        block = cached(root, "blocks-full", block_hash, "https://api.kaspa.org/blocks/" + block_hash + "?includeTransactions=true")
        if block.get("verboseData", {}).get("hash", block_hash) != block_hash:
            raise ValueError("containing block hash mismatch")
        if int(block["header"]["timestamp"]) != stamp:
            continue
        for transaction in block.get("transactions", []):
            if transaction.get("verboseData", {}).get("transactionId") == txid and transaction.get("payload") == tx["payload"]:
                return True
    return False


def verify_group(root, base, route, params, expected, observations):
    remaining = dict(expected)
    cursor = None
    block_time = 0
    pages = 0
    seen = set()
    while True:
        query = {**params, "limit": 50}
        if cursor:
            query["cursor"] = cursor
        else:
            query["block_time"] = block_time
        rows, _ = request(base + route + "?" + urllib.parse.urlencode(query))
        if not isinstance(rows,list):
            raise ValueError("non-list read response")
        pages += 1
        for row in rows:
            txid = row["tx_id"]
            # Upstream API must already deduplicate same-transaction block sightings.
            if txid in seen and int(row["block_time"]) != block_time:
                raise ValueError("unexpected duplicate transaction in pagination")
            seen.add(txid)
            if txid not in remaining:
                continue
            prepared = remaining[txid]
            tx = prepared["transaction"]
            payload = row.get("message_payload", row.get("stashed_data", row.get("message")))
            if bytes.fromhex(payload) != bytes.fromhex(prepared["sealed_hex"]):
                raise ValueError("read-back ciphertext mismatch " + txid)
            if row.get("block_time") != tx["block_time"] and not containing_block_has_time(root, tx, row["block_time"], observations):
                raise ValueError("read-back timestamp is not an archived containing-block time " + txid)
            for key in ("accepting_block","accepting_daa_score"):
                if row.get(key) != tx[key]:
                    raise ValueError("read-back " + key + " mismatch " + txid)
            owner = row.get("owner") if prepared["kind"] == "self_stash" else row.get("sender")
            if owner != tx["sender"]:
                raise ValueError("read-back sender mismatch " + txid)
            if "receiver" in row and row["receiver"] != tx["receiver"]:
                raise ValueError("read-back receiver mismatch " + txid)
            if prepared["kind"] == "payment" and row.get("amount") != tx["amount"]:
                raise ValueError("read-back payment amount mismatch " + txid)
            del remaining[txid]
        if not remaining:
            return {"records":len(expected),"pages":pages}
        if len(rows) < 50:
            raise ValueError("missing imported transactions: " + str(len(remaining)))
        if "cursor" in rows[-1]:
            next_cursor = rows[-1]["cursor"]
            if next_cursor == cursor:
                raise ValueError("opaque cursor stalled")
            cursor = next_cursor
        else:
            next_time = max(int(row["block_time"]) for row in rows)
            if next_time <= block_time:
                raise ValueError("timestamp cursor saturated")
            block_time = next_time


def main():
    p=argparse.ArgumentParser();p.add_argument("directory",type=Path)
    p.add_argument("--base",default="http://127.0.0.1:18082")
    p.add_argument("--export-base", default="http://127.0.0.1:18082")
    args=p.parse_args();root=args.directory
    observations=export_block_observations(args.export_base)
    groups=defaultdict(dict);counts=Counter();records=0
    for path in (root/"imported").glob("*.json.gz"):
        row=load(path);records+=1;counts[row["wire_prefix"]+":"+row["kind"]]+=1
        for route, params in query_for(row):
            key=(route,json.dumps(params,sort_keys=True))
            groups[key][row["transaction"]["tx_id"]]=row
    errors=[];verified=0
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures={pool.submit(verify_group,root,args.base,route,json.loads(params),rows,observations):(route,params)
                 for (route,params),rows in groups.items()}
        for future in concurrent.futures.as_completed(futures):
            try:
                verified+=future.result()["records"]
            except Exception as error:
                route,params=futures[future];errors.append({"route":route,"params":json.loads(params),"error":str(error)})
    atomic_json(root/"verification-errors.json.gz",errors)
    summary={"imported_records":records,"read_groups":len(groups),"verified_read_records":verified,
             "errors":len(errors),"counts":dict(counts),"base":args.base}
    (root/"verification-status.json").write_text(json.dumps(summary));print(json.dumps(summary),flush=True)
    return bool(errors)


if __name__=="__main__":raise SystemExit(main())
