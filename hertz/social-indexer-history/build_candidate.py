#!/usr/bin/env python3
"""Combine immutable verified batches, refusing conflicting duplicate evidence."""
import argparse
import hashlib
import json
from pathlib import Path
from stage import validate_records


def combine(history, overlay, network):
    historical = validate_records(history, network, True)
    live = validate_records(overlay, network, False, True)
    if any(r.get("recovery_scope") != "live-content-and-vote-projection" for r in live):
        raise ValueError("Overlay is not a frozen live projection")
    records = {r["transaction_id"]: r for r in historical}
    duplicate_ids = []
    added_ids = []
    for row in live:
        txid = row["transaction_id"]
        prior = records.get(txid)
        if prior is not None:
            # Only the discovery scope may differ. Acceptance, payload, signer,
            # network and both chain timestamps must all agree exactly.
            canonical = lambda r: {k: v for k, v in r.items() if k != "recovery_scope"}
            if canonical(prior) != canonical(row):
                raise ValueError("Conflicting historical/live duplicate: " + txid)
            duplicate_ids.append(txid)
        else:
            records[txid] = row
            added_ids.append(txid)
    ordered = sorted(
        records.values(), key=lambda r: (r["block_time"], r["transaction_id"])
    )
    data = "".join(
        json.dumps(r, separators=(",", ":")) + "\n" for r in ordered
    ).encode()
    validate_records(data, network, True, True)
    proof = {
        "network": network,
        "history_sha256": hashlib.sha256(history).hexdigest(),
        "overlay_sha256": hashlib.sha256(overlay).hexdigest(),
        "candidate_sha256": hashlib.sha256(data).hexdigest(),
        "historical_records": len(historical),
        "overlay_records": len(live),
        "exact_duplicate_ids": duplicate_ids,
        "added_ids": added_ids,
        "candidate_records": len(ordered),
        "native_combined_replay_verified": False,
        "live_imported": 0,
    }
    return data, proof


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--overlay", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--network", choices=["mainnet", "testnet-10"], required=True)
    args = parser.parse_args()
    data, proof = combine(
        args.history.read_bytes(), args.overlay.read_bytes(), args.network
    )
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "batch.jsonl").write_bytes(data)
    (args.output / "combination.json").write_text(json.dumps(proof, indent=2) + "\n")
    print(json.dumps(proof))
