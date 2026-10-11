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
    for row in live:
        proof = row.get("live_timestamp_provenance", {})
        if (
            proof.get("policy")
            != "preserve-exact-live-time-after-block-membership-verification"
            or proof.get("observed_block_time") != row["block_time"]
            or proof.get("matched_containing_block") != row["containing_block"]
        ):
            raise ValueError("Overlay lacks exact original live timestamp proof")
    records = {r["transaction_id"]: r for r in historical}
    duplicate_ids = []
    added_ids = []
    for row in live:
        txid = row["transaction_id"]
        prior = records.get(txid)
        if prior is not None:
            # A separately proven live timestamp can refer to another containing
            # block. Compare its original canonical archive evidence with history,
            # then retain the live display time and its exact membership proof.
            canonical = lambda r: {k: v for k, v in r.items() if k != "recovery_scope"}
            comparison = canonical(row)
            time_proof = comparison.pop("live_timestamp_provenance", None)
            if time_proof:
                if (
                    time_proof.get("policy")
                    != "preserve-exact-live-time-after-block-membership-verification"
                    or time_proof.get("observed_block_time") != row["block_time"]
                    or time_proof.get("matched_containing_block")
                    != row["containing_block"]
                ):
                    raise ValueError("Invalid preserved live timestamp provenance")
                comparison["block_time"] = time_proof["canonical_archive_block_time"]
                comparison["containing_block"] = time_proof[
                    "canonical_archive_containing_block"
                ]
            if canonical(prior) != comparison:
                raise ValueError("Conflicting historical/live duplicate: " + txid)
            if time_proof:
                records[txid] = row
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
        "preserved_live_timestamp_ids": [r["transaction_id"] for r in live],
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
