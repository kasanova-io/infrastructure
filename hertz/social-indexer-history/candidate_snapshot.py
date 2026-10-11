#!/usr/bin/env python3
"""Freeze completed source inventories and an exact captured checkpoint, offline."""
import argparse
import base64
from collections import Counter
from contextlib import closing
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from capture_tail import validate as validate_capture
from record_evidence import merge_record
from stage import validate_records

SCOPES = {
    "content": "retained-content-and-current-profiles",
    "profiles": "retained-content-and-current-profiles",
    "relationships": "current-relationship-projection",
    "overlay": "live-content-and-vote-projection",
    "tail": "captured-live-event-tail",
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def bind_reconciliation(parent_data, fresh_proof):
    """Add exact immutable inventory identity without rewriting the prior receipt."""
    parent = json.loads(parent_data)
    states = dict(Counter(r[1] for r in fresh_proof["states"]))
    if parent["fresh_states"] != states or parent["fresh_discovered"] != len(
        fresh_proof["states"]
    ):
        raise ValueError("Parent reconciliation counts differ from fresh inventory")
    return {
        "schema": "social-history-bound-reconciliation-v1",
        "parent_receipt_sha256": sha(parent_data),
        "parent_receipt_base64": base64.b64encode(parent_data).decode(),
        "parent_receipt": parent,
        "fresh_inventory_binding": fresh_proof,
        "fresh_inventory_binding_sha256": sha(canonical(fresh_proof)),
        "source_snapshot_atomic": False,
        "graph_globally_complete": False,
    }


def encode(rows):
    return b"".join(
        (json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n").encode()
        for r in sorted(rows, key=lambda r: (r["block_time"], r["transaction_id"]))
    )


def read_inventory(root, component, network, selected=None):
    """One SQLite read transaction; every accepted immutable gzip is hash-bound."""
    root = Path(root).resolve()
    with closing(
        sqlite3.connect(f"file:{root / 'inventory.sqlite'}?mode=ro", uri=True)
    ) as db:
        db.execute("BEGIN")
        config = dict(db.execute("SELECT id,value FROM config"))
        settings = json.loads(config[1])
        if settings["network"] != network or config[3] != SCOPES[component]:
            raise ValueError("Inventory network/scope mismatch")
        jobs = db.execute(
            "SELECT id,route,params,cursor,pages,done FROM jobs ORDER BY id"
        ).fetchall()
        if component != "tail" and (
            any(not j[-1] for j in jobs)
            or (component in {"content", "profiles", "relationships"} and not jobs)
        ):
            raise ValueError("Incomplete discovery inventory")
        states = db.execute(
            "SELECT txid,status,kind,error FROM records ORDER BY txid"
        ).fetchall()
        observation_hash = hashlib.sha256()
        for observation in db.execute(
            "SELECT txid,job,data FROM observations ORDER BY txid,job"
        ):
            observation_hash.update(
                (json.dumps(observation, separators=(",", ":")) + "\n").encode()
            )
        chosen = [r for r in states if selected is None or r[0] in selected]
        if selected is not None and {r[0] for r in chosen} != set(selected):
            raise ValueError("Captured checkpoint missing from verifier inventory")
        if any(r[1] not in {"verified", "quarantined"} for r in chosen):
            raise ValueError("Pending archive verification")
        if component in {"overlay", "tail", "profiles"} and any(
            r[1] != "verified" for r in chosen
        ):
            raise ValueError("Current state has unverified exclusions")
        rows, hashes, legacy_content_scope_ids = [], {}, []
        for txid, status, kind, error in chosen:
            if status != "verified":
                continue
            path = root / "verified" / (txid + ".json.gz")
            data = path.read_bytes()
            row = json.loads(gzip.decompress(data))
            legacy_content = (
                component == "content"
                and "recovery_scope" not in row
                and kind in {"post", "quote", "reply", "broadcast"}
            )
            if (
                row["transaction_id"] != txid
                or row["kind"] != kind
                or (
                    row.get("recovery_scope") != SCOPES[component]
                    and not legacy_content
                )
            ):
                raise ValueError("Prepared record disagrees with inventory")
            if legacy_content:
                legacy_content_scope_ids.append(txid)
            if component == "profiles" and kind != "broadcast":
                raise ValueError("Fresh profile inventory contains non-profile record")
            validate_records(encode([row]), network, True, True, True)
            hashes[txid] = sha(data)
            rows.append(row)
        proof = {
            "root": str(root),
            "configuration": settings,
            "bindings": config,
            "jobs": jobs,
            "states": chosen,
            "verified_gzip_sha256": hashes,
            "legacy_content_scope_absent_ids": legacy_content_scope_ids,
            "observation_rows_sha256": observation_hash.hexdigest(),
            "source_page_sha256": {
                p.name: sha(p.read_bytes())
                for p in sorted((root / "pages").glob("*.json.gz"))
            },
            "outside_selected_checkpoint_records": len(states) - len(chosen),
            "quarantined": [
                {"id": r[0], "kind": r[2], "reason": r[3]}
                for r in chosen
                if r[1] == "quarantined"
            ],
        }
    return rows, proof


def read_capture(root, network):
    root = Path(root).resolve()
    manifest_data = (root / "manifest.json").read_bytes()
    manifest = json.loads(manifest_data)
    config = manifest["configuration"]
    if (
        config["network"] != network
        or config["max_seconds"] > 21600
        or config["max_bytes"] > 200 * 1024**2
        or config["interval_seconds"] != 300
    ):
        raise ValueError("Capture configuration outside reviewed bounds")
    with closing(
        sqlite3.connect(f"file:{root / 'inventory.sqlite'}?mode=ro", uri=True)
    ) as db:
        db.execute("BEGIN")
        checkpoints = db.execute(
            "SELECT ordinal,latest FROM snapshots ORDER BY ordinal"
        ).fetchall()
        captured = {
            txid: (payload, stamp)
            for txid, payload, stamp in db.execute(
                "SELECT txid,payload,block_time FROM records"
            )
        }
        if not checkpoints:
            raise ValueError("No durable captured checkpoint")
        union, receipts, previous = {}, [], None
        for expected, (ordinal, latest) in enumerate(checkpoints, 1):
            if ordinal != expected:
                raise ValueError("Capture checkpoint ordinal gap")
            data = (root / f"snapshot-{ordinal:04d}.json.gz").read_bytes()
            snapshot = json.loads(gzip.decompress(data))
            continuity = validate_capture(snapshot, network, previous)
            if (
                continuity["retention_overlap"] is False
                or snapshot["continuity"] != continuity
                or snapshot["ordinal"] != ordinal
                or snapshot["raw_latest"] != latest
            ):
                raise ValueError("Capture continuity/provenance mismatch")
            for row in snapshot["records"]:
                txid = row["transaction_id"]
                observation = (row["payload"], row["block_time"])
                if txid in union and union[txid] != observation:
                    raise ValueError("Captured original payload/time changed")
                union[txid] = observation
            receipts.append(
                {
                    "ordinal": ordinal,
                    "sha256": sha(data),
                    "oldest": snapshot["raw_oldest"],
                    "latest": latest,
                }
            )
            previous = latest
        if union != captured:
            raise ValueError("Captured checkpoint ledger differs from snapshot union")
    return captured, {
        "root": str(root),
        "manifest": manifest,
        "manifest_sha256": sha(manifest_data),
        "snapshots": receipts,
        "pre_capture_gap_unresolved": True,
        "complete_chain_tail_proven": False,
    }


def assemble(paths, network):
    captured, capture_proof = read_capture(paths["capture"], network)
    components, proofs = {}, {}
    for name in SCOPES:
        components[name], proofs[name] = read_inventory(
            paths[name], name, network, set(captured) if name == "tail" else None
        )
    settings = proofs["content"]["configuration"]
    if any(p["configuration"] != settings for p in proofs.values()):
        raise ValueError("Candidate components use different source/archive/network")
    marker = json.loads(proofs["tail"]["bindings"][2])
    if (
        marker["capture"] != capture_proof["root"]
        or marker["manifest"] != capture_proof["manifest"]
    ):
        raise ValueError("Tail verifier is bound to another capture")
    for row in components["tail"]:
        if captured[row["transaction_id"]] != (row["payload"], row["block_time"]):
            raise ValueError("Archive record differs from original captured event")
        if row["kind"] in {"follow", "block"}:
            raise ValueError(
                "Relationship tail requires explicit overlap reconciliation"
            )
    fresh_profiles = {}
    for row in components["profiles"]:
        owner = bytes.fromhex(row["payload"]).decode().split(":")[3]
        if owner in fresh_profiles:
            raise ValueError("Multiple current profile records for one owner")
        fresh_profiles[owner] = row
    historical_profile_ids = {}
    for row in components["content"]:
        if row["kind"] != "broadcast":
            continue
        owner = bytes.fromhex(row["payload"]).decode().split(":")[3]
        fresh = fresh_profiles.get(owner)
        if fresh is None:
            raise ValueError("Historical profile owner missing from fresh projection")
        if (
            row["transaction_id"] != fresh["transaction_id"]
            and row["block_time"] >= fresh["block_time"]
        ):
            raise ValueError("Fresh profile does not chronologically supersede history")
        historical_profile_ids.setdefault(owner, []).append(row["transaction_id"])
    records, duplicates = {}, {}
    for name, rows in components.items():
        duplicates[name] = []
        for row in rows:
            if name in {"overlay", "tail"} and "live_timestamp_provenance" not in row:
                raise ValueError("Missing original live timestamp proof")
            if merge_record(records, row):
                duplicates[name].append(row["transaction_id"])
    # Exact source rows are retained, never silently discard superseded broadcasts.
    profile_times, pairs = {}, set()
    for row in records.values():
        fields = bytes.fromhex(row["payload"]).decode().split(":")
        if row["kind"] == "broadcast":
            key = (bytes.fromhex(fields[3]), row["block_time"])
            if key in profile_times:
                raise ValueError("Equal-time profiles need explicit ordering")
            profile_times[key] = row["transaction_id"]
        if row["kind"] in {"follow", "block"}:
            key = (row["kind"], bytes.fromhex(fields[3]), bytes.fromhex(fields[6]))
            if key in pairs:
                raise ValueError("Ambiguous active relationship projection")
            pairs.add(key)
    data = encode(records.values())
    validate_records(data, network, True, True, True)
    original = {r["transaction_id"] for r in components["content"]}
    if not original.issubset(records):
        raise ValueError("Original accepted content coverage lost")
    reconciliation_data = Path(paths["reconciliation"]).read_bytes()
    reconciliation = json.loads(reconciliation_data)
    graph_proof = proofs["relationships"]
    try:
        parent_data = base64.b64decode(
            reconciliation["parent_receipt_base64"], validate=True
        )
        if (
            sha(parent_data) != reconciliation["parent_receipt_sha256"]
            or json.loads(parent_data) != reconciliation["parent_receipt"]
        ):
            raise ValueError("Parent reconciliation bytes/hash/content mismatch")
        expected_reconciliation = bind_reconciliation(parent_data, graph_proof)
    except (KeyError, TypeError, UnicodeError) as exc:
        raise ValueError("Malformed bound parent reconciliation") from exc
    if canonical(reconciliation) != canonical(expected_reconciliation):
        raise ValueError("Fresh graph reconciliation identity does not match inventory")
    proof = {
        "network": network,
        "candidate_sha256": sha(data),
        "candidate_records": len(records),
        "kinds": dict(Counter(r["kind"] for r in records.values())),
        "components": proofs,
        "duplicates": duplicates,
        "captured_checkpoint": capture_proof,
        "relationship_reconciliation": reconciliation,
        "relationship_reconciliation_sha256": sha(reconciliation_data),
        "source_snapshot_atomic": False,
        "graph_globally_complete": False,
        "original_accepted_content_included": len(original),
        "fresh_profile_ids": {
            owner: row["transaction_id"] for owner, row in fresh_profiles.items()
        },
        "historical_profile_ids_retained": historical_profile_ids,
        "native_combined_replay_verified": False,
        "live_imported": 0,
    }
    return data, proof


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in [*SCOPES, "capture", "reconciliation", "output"]:
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--network", choices=["mainnet", "testnet-10"], required=True)
    args = p.parse_args()
    data, proof = assemble(vars(args), args.network)
    encoded_proof = (json.dumps(proof, indent=2) + "\n").encode()
    if shutil.disk_usage(args.output.parent).free < 2 * 1024**3 + len(data) + len(
        encoded_proof
    ):
        raise ValueError("Candidate would violate2GiB free-space guard")
    args.output.mkdir(exist_ok=False)
    (args.output / "batch.jsonl").write_bytes(data)
    (args.output / "manifest.json").write_bytes(encoded_proof)
    print(
        json.dumps(
            {
                k: proof[k]
                for k in [
                    "candidate_sha256",
                    "candidate_records",
                    "native_combined_replay_verified",
                    "live_imported",
                ]
            }
        )
    )
