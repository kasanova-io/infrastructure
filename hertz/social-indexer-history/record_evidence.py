"""Compare original archive evidence while preserving separately proven live time."""

from stage import HEX64


def canonical_record(row):
    value = {k: v for k, v in row.items() if k != "recovery_scope"}
    proof = value.pop("live_timestamp_provenance", None)
    if proof is not None:
        if (
            proof.get("policy")
            != "preserve-exact-live-time-after-block-membership-verification"
            or proof.get("observed_block_time") != row["block_time"]
            or proof.get("matched_containing_block") != row["containing_block"]
            or not isinstance(proof.get("canonical_archive_block_time"), int)
            or not HEX64.fullmatch(proof.get("canonical_archive_containing_block", ""))
        ):
            raise ValueError("Invalid original live timestamp provenance")
        value["block_time"] = proof["canonical_archive_block_time"]
        value["containing_block"] = proof["canonical_archive_containing_block"]
    return value


def merge_record(records, row):
    """Prefer proven live time, but never choose between conflicting live observations."""
    txid = row["transaction_id"]
    prior = records.get(txid)
    current_canonical = canonical_record(row)
    if prior is not None:
        if canonical_record(prior) != current_canonical:
            raise ValueError("Conflicting duplicate archive evidence: " + txid)
        if "live_timestamp_provenance" in prior and "live_timestamp_provenance" in row:
            if prior["block_time"] != row["block_time"]:
                raise ValueError("Conflicting original live timestamps: " + txid)
    if prior is None or "live_timestamp_provenance" in row:
        records[txid] = row
    return prior is not None
