"""Preserve an observed live timestamp only with exact containing-block proof."""

from history import load, atomic_json, HEX64


def preserve_observed_time(history, txid, observed_time):
    if not isinstance(observed_time, int) or observed_time < 0:
        raise ValueError("Invalid observed live block time")
    path = history.root / "verified" / (txid + ".json.gz")
    prepared = load(path)
    tx = load(history.root / "archive" / (txid + ".json.gz"))
    if (
        tx.get("transaction_id") != txid
        or tx.get("payload") != prepared["payload"]
        or tx.get("is_accepted") is not True
    ):
        raise ValueError("Live timestamp archive transaction mismatch")
    matched = None
    for block_hash in sorted(tx.get("block_hash", [])):
        if not HEX64.fullmatch(block_hash):
            raise ValueError("Invalid archived containing-block identity")
        block = history.cache(
            "blocks",
            block_hash + "-full",
            history.archive + "/blocks/" + block_hash + "?includeTransactions=true",
        )
        if block.get("verboseData", {}).get("hash") != block_hash:
            raise ValueError("Live timestamp containing block hash mismatch")
        if int(block["header"]["timestamp"]) == observed_time and any(
            item.get("verboseData", {}).get("transactionId") == txid
            and item.get("payload") == prepared["payload"]
            for item in block.get("transactions", [])
        ):
            matched = block_hash
            break
    if matched is None:
        raise ValueError(
            "Observed live timestamp has no exact archived containing-block proof"
        )
    prepared["live_timestamp_provenance"] = {
        "observed_block_time": observed_time,
        "matched_containing_block": matched,
        "canonical_archive_block_time": prepared["block_time"],
        "canonical_archive_containing_block": prepared["containing_block"],
        "policy": "preserve-exact-live-time-after-block-membership-verification",
    }
    prepared["block_time"] = observed_time
    prepared["containing_block"] = matched
    atomic_json(path, prepared)
