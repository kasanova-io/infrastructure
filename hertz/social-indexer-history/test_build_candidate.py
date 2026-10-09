import hashlib
import json
import unittest
from build_candidate import combine


def encoded(rows):
    return "".join(json.dumps(r) + "\n" for r in rows).encode()


def live(row):
    return {
        **row,
        "recovery_scope": "live-content-and-vote-projection",
        "live_timestamp_provenance": {
            "policy": "preserve-exact-live-time-after-block-membership-verification",
            "observed_block_time": row["block_time"],
            "matched_containing_block": row["containing_block"],
            "canonical_archive_block_time": row["block_time"],
            "canonical_archive_containing_block": row["containing_block"],
        },
    }


class CandidateTest(unittest.TestCase):
    def setUp(self):
        raw = b"k:1:post:02" + b"1" * 64 + b":dd:aGVsbG8="
        self.row = {
            "transaction_id": "a" * 64,
            "network": "mainnet",
            "kind": "post",
            "payload": raw.hex(),
            "payload_sha256": hashlib.sha256(raw).hexdigest(),
            "block_time": 1,
            "accepting_block": "b" * 64,
            "containing_block": "c" * 64,
            "recovery_scope": "retained-content-and-current-profiles",
        }

    def test_exact_duplicates_preserve_historical_record_and_new_ids(self):
        duplicate = live(self.row)
        extra = live({**self.row, "transaction_id": "d" * 64, "block_time": 2})
        data, proof = combine(
            encoded([self.row]), encoded([duplicate, extra]), "mainnet"
        )
        result = [json.loads(l) for l in data.splitlines()]
        self.assertEqual(result, [duplicate, extra])
        self.assertEqual(proof["exact_duplicate_ids"], [self.row["transaction_id"]])
        self.assertEqual(proof["added_ids"], [extra["transaction_id"]])
        self.assertFalse(proof["native_combined_replay_verified"])

    def test_duplicate_chain_provenance_disagreement_fails(self):
        for name, value in [
            ("block_time", 2),
            ("accepting_block", "d" * 64),
            ("containing_block", "e" * 64),
        ]:
            duplicate = live({**self.row, name: value})
            with self.assertRaisesRegex(
                ValueError, "Conflicting historical/live duplicate"
            ):
                combine(encoded([self.row]), encoded([duplicate]), "mainnet")

    def test_snapshot_cannot_claim_another_scope(self):
        with self.assertRaisesRegex(ValueError, "not a frozen live projection"):
            combine(encoded([self.row]), encoded([self.row]), "mainnet")

    def test_original_live_time_wins_only_with_canonical_equivalence(self):
        duplicate = live(self.row)
        duplicate["block_time"] = 2
        duplicate["containing_block"] = "d" * 64
        duplicate["live_timestamp_provenance"].update(
            observed_block_time=2, matched_containing_block="d" * 64
        )
        data, proof = combine(encoded([self.row]), encoded([duplicate]), "mainnet")
        self.assertEqual(json.loads(data)["block_time"], 2)
        self.assertEqual(
            proof["preserved_live_timestamp_ids"], [self.row["transaction_id"]]
        )

    def test_legacy_normalized_overlay_is_rejected(self):
        duplicate = {**self.row, "recovery_scope": "live-content-and-vote-projection"}
        with self.assertRaisesRegex(ValueError, "original live timestamp proof"):
            combine(encoded([self.row]), encoded([duplicate]), "mainnet")
