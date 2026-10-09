import hashlib
import json
import unittest
from build_candidate import combine


def encoded(rows):
    return "".join(json.dumps(r) + "\n" for r in rows).encode()


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
        duplicate = {**self.row, "recovery_scope": "live-content-and-vote-projection"}
        extra = {**duplicate, "transaction_id": "d" * 64, "block_time": 2}
        data, proof = combine(
            encoded([self.row]), encoded([duplicate, extra]), "mainnet"
        )
        result = [json.loads(l) for l in data.splitlines()]
        self.assertEqual(result, [self.row, extra])
        self.assertEqual(proof["exact_duplicate_ids"], [self.row["transaction_id"]])
        self.assertEqual(proof["added_ids"], [extra["transaction_id"]])
        self.assertFalse(proof["native_combined_replay_verified"])

    def test_duplicate_chain_provenance_disagreement_fails(self):
        for name, value in [
            ("block_time", 2),
            ("accepting_block", "d" * 64),
            ("containing_block", "e" * 64),
        ]:
            duplicate = {
                **self.row,
                "recovery_scope": "live-content-and-vote-projection",
                name: value,
            }
            with self.assertRaisesRegex(
                ValueError, "Conflicting historical/live duplicate"
            ):
                combine(encoded([self.row]), encoded([duplicate]), "mainnet")

    def test_snapshot_cannot_claim_another_scope(self):
        with self.assertRaisesRegex(ValueError, "not a frozen live projection"):
            combine(encoded([self.row]), encoded([self.row]), "mainnet")
