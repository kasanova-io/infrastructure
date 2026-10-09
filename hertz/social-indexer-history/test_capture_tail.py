import copy
import unittest
from capture_tail import validate


class TailTest(unittest.TestCase):
    def setUp(self):
        self.snapshot = {
            "network": "mainnet",
            "raw_oldest": 100,
            "raw_latest": 200,
            "k_count": 1,
            "payload_bytes": 20,
            "records": [
                {
                    "transaction_id": "a" * 64,
                    "block_time": 150,
                    "payload": b"k:1:follow:key:sig:unfollow:target".hex(),
                }
            ],
        }

    def test_undo_payload_is_preserved_and_only_window_overlap_claimed(self):
        before = copy.deepcopy(self.snapshot)
        proof = validate(self.snapshot, "mainnet", 180)
        self.assertTrue(proof["retention_overlap"])
        self.assertFalse(proof["complete_chain_tail_proven"])
        self.assertTrue(proof["pre_capture_gap_unresolved"])
        self.assertEqual(self.snapshot, before)

    def test_retention_gap_or_reversal_is_explicit(self):
        self.assertFalse(validate(self.snapshot, "mainnet", 90)["retention_overlap"])
        self.assertFalse(validate(self.snapshot, "mainnet", 210)["retention_overlap"])

    def test_truncated_snapshot_and_wrong_network_fail(self):
        with self.assertRaises(ValueError):
            validate(self.snapshot, "testnet-10", 180)
        self.snapshot["k_count"] = 2
        with self.assertRaises(ValueError):
            validate(self.snapshot, "mainnet", 180)

    def test_duplicate_or_nonprotocol_payload_fail(self):
        self.snapshot["records"][0]["payload"] = b"other".hex()
        with self.assertRaises(ValueError):
            validate(self.snapshot, "mainnet", 180)
