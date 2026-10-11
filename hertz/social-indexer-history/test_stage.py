import copy
import hashlib
import json
import unittest
import tempfile
from pathlib import Path
from unittest.mock import Mock
from stage import Stage, validate_records


class StageTest(unittest.TestCase):
    def setUp(self):
        raw = b"k:1:post:02" + b"1" * 64 + b":dd:aGVsbG8="
        self.row = {
            "transaction_id": "a" * 64,
            "network": "mainnet",
            "kind": "post",
            "payload": raw.hex(),
            "payload_sha256": hashlib.sha256(raw).hexdigest(),
            "accepting_block": "b" * 64,
            "containing_block": "c" * 64,
            "block_time": 10,
        }

    def check(self, rows):
        return validate_records(("\n".join(map(json.dumps, rows))).encode(), "mainnet")

    def test_good_archive_record(self):
        self.assertEqual(len(self.check([self.row])), 1)

    def test_concurrent_batch_cannot_take_over(self):
        stage = Stage.__new__(Stage)
        stage.sql = Mock(side_effect=["mainnet", "", "", "another_batch"])
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "batch.jsonl"
            path.write_text(json.dumps(self.row) + "\n")
            with self.assertRaises(ValueError):
                stage.run(path, "mainnet")
        self.assertEqual(stage.sql.call_count, 4)

    def test_wrong_network(self):
        self.row["network"] = "testnet-10"
        with self.assertRaises(ValueError):
            self.check([self.row])

    def test_relationship_snapshot_not_allowed(self):
        self.row["kind"] = "follow"
        with self.assertRaises(ValueError):
            self.check([self.row])

    def test_relationship_projection_requires_scope_and_active_action(self):
        self.row["kind"] = "follow"
        raw = b"k:1:follow:02" + b"1" * 64 + b":dd:follow:03" + b"2" * 64
        self.row.update(
            payload=raw.hex(), payload_sha256=hashlib.sha256(raw).hexdigest()
        )
        with self.assertRaises(ValueError):
            validate_records(json.dumps(self.row).encode(), "mainnet", True)
        self.row["recovery_scope"] = "current-relationship-projection"
        self.assertEqual(
            len(validate_records(json.dumps(self.row).encode(), "mainnet", True)), 1
        )
        raw = raw.replace(b":dd:follow:", b":dd:unfollow:")
        self.row.update(
            payload=raw.hex(), payload_sha256=hashlib.sha256(raw).hexdigest()
        )
        with self.assertRaises(ValueError):
            validate_records(json.dumps(self.row).encode(), "mainnet", True)

    def test_payload_tamper(self):
        self.row["payload"] = "00"
        with self.assertRaises(ValueError):
            self.check([self.row])

    def test_vote_requires_explicit_live_overlay_scope_and_value(self):
        raw = b"k:1:vote:02" + b"1" * 64 + b":dd:" + b"f" * 64 + b":upvote:"
        self.row.update(
            kind="vote",
            payload=raw.hex(),
            payload_sha256=hashlib.sha256(raw).hexdigest(),
        )
        with self.assertRaises(ValueError):
            validate_records(json.dumps(self.row).encode(), "mainnet", False, True)
        self.row["recovery_scope"] = "live-content-and-vote-projection"
        with self.assertRaises(ValueError):
            self.check([self.row])
        self.assertEqual(
            len(
                validate_records(json.dumps(self.row).encode(), "mainnet", False, True)
            ),
            1,
        )
        raw = raw.replace(b"upvote", b"invalid")
        self.row.update(
            payload=raw.hex(), payload_sha256=hashlib.sha256(raw).hexdigest()
        )
        with self.assertRaises(ValueError):
            validate_records(json.dumps(self.row).encode(), "mainnet", False, True)

    def test_out_of_order_and_duplicates(self):
        second = copy.deepcopy(self.row)
        second["transaction_id"] = "d" * 64
        second["block_time"] = 9
        with self.assertRaises(ValueError):
            self.check([self.row, second])
        with self.assertRaises(ValueError):
            self.check([self.row, self.row])


if __name__ == "__main__":
    unittest.main()
