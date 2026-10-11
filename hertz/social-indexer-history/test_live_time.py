import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import unittest
from history import atomic_json, load
from live_time import preserve_observed_time


class LiveTimeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "verified").mkdir()
        (self.root / "archive").mkdir()
        self.txid = "a" * 64
        self.first = "b" * 64
        self.other = "c" * 64
        self.payload = "6b3a313a"
        self.prepared = {
            "payload": self.payload,
            "block_time": 100,
            "containing_block": self.first,
            "accepting_time": 300,
        }
        atomic_json(self.root / "verified" / (self.txid + ".json.gz"), self.prepared)
        atomic_json(
            self.root / "archive" / (self.txid + ".json.gz"),
            {
                "transaction_id": self.txid,
                "payload": self.payload,
                "is_accepted": True,
                "block_hash": [self.first, self.other],
            },
        )
        self.blocks = [
            {
                "verboseData": {"hash": h},
                "header": {"timestamp": stamp},
                "transactions": [
                    {
                        "verboseData": {"transactionId": self.txid},
                        "payload": self.payload,
                    }
                ],
            }
            for h, stamp in [(self.first, 100), (self.other, 200)]
        ]
        self.history = SimpleNamespace(
            root=self.root,
            archive="https://archive",
            cache=Mock(side_effect=self.blocks),
        )

    def test_exact_alternate_block_preserves_live_time_and_original_proof(self):
        preserve_observed_time(self.history, self.txid, 200)
        result = load(self.root / "verified" / (self.txid + ".json.gz"))
        self.assertEqual(result["block_time"], 200)
        self.assertEqual(result["containing_block"], self.other)
        self.assertEqual(result["accepting_time"], 300)
        self.assertEqual(
            result["live_timestamp_provenance"]["canonical_archive_block_time"], 100
        )

    def test_wrong_payload_or_unmatched_time_never_changes_prepared_record(self):
        self.blocks[1]["transactions"][0]["payload"] = "00"
        with self.assertRaisesRegex(ValueError, "no exact archived"):
            preserve_observed_time(self.history, self.txid, 200)
        self.assertEqual(
            load(self.root / "verified" / (self.txid + ".json.gz")), self.prepared
        )
