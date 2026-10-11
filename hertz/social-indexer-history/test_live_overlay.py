import json
from pathlib import Path
import tempfile
import unittest
from history import History
from live_overlay import LiveOverlay
from verify_stage import check_vote


class LiveOverlayTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        h = History(
            self.root / "shared", "https://source", "https://archive", "mainnet"
        )
        h.db.close()
        self.row = {
            "id": "a" * 64,
            "contentType": "vote",
            "userPublicKey": "02" + "1" * 64,
            "signature": "dd",
            "parentPostId": "b" * 64,
            "voteType": "downvote",
        }
        self.snapshot = self.root / "snapshot.json"
        self.write_snapshot()

    def write_snapshot(self):
        self.snapshot.write_text(
            json.dumps(
                {
                    "scope": "live-projection-read-only",
                    "network": "mainnet",
                    "records": [self.row],
                }
            )
        )

    def overlay(self):
        return LiveOverlay(
            self.root / "overlay",
            "https://source",
            "https://archive",
            "mainnet",
            self.root / "shared",
            self.snapshot,
        )

    def test_vote_target_value_and_signer_are_bound(self):
        h = self.overlay()
        self.addCleanup(h.db.close)
        fields = [
            "k",
            "1",
            "vote",
            self.row["userPublicKey"],
            "dd",
            self.row["parentPostId"],
            "downvote",
            "",
        ]
        observed = [(json.dumps(self.row),)]
        h.verify_observations("vote", fields, observed)
        for index, value in [
            (3, "03" + "1" * 64),
            (4, "aa"),
            (5, "c" * 64),
            (6, "upvote"),
        ]:
            changed = fields.copy()
            changed[index] = value
            with self.assertRaises(ValueError):
                h.verify_observations("vote", changed, observed)

    def test_snapshot_cannot_be_rebound_and_resume_deduplicates(self):
        h = self.overlay()
        h.db.close()
        h = self.overlay()
        self.assertEqual(h.db.execute("SELECT count(*) FROM records").fetchone()[0], 1)
        h.db.close()
        self.row["voteType"] = "upvote"
        self.write_snapshot()
        with self.assertRaisesRegex(ValueError, "another frozen snapshot"):
            self.overlay()

    def test_shared_rate_network_mismatch_is_rejected(self):
        with self.assertRaisesRegex(
            ValueError, "Shared archive source/network mismatch"
        ):
            LiveOverlay(
                self.root / "overlay",
                "https://wrong",
                "https://archive",
                "mainnet",
                self.root / "shared",
                self.snapshot,
            )

    def test_observation_is_required(self):
        h = self.overlay()
        self.addCleanup(h.db.close)
        with self.assertRaisesRegex(ValueError, "original database observation"):
            h.verify_observations("vote", [], [])

    def test_exact_native_vote_readback(self):
        row = {
            "transaction_id": self.row["id"],
            "sender_pubkey": self.row["userPublicKey"],
            "payload": (
                "k:1:vote:"
                + self.row["userPublicKey"]
                + ":dd:"
                + self.row["parentPostId"]
                + ":downvote:"
            )
            .encode()
            .hex(),
        }
        value = {
            k: self.row[k] for k in ["id", "userPublicKey", "parentPostId", "voteType"]
        }
        check_vote(row, value)
        for key in value:
            altered = {**value, key: "wrong"}
            with self.assertRaises(ValueError):
                check_vote(row, altered)
