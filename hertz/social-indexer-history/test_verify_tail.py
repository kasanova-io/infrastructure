import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from history import History
from verify_tail import TailHistory


class TailVerifyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        shared = History(
            root / "shared", "https://source", "https://archive", "mainnet"
        )
        shared.db.close()
        capture = root / "capture"
        capture.mkdir()
        (capture / "manifest.json").write_text(
            json.dumps(
                {
                    "configuration": {"network": "mainnet", "max_seconds": 21600},
                    "started_at": time.time(),
                }
            )
        )
        self.raw = "k:1:follow:02" + "1" * 64 + ":dd:unfollow:03" + "2" * 64
        self.db = sqlite3.connect(capture / "inventory.sqlite")
        self.addCleanup(self.db.close)
        self.db.execute(
            "CREATE TABLE records(txid TEXT PRIMARY KEY,payload TEXT,block_time INTEGER)"
        )
        self.db.execute(
            "INSERT INTO records VALUES(?,?,?)",
            ("a" * 64, self.raw.encode().hex(), 100),
        )
        self.db.commit()
        self.h = TailHistory(
            root / "verify",
            "https://source",
            "https://archive",
            "mainnet",
            root / "shared",
            capture,
        )
        self.addCleanup(self.h.db.close)

    def test_incremental_seed_keeps_undo_and_rejects_changed_source(self):
        self.h.seed_captured()
        self.h.seed_captured()
        self.assertEqual(
            self.h.db.execute("SELECT count(*) FROM records").fetchone()[0], 1
        )
        observations = self.h.db.execute("SELECT data FROM observations").fetchall()
        self.h.verify_observations("follow", self.raw.split(":"), observations)
        with self.assertRaises(ValueError):
            self.h.verify_observations(
                "follow",
                self.raw.replace("unfollow", "follow").split(":"),
                observations,
            )
        self.db.execute("UPDATE records SET block_time=101")
        self.db.commit()
        with self.assertRaisesRegex(ValueError, "observation changed"):
            self.h.seed_captured()

    def test_missing_capture_evidence_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Missing original"):
            self.h.verify_observations("follow", self.raw.split(":"), [])
