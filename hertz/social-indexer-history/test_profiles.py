import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from history import History, atomic_json, CapacityPause
from profiles import Profiles

OWNER = "1" * 64
TX = "a" * 64
ROW = {
    "id": TX,
    "userPublicKey": OWNER,
    "signature": "dd",
    "timestamp": 1,
    "userNickname": "bmFtZQ==",
    "userProfileImage": None,
    "postContent": "Ymlv",
}


class ProfilesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.content = History(
            root / "content", "https://source", "https://archive", "mainnet"
        )
        self.addCleanup(self.content.db.close)
        self.h = Profiles(
            root / "fresh",
            "https://source",
            "https://archive",
            "mainnet",
            root / "content",
        )
        self.addCleanup(self.h.db.close)

    def test_existing_pagination_preserves_xonly_without_expanding_jobs(self):
        page = {"posts": [ROW], "pagination": {"hasMore": False, "nextCursor": None}}
        with patch.object(
            self.h, "request", side_effect=[{"network": "mainnet"}, {}]
        ), patch.object(self.h, "cache", return_value=page):
            self.h.discover(30)
        self.assertEqual(self.h.latest_observed(), {OWNER: TX})
        self.assertEqual(
            self.h.db.execute("SELECT route,done FROM jobs").fetchall(),
            [("/get-users", 1)],
        )
        self.assertEqual(self.h.rate_root, self.content.root.resolve())

    def test_fields_are_compared_to_actual_broadcast_payload(self):
        fields = ["k", "1", "broadcast", OWNER, "dd", "bmFtZQ==", "", "Ymlv"]
        self.h.verify_observations("broadcast", fields, [(json.dumps(ROW),)])
        for key in ["userNickname", "userProfileImage", "postContent", "signature"]:
            with self.assertRaises(ValueError):
                self.h.verify_observations(
                    "broadcast", fields, [(json.dumps({**ROW, key: "bad"}),)]
                )

    def test_reuses_only_raw_archive_and_blocks_not_prepared_verdicts(self):
        for directory in ["archive", "blocks"]:
            atomic_json(
                self.content.root / directory / (TX + ".json.gz"),
                {"original": directory},
            )
            self.assertEqual(
                self.h.cache(directory, TX, "unused"), {"original": directory}
            )
        atomic_json(
            self.content.root / "verified" / (TX + ".json.gz"), {"not": "trusted"}
        )
        with patch.object(History, "cache", return_value={"fresh": True}) as fallback:
            self.assertEqual(self.h.cache("verified", TX, "unused"), {"fresh": True})
            fallback.assert_called_once()

    def test_incomplete_or_changed_owner_projection_is_rejected(self):
        self.h.add_job("/get-users", {}, "posts")
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.h.latest_observed()
        self.h.db.execute("UPDATE jobs SET done=1")
        for txid in [TX, "b" * 64]:
            self.h.db.execute(
                "INSERT INTO observations VALUES(?,?,?)",
                (txid, "job", json.dumps({**ROW, "id": txid})),
            )
        with self.assertRaisesRegex(ValueError, "changed during"):
            self.h.latest_observed()

    def test_inventory_source_must_match(self):
        with self.assertRaisesRegex(ValueError, "source/archive/network"):
            Profiles(
                Path(self.tmp.name) / "other",
                "https://wrong",
                "https://archive",
                "mainnet",
                self.content.root,
            )

    def test_reused_archive_still_obeys_disk_guard(self):
        atomic_json(
            self.content.root / "archive" / (TX + ".json.gz"), {"accepted": True}
        )
        with patch("profiles.shutil.disk_usage") as usage:
            usage.return_value.free = 1024
            with self.assertRaises(CapacityPause):
                self.h.cache("archive", TX, "unused")
