import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from history import History
from relationships import Relationships

OWNER = "02" + "1" * 64
TARGET = "03" + "2" * 64
TX = "a" * 64


class RelationshipsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.content = History(
            root / "content", "https://source", "https://archive", "mainnet"
        )
        self.addCleanup(self.content.db.close)
        self.h = Relationships(
            root / "relations",
            "https://source",
            "https://archive",
            "mainnet",
            root / "content",
        )
        self.addCleanup(self.h.db.close)

    def test_following_and_followers_preserve_original_actor(self):
        page = {
            "posts": [
                {"id": TX, "userPublicKey": TARGET, "signature": "dd", "timestamp": 100}
            ],
            "pagination": {"hasMore": False, "nextCursor": None},
        }
        with patch.object(
            self.h, "request", side_effect=[{"network": "mainnet"}, {}]
        ), patch.object(self.h, "cache", return_value=page):
            self.h.collect(3, [OWNER])
        rows = [
            json.loads(x[0]) for x in self.h.db.execute("SELECT data FROM observations")
        ]
        following = next(r for r in rows if r["source_route"] == "/get-users-following")
        followers = next(r for r in rows if r["source_route"] == "/get-users-followers")
        self.assertEqual((following["sender"], following["target"]), (OWNER, TARGET))
        self.assertEqual((followers["sender"], followers["target"]), (TARGET, OWNER))
        self.h.verify_observations(
            "follow",
            ["k", "1", "follow", OWNER, "dd", "follow", TARGET],
            [(json.dumps(following),)],
        )
        with self.assertRaises(ValueError):
            self.h.verify_observations(
                "follow",
                ["k", "1", "follow", TARGET, "dd", "follow", OWNER],
                [(json.dumps(following),)],
            )
        with self.assertRaises(ValueError):
            self.h.verify_observations(
                "follow",
                ["k", "1", "follow", OWNER, "dd", "unfollow", TARGET],
                [(json.dumps(following),)],
            )

    def test_separate_scope_and_shared_rate_directory(self):
        self.assertEqual(self.h.rate_root, self.content.root.resolve())
        with self.assertRaises(ValueError):
            Relationships(
                self.content.root,
                "https://source",
                "https://archive",
                "mainnet",
                self.content.root,
            )
        with self.assertRaises(ValueError):
            History(self.h.root, "https://source", "https://archive", "mainnet")
        self.h.owner_jobs(TARGET[2:])
        self.assertEqual(
            self.h.db.execute("SELECT count(*) FROM jobs").fetchone()[0], 0
        )
