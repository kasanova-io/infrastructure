import json
from pathlib import Path
import tempfile
import sqlite3
import threading
import time
import unittest
import urllib.error
from unittest.mock import patch
from history import History, ANON, CapacityPause

KEY = "02" + "1" * 64
TX = "a" * 64
BLOCK = "b" * 64
ACCEPT = "c" * 64
ROW = {"id": TX, "userPublicKey": KEY, "signature": "dd", "postContent": "aGVsbG8="}


class HistoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.h = History(self.tmp.name, "https://source", "https://archive", "mainnet")
        self.addCleanup(self.h.db.close)

    def test_network_configuration_cannot_be_reused(self):
        with self.assertRaises(ValueError):
            History(self.tmp.name, "https://other", "https://archive", "mainnet")

    def test_cursor_is_exclusive_and_resume_keeps_records(self):
        self.h.seed = lambda: None
        self.h.add_job("/get-posts-watching", {"requesterPubkey": ANON}, "posts")
        self.h.db.commit()
        pages = [
            {"posts": [ROW], "pagination": {"hasMore": True, "nextCursor": "100_1"}},
            {"posts": [], "pagination": {"hasMore": False, "nextCursor": None}},
        ]
        with patch.object(self.h, "cache", side_effect=pages):
            self.h.discover(1)
            self.h.discover(1)
        self.assertEqual(
            self.h.db.execute("SELECT count(*) FROM records").fetchone()[0], 1
        )
        self.assertEqual(
            self.h.db.execute(
                'SELECT done FROM jobs WHERE route="/get-posts-watching"'
            ).fetchone()[0],
            1,
        )

    def test_targeted_parent_reuses_checkpointed_discovery(self):
        self.h.seed = lambda: None
        self.h.add_job("/get-posts-watching", {"requesterPubkey": ANON}, "posts")
        with patch.object(
            self.h,
            "cache",
            return_value={
                "replies": [ROW],
                "pagination": {"hasMore": False, "nextCursor": None},
            },
        ):
            self.h.discover(1, [TX])
        self.assertEqual(
            self.h.db.execute("SELECT count(*) FROM records").fetchone()[0], 1
        )
        self.assertEqual(
            self.h.db.execute(
                "SELECT done FROM jobs WHERE route='/get-posts-watching'"
            ).fetchone()[0],
            0,
        )

    def test_stalled_cursor_does_not_mark_complete(self):
        self.h.seed = lambda: None
        self.h.add_job("/get-posts-watching", {}, "posts")
        self.h.db.execute('UPDATE jobs SET cursor="100_1"')
        self.h.db.commit()
        with patch.object(
            self.h,
            "cache",
            return_value={
                "posts": [ROW],
                "pagination": {"hasMore": True, "nextCursor": "100_1"},
            },
        ):
            with self.assertRaises(ValueError):
                self.h.discover(1)
        self.assertEqual(self.h.db.execute("SELECT done FROM jobs").fetchone()[0], 0)

    def archive(self, accepted=True, body="aGVsbG8="):
        payload = ("k:1:post:" + KEY + ":dd:" + body).encode().hex()
        tx = {
            "transaction_id": TX,
            "is_accepted": accepted,
            "payload": payload,
            "accepting_block_hash": ACCEPT,
            "block_hash": [BLOCK],
            "outputs": [{"script_public_key_address": "kaspa:fixture"}],
        }
        accept = {
            "verboseData": {"hash": ACCEPT},
            "header": {"daaScore": 9, "timestamp": 200},
        }
        block = {
            "verboseData": {"hash": BLOCK},
            "header": {"timestamp": 190},
            "transactions": [
                {"verboseData": {"transactionId": TX}, "payload": payload}
            ],
        }
        self.h.db.execute("INSERT INTO records(txid) VALUES (?)", (TX,))
        self.h.db.execute(
            "INSERT INTO observations VALUES (?,?,?)", (TX, "test", json.dumps(ROW))
        )
        self.h.db.commit()
        return [tx, accept, block]

    def test_verifies_raw_inclusion_and_preserves_chain_times(self):
        with patch.object(self.h, "cache", side_effect=self.archive()):
            self.h.verify(1)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "verified"
        )
        dest = Path(self.tmp.name) / "input.jsonl"
        self.assertEqual(self.h.export(dest), 1)
        result = json.loads(dest.read_text())
        self.assertEqual(result["block_time"], 190)
        self.assertEqual(result["accepting_daa_score"], 9)
        with self.assertRaises(FileExistsError):
            self.h.export(dest)

    def test_rejects_nonaccepted_without_import(self):
        with patch.object(self.h, "cache", side_effect=self.archive(False)):
            self.h.verify(1)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "quarantined"
        )
        self.assertFalse(list((Path(self.tmp.name) / "verified").iterdir()))

    def test_verifier_includes_newly_discovered_records(self):
        self.archive()
        second = "e" * 64

        def prepared(txid):
            if txid == TX:
                self.h.db.execute("INSERT INTO records(txid) VALUES (?)", (second,))
                self.h.db.commit()
            return "post"

        with patch.object(self.h, "verify_one", side_effect=prepared):
            self.h.verify(0)
        self.assertEqual(
            self.h.db.execute(
                "SELECT count(*) FROM records WHERE status='verified'"
            ).fetchone()[0],
            2,
        )

    def test_parallel_verifier_bounds_workers_budget_and_keeps_failures(self):
        ids = [f"{i:064x}" for i in range(9)]
        self.h.db.executemany(
            "INSERT INTO records(txid) VALUES (?)", [(x,) for x in ids]
        )
        self.h.db.commit()
        active, maximum, seen = 0, 0, []
        lock = threading.Lock()

        def check(txid, observations):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                seen.append(txid)
            time.sleep(0.02)
            with lock:
                active -= 1
            if txid == ids[1]:
                raise ValueError("unaccepted")
            return "post"

        with patch.object(self.h, "verify_one", side_effect=check):
            self.h.verify(7, workers=3)
        self.assertEqual(len(seen), 7)
        self.assertEqual(len(set(seen)), 7)
        self.assertEqual(maximum, 3)
        self.assertEqual(
            dict(
                self.h.db.execute("SELECT status,count(*) FROM records GROUP BY status")
            ),
            {"discovered": 2, "quarantined": 1, "verified": 6},
        )
        # Retry failed snapshot exactly once; workers never loop the quarantine.
        with patch.object(
            self.h, "verify_one", side_effect=ValueError("still invalid")
        ):
            self.h.verify(0, retry=True, selected=[ids[1]], workers=2)
        self.assertEqual(
            self.h.db.execute(
                "SELECT status FROM records WHERE txid=?", (ids[1],)
            ).fetchone()[0],
            "quarantined",
        )

    def test_parallel_read_cursor_does_not_lock_concurrent_discovery(self):
        ids = [f"{i:064x}" for i in range(3)]
        self.h.db.executemany(
            "INSERT INTO records(txid) VALUES (?)", [(x,) for x in ids[:2]]
        )
        self.h.db.commit()

        def check(txid, observations):
            connection = sqlite3.connect(self.h.root / "inventory.sqlite")
            with connection:
                connection.execute(
                    "INSERT OR IGNORE INTO records(txid) VALUES (?)", (ids[2],)
                )
            connection.close()
            return "post"

        with patch.object(self.h, "verify_one", side_effect=check):
            self.h.verify(0, workers=2)
        self.assertEqual(
            dict(
                self.h.db.execute("SELECT status,count(*) FROM records GROUP BY status")
            ),
            {"verified": 3},
        )

    def test_parallel_capacity_pause_leaves_record_pending(self):
        self.archive()
        with patch.object(self.h, "verify_one", side_effect=CapacityPause("disk")):
            with self.assertRaises(CapacityPause):
                self.h.verify(1, workers=4)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "discovered"
        )

    def test_capacity_pause_preserves_pending_record(self):
        self.archive()
        with patch.object(self.h, "cache", side_effect=CapacityPause("disk")):
            with self.assertRaises(CapacityPause):
                self.h.verify(1)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "discovered"
        )

    def test_provider_failure_pauses_without_rejecting_a_record(self):
        self.archive()
        with patch.object(
            self.h, "verify_one", side_effect=RuntimeError("request retries exhausted")
        ):
            with self.assertRaises(RuntimeError):
                self.h.verify(1)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "discovered"
        )
        self.assertFalse(list((Path(self.tmp.name) / "quarantine").iterdir()))

    def test_missing_archive_transaction_is_excluded_with_retryable_evidence(self):
        archive = self.archive()
        url = (
            self.h.archive + "/transactions/" + TX + "?resolve_previous_outpoints=light"
        )
        with patch.object(
            self.h,
            "cache",
            side_effect=urllib.error.HTTPError(url, 404, "Not Found", {}, None),
        ):
            self.h.verify(1)
        status, error = self.h.db.execute("SELECT status,error FROM records").fetchone()
        self.assertEqual(status, "quarantined")
        self.assertIn("HTTP 404", error)
        self.assertIn(url, error)
        with patch.object(self.h, "cache", side_effect=archive):
            self.h.verify(1, retry=True)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "verified"
        )

    def test_archive_server_error_still_pauses(self):
        self.archive()
        with patch.object(
            self.h,
            "cache",
            side_effect=urllib.error.HTTPError("archive", 503, "Unavailable", {}, None),
        ):
            with self.assertRaises(urllib.error.HTTPError):
                self.h.verify(1)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "discovered"
        )

    def test_preserves_native_xonly_signer_bytes(self):
        data = self.archive()
        raw = (
            bytes.fromhex(data[0]["payload"])
            .decode()
            .replace(KEY, KEY[2:])
            .encode()
            .hex()
        )
        data[0]["payload"] = raw
        data[2]["transactions"][0]["payload"] = raw
        row = dict(ROW, userPublicKey=KEY[2:])
        self.h.db.execute("UPDATE observations SET data=?", (json.dumps(row),))
        self.h.db.commit()
        with patch.object(self.h, "cache", side_effect=data):
            self.h.verify(1)
        from history import load

        saved = load(Path(self.tmp.name) / "verified" / (TX + ".json.gz"))
        self.assertEqual(saved["sender_pubkey"], KEY[2:])
        self.assertEqual(saved["payload"], raw)

    def test_rejects_archive_network_mismatch(self):
        data = self.archive()
        data[0]["outputs"][0]["script_public_key_address"] = "kaspatest:fixture"
        with patch.object(self.h, "cache", side_effect=data):
            self.h.verify(1)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "quarantined"
        )

    def test_rejects_changed_payload(self):
        with patch.object(self.h, "cache", side_effect=self.archive(body="ZXZpbA==")):
            self.h.verify(1)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "quarantined"
        )

    def test_rejects_missing_block_inclusion(self):
        data = self.archive()
        data[2]["transactions"] = []
        with patch.object(self.h, "cache", side_effect=data):
            self.h.verify(1)
        self.assertEqual(
            self.h.db.execute("SELECT status FROM records").fetchone()[0], "quarantined"
        )


if __name__ == "__main__":
    unittest.main()
