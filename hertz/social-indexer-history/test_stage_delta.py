import copy
import json
import unittest
from unittest.mock import Mock

from stage import Stage


class DeltaLineageTest(unittest.TestCase):
    def setUp(self):
        self.stage = Stage.__new__(Stage)
        self.parent = "1" * 64
        self.batch = "2" * 64
        self.records = [{"block_time": 200, "transaction_id": "a" * 64}]
        self.parent_manifest = {
            "network": "testnet-10",
            "record_count": 1,
            "start_ordinal": 1,
            "end_ordinal": 1,
            "first_block_time": 100,
            "last_block_time": 100,
            "first_transaction_id": "d" * 64,
            "last_transaction_id": "d" * 64,
            "runtime": None,
        }
        self.manifest = {
            "network": "testnet-10",
            "record_count": 1,
            "start_ordinal": 2,
            "end_ordinal": 2,
            "first_block_time": 200,
            "last_block_time": 200,
            "first_transaction_id": "a" * 64,
            "last_transaction_id": "a" * 64,
            "runtime": None,
        }
        self.latest = {
            "sha256": self.parent,
            "parent": None,
            "manifest": self.parent_manifest,
            "complete": True,
        }
        self.parent_row = {"manifest": self.parent_manifest, "complete": True}

    def responses(
        self,
        *,
        latest=None,
        existing=None,
        parent=None,
        count=1,
        last=1,
        watermark=None
    ):
        self.stage.sql = Mock(
            side_effect=[
                json.dumps(self.latest if latest is None else latest),
                json.dumps(existing) if existing else "",
                json.dumps(self.parent_row if parent is None else parent),
                json.dumps({"count": count, "first": 1, "last": last}),
                watermark if watermark is not None else "d" * 64 + ":100",
                "",
            ]
        )

    def prepare(self):
        return self.stage.prepare_lineage(
            self.records, self.batch, "testnet-10", self.parent
        )

    def test_new_delta_durably_binds_parent_count_and_watermark_before_replay(self):
        self.responses()
        self.assertEqual(self.prepare(), 1)
        query = self.stage.sql.call_args.args[0]
        self.assertIn("INSERT INTO history_lineage", query)
        self.assertIn(self.parent, query)
        self.assertNotIn("INSERT INTO transactions", query)

    def test_incomplete_parent_and_wrong_current_head_are_rejected(self):
        self.responses(parent={**self.parent_row, "complete": False})
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.prepare()
        self.responses(latest={**self.latest, "sha256": "f" * 64})
        with self.assertRaisesRegex(ValueError, "exact completed current head"):
            self.prepare()

    def test_equal_or_earlier_timestamp_never_uses_txid_as_ordering_escape(self):
        for stamp in [99, 100]:
            self.records[0]["block_time"] = stamp
            self.responses()
            with self.subTest(stamp=stamp), self.assertRaisesRegex(
                ValueError, "chronological boundary"
            ):
                self.prepare()

    def test_ordinal_gap_or_wrong_parent_watermark_rejects(self):
        self.responses(count=1, last=2)
        with self.assertRaisesRegex(ValueError, "ordinal boundary"):
            self.prepare()
        self.responses(watermark="e" * 64 + ":100")
        with self.assertRaisesRegex(ValueError, "watermark"):
            self.prepare()

    def test_pending_delta_resumes_exact_manifest_without_new_lineage(self):
        existing = {"parent": self.parent, "manifest": self.manifest, "complete": False}
        self.responses(latest={"sha256": self.batch}, existing=existing)
        self.assertEqual(self.prepare(), 1)
        self.assertFalse(
            any("INSERT" in call.args[0] for call in self.stage.sql.call_args_list)
        )
        changed = copy.deepcopy(existing)
        changed["manifest"]["record_count"] = 2
        self.responses(latest={"sha256": self.batch}, existing=changed)
        with self.assertRaisesRegex(ValueError, "Resumed batch lineage"):
            self.prepare()

    def test_completed_delta_cannot_hide_missing_ledger_rows(self):
        existing = {"parent": self.parent, "manifest": self.manifest, "complete": True}
        self.responses(latest={"sha256": self.batch}, existing=existing)
        with self.assertRaisesRegex(ValueError, "ordinal boundary"):
            self.prepare()
        self.responses(
            latest={"sha256": self.batch}, existing=existing, count=2, last=2
        )
        self.assertEqual(self.prepare(), 1)

    def test_completion_requires_every_selected_record(self):
        self.stage.sql = Mock(return_value="0")
        with self.assertRaisesRegex(ValueError, "partially replayed"):
            self.stage.complete_lineage(self.batch, self.records, 1)
        self.assertEqual(self.stage.sql.call_count, 1)

    def test_parent_runtime_identity_is_not_silently_rebound(self):
        self.responses()
        with self.assertRaisesRegex(ValueError, "runtime differs"):
            self.stage.prepare_lineage(
                self.records, self.batch, "testnet-10", self.parent, {"changed": True}
            )
