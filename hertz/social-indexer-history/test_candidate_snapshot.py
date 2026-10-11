import gzip
import hashlib
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from candidate_snapshot import (
    read_inventory,
    read_capture,
    assemble,
    SCOPES,
    bind_reconciliation,
)
from history import History, atomic_json
import test_build_candidate as fixtures
from capture_tail import validate


class CandidateSnapshotTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        fixture = fixtures.CandidateTest()
        fixture.setUp()
        self.row = fixture.row

    def inventory(self, name, rows):
        path = self.root / name
        cls = type("Inventory", (History,), {"scope": SCOPES[name]})
        h = cls(path, "https://source", "https://archive", "mainnet")
        with h.db:
            if name in {"content", "profiles", "relationships"}:
                h.db.execute("INSERT INTO jobs(id,done) VALUES('complete',1)")
            for row in rows:
                h.db.execute(
                    "INSERT INTO records(txid,status,kind) VALUES(?, 'verified', ?)",
                    (row["transaction_id"], row["kind"]),
                )
                atomic_json(
                    path / "verified" / (row["transaction_id"] + ".json.gz"), row
                )
        h.db.close()
        return path

    def capture(self):
        path = self.root / "capture"
        path.mkdir()
        manifest = {
            "configuration": {
                "network": "mainnet",
                "max_seconds": 21600,
                "max_bytes": 200 * 1024**2,
                "interval_seconds": 300,
            },
            "started_at": 0,
        }
        (path / "manifest.json").write_text(json.dumps(manifest))
        snapshot = {
            "network": "mainnet",
            "records": [],
            "k_count": 0,
            "payload_bytes": 0,
            "raw_oldest": 1,
            "raw_latest": 10,
            "ordinal": 1,
        }
        snapshot["continuity"] = validate(snapshot, "mainnet", None)
        atomic_json(path / "snapshot-0001.json.gz", snapshot)
        with sqlite3.connect(path / "inventory.sqlite") as db:
            db.executescript(
                "CREATE TABLE records(txid TEXT,payload TEXT,block_time INTEGER);CREATE TABLE snapshots(ordinal INTEGER,latest INTEGER);INSERT INTO snapshots VALUES(1,10);"
            )
        return path, manifest

    def test_reads_exact_accepted_inventory_and_rejects_pending_or_scope_drift(self):
        path = self.inventory("content", [self.row])
        rows, proof = read_inventory(path, "content", "mainnet")
        self.assertEqual(rows, [self.row])
        self.assertIn(self.row["transaction_id"], proof["verified_gzip_sha256"])
        with sqlite3.connect(path / "inventory.sqlite") as db:
            db.execute("UPDATE records SET status='discovered'")
        with self.assertRaisesRegex(ValueError, "Pending"):
            read_inventory(path, "content", "mainnet")
        with self.assertRaisesRegex(ValueError, "network/scope"):
            read_inventory(path, "content", "testnet-10")

    def test_prepared_row_cannot_disagree_with_durable_record(self):
        path = self.inventory("content", [self.row])
        atomic_json(
            path / "verified" / (self.row["transaction_id"] + ".json.gz"),
            {**self.row, "transaction_id": "d" * 64},
        )
        with self.assertRaisesRegex(ValueError, "disagrees"):
            read_inventory(path, "content", "mainnet")

    def test_empty_capture_retains_precapture_gap_and_rejects_snapshot_gap(self):
        path, _ = self.capture()
        rows, proof = read_capture(path, "mainnet")
        self.assertEqual(rows, {})
        self.assertTrue(proof["pre_capture_gap_unresolved"])
        self.assertFalse(proof["complete_chain_tail_proven"])
        with sqlite3.connect(path / "inventory.sqlite") as db:
            db.execute("UPDATE snapshots SET ordinal=2")
        with self.assertRaisesRegex(ValueError, "ordinal gap"):
            read_capture(path, "mainnet")

    def test_tail_pending_blocks_selected_checkpoint_but_later_ids_do_not(self):
        path = self.inventory("tail", [])
        with sqlite3.connect(path / "inventory.sqlite") as db:
            db.execute("INSERT INTO records(txid,status) VALUES('later','discovered')")
        self.assertEqual(read_inventory(path, "tail", "mainnet", set())[0], [])
        with self.assertRaisesRegex(ValueError, "missing from verifier"):
            read_inventory(path, "tail", "mainnet", {self.row["transaction_id"]})

    def assembled_paths(self):
        capture, manifest = self.capture()
        paths = {"capture": capture}
        for name in SCOPES:
            rows = [self.row] if name == "content" else []
            if name == "overlay":
                row = fixtures.live(self.row)
                row["block_time"] = 2
                row["live_timestamp_provenance"]["observed_block_time"] = 2
                rows = [row]
            paths[name] = self.inventory(name, rows)
        with sqlite3.connect(paths["tail"] / "inventory.sqlite") as db:
            db.execute(
                "INSERT INTO config VALUES(2,?)",
                (
                    json.dumps(
                        {"capture": str(capture.resolve()), "manifest": manifest}
                    ),
                ),
            )
        paths["reconciliation"] = self.root / "reconciliation.json"
        paths["reconciliation"].write_text(
            json.dumps(
                {
                    "fresh_states": {},
                    "fresh_discovered": 0,
                    "source_snapshot_atomic": False,
                }
            )
        )
        _, graph_proof = read_inventory(
            paths["relationships"], "relationships", "mainnet"
        )
        bound = bind_reconciliation(paths["reconciliation"].read_bytes(), graph_proof)
        paths["reconciliation"].write_text(json.dumps(bound))
        return paths

    def test_assembly_preserves_original_and_proven_live_time(self):
        paths = self.assembled_paths()
        data, proof = assemble(paths, "mainnet")
        self.assertEqual(json.loads(data)["block_time"], 2)
        self.assertEqual(proof["original_accepted_content_included"], 1)
        self.assertFalse(proof["native_combined_replay_verified"])
        self.assertFalse(proof["graph_globally_complete"])
        self.assertEqual(proof["live_imported"], 0)
        self.assertEqual(proof["duplicates"]["overlay"], [self.row["transaction_id"]])

    def test_conflicting_live_times_never_choose_one(self):
        from record_evidence import merge_record

        first = fixtures.live(self.row)
        second = fixtures.live(self.row)
        second["block_time"] = 2
        second["live_timestamp_provenance"]["observed_block_time"] = 2
        records = {}
        merge_record(records, first)
        with self.assertRaisesRegex(ValueError, "Conflicting original live timestamps"):
            merge_record(records, second)
        self.assertEqual(records[self.row["transaction_id"]], first)

    def test_conflicting_acceptance_never_overwrites_original(self):
        from record_evidence import merge_record

        records = {self.row["transaction_id"]: self.row}
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate archive"):
            merge_record(records, {**self.row, "accepting_block": "f" * 64})
        self.assertEqual(records[self.row["transaction_id"]], self.row)

    def test_captured_snapshot_union_must_match_ledger(self):
        path, _ = self.capture()
        with sqlite3.connect(path / "inventory.sqlite") as db:
            db.execute(
                "INSERT INTO records VALUES(?,?,?)",
                (
                    self.row["transaction_id"],
                    self.row["payload"],
                    self.row["block_time"],
                ),
            )
        with self.assertRaisesRegex(ValueError, "differs from snapshot union"):
            read_capture(path, "mainnet")

    def test_original_quarantines_are_explicit_but_current_profile_exclusions_block(
        self,
    ):
        for name in ["content", "profiles"]:
            path = self.inventory(name, [])
            with sqlite3.connect(path / "inventory.sqlite") as db:
                db.execute(
                    "INSERT INTO records(txid,status,kind,error) VALUES(?, 'quarantined','broadcast','not accepted')",
                    (self.row["transaction_id"],),
                )
            if name == "content":
                rows, proof = read_inventory(path, name, "mainnet")
                self.assertEqual(rows, [])
                self.assertEqual(proof["quarantined"][0]["reason"], "not accepted")
            else:
                with self.assertRaisesRegex(ValueError, "unverified exclusions"):
                    read_inventory(path, name, "mainnet")

    def test_incomplete_source_jobs_block_even_all_observed_records_verified(self):
        path = self.inventory("content", [self.row])
        with sqlite3.connect(path / "inventory.sqlite") as db:
            db.execute("UPDATE jobs SET done=0")
        with self.assertRaisesRegex(ValueError, "Incomplete discovery"):
            read_inventory(path, "content", "mainnet")

    def add_row(self, path, row):
        with sqlite3.connect(path / "inventory.sqlite") as db:
            db.execute(
                "INSERT INTO records(txid,status,kind) VALUES(?,'verified',?)",
                (row["transaction_id"], row["kind"]),
            )
        atomic_json(path / "verified" / (row["transaction_id"] + ".json.gz"), row)

    def profile_row(self, txid, stamp):
        raw = b"k:1:broadcast:02" + b"1" * 64 + b":dd:bmFtZQ==::Ymlv"
        return {
            **self.row,
            "transaction_id": txid,
            "kind": "broadcast",
            "block_time": stamp,
            "payload": raw.hex(),
            "payload_sha256": hashlib.sha256(raw).hexdigest(),
        }

    def test_missing_fresh_profile_cannot_reinstate_old_state(self):
        paths = self.assembled_paths()
        self.add_row(paths["content"], self.profile_row("d" * 64, 3))
        with self.assertRaisesRegex(ValueError, "missing from fresh projection"):
            assemble(paths, "mainnet")

    def test_fresh_profile_must_supersede_history_and_both_are_retained(self):
        paths = self.assembled_paths()
        self.add_row(paths["content"], self.profile_row("d" * 64, 3))
        self.add_row(paths["profiles"], self.profile_row("e" * 64, 4))
        data, proof = assemble(paths, "mainnet")
        self.assertEqual(
            [r["transaction_id"] for r in map(json.loads, data.splitlines())],
            ["a" * 64, "d" * 64, "e" * 64],
        )
        self.assertEqual(proof["original_accepted_content_included"], 2)
        atomic_json(
            paths["profiles"] / "verified" / ("e" * 64 + ".json.gz"),
            self.profile_row("e" * 64, 3),
        )
        with self.assertRaisesRegex(ValueError, "chronologically supersede"):
            assemble(paths, "mainnet")

    def add_captured(self, paths, kind):
        suffix = (
            ("a" * 64 + ":upvote:0") if kind == "vote" else ("unfollow:03" + "2" * 64)
        )
        raw = ("k:1:" + kind + ":02" + "1" * 64 + ":dd:" + suffix).encode()
        row = fixtures.live(
            {
                **self.row,
                "transaction_id": "f" * 64,
                "kind": kind,
                "block_time": 3,
                "payload": raw.hex(),
                "payload_sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
        row["recovery_scope"] = "captured-live-event-tail"
        self.add_row(paths["tail"], row)
        captured = {
            "transaction_id": row["transaction_id"],
            "payload": row["payload"],
            "block_time": 3,
        }
        path = paths["capture"] / "snapshot-0001.json.gz"
        snapshot = json.loads(gzip.decompress(path.read_bytes()))
        snapshot.update(records=[captured], k_count=1, payload_bytes=len(raw))
        atomic_json(path, snapshot)
        with sqlite3.connect(paths["capture"] / "inventory.sqlite") as db:
            db.execute(
                "INSERT INTO records VALUES(?,?,?)",
                (row["transaction_id"], row["payload"], 3),
            )
        return row

    def test_real_checkpoint_vote_shape_is_included_but_relationship_requires_review(
        self,
    ):
        paths = self.assembled_paths()
        row = self.add_captured(paths, "vote")
        data, proof = assemble(paths, "mainnet")
        self.assertEqual(
            [r["transaction_id"] for r in map(json.loads, data.splitlines())],
            ["a" * 64, "f" * 64],
        )
        self.assertEqual(proof["kinds"]["vote"], 1)
        changed = {
            **row,
            "block_time": 4,
            "live_timestamp_provenance": {
                **row["live_timestamp_provenance"],
                "observed_block_time": 4,
            },
        }
        atomic_json(paths["tail"] / "verified" / ("f" * 64 + ".json.gz"), changed)
        with self.assertRaisesRegex(ValueError, "differs from original captured"):
            assemble(paths, "mainnet")

    def test_negative_tail_not_silently_dropped_or_replayed_without_overlap_proof(self):
        paths = self.assembled_paths()
        self.add_captured(paths, "follow")
        with self.assertRaisesRegex(ValueError, "explicit overlap reconciliation"):
            assemble(paths, "mainnet")

    def test_same_graph_counts_do_not_authorize_different_ids(self):
        paths = self.assembled_paths()
        # Empty fixture graph still has a durable discovery job. Changing its
        # exact identity keeps every count unchanged but invalidates binding.
        with sqlite3.connect(paths["relationships"] / "inventory.sqlite") as db:
            db.execute("UPDATE jobs SET id='different-discovery'")
        with self.assertRaisesRegex(ValueError, "reconciliation identity"):
            assemble(paths, "mainnet")

    def test_changed_graph_observation_fails_exact_binding(self):
        paths = self.assembled_paths()
        with sqlite3.connect(paths["relationships"] / "inventory.sqlite") as db:
            db.execute(
                "INSERT INTO observations VALUES('same-counts','job','changed-source-payload')"
            )
        with self.assertRaisesRegex(ValueError, "reconciliation identity"):
            assemble(paths, "mainnet")

    def graph_paths(self):
        paths = self.assembled_paths()
        raw = ("k:1:follow:02" + "1" * 64 + ":dd:follow:03" + "2" * 64).encode()
        row = {
            **self.row,
            "transaction_id": "e" * 64,
            "kind": "follow",
            "block_time": 3,
            "payload": raw.hex(),
            "payload_sha256": hashlib.sha256(raw).hexdigest(),
            "recovery_scope": "current-relationship-projection",
        }
        self.add_row(paths["relationships"], row)
        _, proof = read_inventory(paths["relationships"], "relationships", "mainnet")
        parent = json.dumps(
            {"fresh_states": {"verified": 1}, "fresh_discovered": 1}
        ).encode()
        paths["reconciliation"].write_text(
            json.dumps(bind_reconciliation(parent, proof))
        )
        return paths, row

    def test_same_record_count_different_transaction_id_is_rejected(self):
        paths, row = self.graph_paths()
        assemble(paths, "mainnet")
        with sqlite3.connect(paths["relationships"] / "inventory.sqlite") as db:
            db.execute("UPDATE records SET txid=?", ("d" * 64,))
        atomic_json(
            paths["relationships"] / "verified" / ("d" * 64 + ".json.gz"),
            {**row, "transaction_id": "d" * 64},
        )
        with self.assertRaisesRegex(ValueError, "reconciliation identity"):
            assemble(paths, "mainnet")

    def test_changed_prepared_payload_with_valid_digest_invalidates_binding(self):
        paths, row = self.graph_paths()
        raw = bytes.fromhex(row["payload"]).replace(b":dd:", b":ee:")
        row.update(payload=raw.hex(), payload_sha256=hashlib.sha256(raw).hexdigest())
        atomic_json(paths["relationships"] / "verified" / ("e" * 64 + ".json.gz"), row)
        with self.assertRaisesRegex(ValueError, "reconciliation identity"):
            assemble(paths, "mainnet")

    def test_legacy_content_missing_scope_is_preserved_with_explicit_inventory_binding(
        self,
    ):
        original = {k: v for k, v in self.row.items() if k != "recovery_scope"}
        path = self.inventory("content", [original])
        rows, proof = read_inventory(path, "content", "mainnet")
        self.assertEqual(rows, [original])
        self.assertEqual(
            proof["legacy_content_scope_absent_ids"], [original["transaction_id"]]
        )
        with self.assertRaisesRegex(ValueError, "network/scope"):
            read_inventory(path, "relationships", "mainnet")
        overlay = self.inventory("overlay", [original])
        with self.assertRaisesRegex(ValueError, "disagrees with inventory"):
            read_inventory(overlay, "overlay", "mainnet")

    def test_parent_receipt_hash_is_actually_checked(self):
        paths = self.assembled_paths()
        value = json.loads(paths["reconciliation"].read_text())
        value["parent_receipt_sha256"] = "0" * 64
        paths["reconciliation"].write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "bytes/hash/content"):
            assemble(paths, "mainnet")

    def test_embedded_parent_must_equal_original_encoded_bytes(self):
        paths = self.assembled_paths()
        value = json.loads(paths["reconciliation"].read_text())
        value["parent_receipt"]["fresh_discovered"] = 2
        paths["reconciliation"].write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "bytes/hash/content"):
            assemble(paths, "mainnet")

    def test_valid_reencoded_parent_still_requires_exact_inventory_counts(self):
        import base64

        paths = self.assembled_paths()
        value = json.loads(paths["reconciliation"].read_text())
        parent = {**value["parent_receipt"], "fresh_discovered": 2}
        raw = json.dumps(parent).encode()
        value.update(
            parent_receipt=parent,
            parent_receipt_base64=base64.b64encode(raw).decode(),
            parent_receipt_sha256=hashlib.sha256(raw).hexdigest(),
        )
        paths["reconciliation"].write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "Parent reconciliation counts"):
            assemble(paths, "mainnet")
