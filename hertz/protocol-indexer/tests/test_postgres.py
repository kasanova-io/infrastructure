"""Run against a disposable database only; exercises real transactions and triggers."""

import copy
import hashlib
import os
from pathlib import Path
import unittest
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from protocol_indexer.cli import validate_binding, validate_source
from protocol_indexer.source import SIMPLY_IMAGE_REF
from protocol_indexer.journal import (
    SIMPLY_REVISION, BackfillAdapter, ScanAdapter, capture_one, connect, discard_unrelated, install, prune_source,
)
from protocol_indexer.routing import Family

DSN = os.environ.get("PROTOCOL_TEST_DATABASE_URL")


@unittest.skipUnless(DSN, "set PROTOCOL_TEST_DATABASE_URL to an empty disposable database")
class PostgresHarness(unittest.TestCase):
    def setUp(self):
        self.db = connect(DSN)
        # The test harness owns this exact disposable database, not a caller's
        # production schema. Require an explicit test name before any DDL.
        if not self.db.info.dbname.startswith("protocol_test_"):
            self.db.close()
            raise ValueError("integration tests require a protocol_test_* database")
        self.db.execute("DROP SCHEMA IF EXISTS protocol_indexer CASCADE")
        self.db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        self.db.execute(Path(__file__).with_name("simply-schema-23.sql").read_text())
        install(self.db, "testnet-10", activated_at_ms=1000)
        self.count = 0

    def tearDown(self):
        self.db.close()

    def txid(self):
        self.count += 1
        return hashlib.sha256(str(self.count).encode()).digest()

    def body(self, txid, payload, timestamp=2000):
        self.db.execute("""
            INSERT INTO transactions (transaction_id, payload, block_time, block_hash, inputs, outputs)
            VALUES (%s, %s, %s, %s,
                ARRAY[ROW(0, %s, 0, %s, 1, %s, 30000000, NULL, NULL)::transactions_inputs],
                ARRAY[ROW(0, 10000000, %s, 'kaspatest:fixture', NULL, NULL)::transactions_outputs])
        """, (txid, payload, timestamp, b"b" * 32, b"p" * 32, b"sig", b"sender-script", b"author-script"))

    def accept(self, txid, block=b"a" * 32):
        self.db.execute("INSERT INTO transactions_acceptances VALUES (%s, %s)", (txid, block))

    def ingest(self, payload=b"kchat:1:post:key:sig:body:[]", timestamp=2000):
        txid = self.txid()
        self.body(txid, payload, timestamp)
        self.accept(txid)
        self.assertTrue(capture_one(self.db))
        return txid

    def scalar(self, query, params=()):
        return next(iter(self.db.execute(query, params).fetchone().values()))

class PostgresTests(PostgresHarness):
    def test_journal_epoch_is_stable_across_install_and_not_a_process_identity(self):
        before = self.scalar("SELECT journal_id FROM protocol_indexer.config")
        self.assertIsNotNone(before)
        install(self.db, "testnet-10", activated_at_ms=999999)
        self.assertEqual(self.scalar("SELECT journal_id FROM protocol_indexer.config"), before)
        self.assertEqual(self.scalar("SELECT activated_at_ms FROM protocol_indexer.config"), 1000)

    def test_old_journal_config_gains_identity_without_rewriting_captured_history(self):
        txid = self.ingest()
        before = self.db.execute("SELECT * FROM protocol_indexer.canonical WHERE transaction_id=%s", (txid,)).fetchone()
        self.db.execute("ALTER TABLE protocol_indexer.config DROP COLUMN journal_id")
        install(self.db, "testnet-10", activated_at_ms=999999)
        self.assertIsNotNone(self.scalar("SELECT journal_id FROM protocol_indexer.config"))
        self.assertEqual(self.scalar("SELECT activated_at_ms FROM protocol_indexer.config"), 1000)
        self.assertEqual(self.db.execute("SELECT * FROM protocol_indexer.canonical WHERE transaction_id=%s", (txid,)).fetchone(), before)

    def test_all_six_scan_and_backfill_adapters_preserve_exact_body(self):
        cases = {
            Family.KACHAT_POSTS: b"kchat:1:post:key:sig:4oGgaGk=:[]",
            Family.KACHAT_MESSAGES: b"kchat:1:handshake:\xff\x00:binary\xfe",
            Family.KACHAT_ACTIONS: b"kchat:1:tip:key:sig:post:author:0102",
            Family.KASIA_MESSAGES: b"ciph_msg:1:comm:alias:0102",
            Family.KSOCIAL_POSTS: b"k:1:reply:key:sig:post:aGk=:[]",
            Family.KSOCIAL_ACTIONS: b"k:1:follow:key:sig:follow:author",
        }
        for family, payload in cases.items():
            txid = self.ingest(payload)
            adapter = ScanAdapter(family)
            self.assertIsNotNone(adapter.run_one(self.db))
            before = self.db.execute("SELECT * FROM protocol_indexer.records WHERE transaction_id = %s", (txid,)).fetchone()
            self.assertEqual(before["family"], family.value)
            self.assertEqual(bytes.fromhex(before["source_transaction"]["payload"][2:]), payload)
            self.assertEqual(before["source_transaction"]["outputs"][0]["amount"], 10000000)
            self.assertEqual(before["source_transaction"]["inputs"][0]["previous_outpoint_amount"], 30000000)
            self.db.execute("DELETE FROM protocol_indexer.records WHERE transaction_id = %s", (txid,))
            backfill = BackfillAdapter(family)
            cursor = b""
            while (cursor := backfill.run_page(self.db, cursor, limit=2)) is not None:
                pass
            after = self.db.execute("SELECT * FROM protocol_indexer.records WHERE transaction_id = %s", (txid,)).fetchone()
            self.assertEqual({k: v for k, v in before.items() if k != "mode"},
                             {k: v for k, v in after.items() if k != "mode"})
            self.assertEqual(after["mode"], "backfill")
            self.assertIsNone(adapter.run_one(self.db))

    def test_body_and_acceptance_can_arrive_in_either_order(self):
        one = self.txid()
        self.body(one, b"k:1:post:a:b:c")
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT kind FROM protocol_indexer.events"), "observed")
        self.accept(one)
        self.assertTrue(capture_one(self.db))
        two = self.txid()
        self.accept(two)
        self.assertFalse(capture_one(self.db))
        self.body(two, b"ciph_msg:1:comm:a:b")
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.canonical WHERE accepting_block IS NOT NULL"), 2)

    def test_acceptance_insert_before_body_commit_cannot_lose_its_wakeup(self):
        txid = self.txid()
        with connect(DSN) as acceptance_writer:
            with acceptance_writer.transaction():
                # Body is absent from the acceptance trigger's snapshot. The
                # trigger must nevertheless enqueue it in that same transaction.
                acceptance_writer.execute("INSERT INTO transactions_acceptances VALUES (%s, %s)", (txid, b"a" * 32))
                # Inserting the body now would correctly block on the pending
                # row until acceptance commits. A separate raw body writer can
                # only commit after the durable wakeup, so it cannot be lost.
                self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.pending"), 0)
            self.body(txid, b"k:1:post:a:b:c")
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT kind FROM protocol_indexer.events"), "accepted")

    def test_unknown_body_wakeup_is_discarded_for_non_protocol_transaction(self):
        txid = self.txid()
        self.accept(txid)
        self.body(txid, b"unrelated")
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.canonical"), 0)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.pending"), 0)

    def test_missing_body_does_not_delay_ready_protocol_transactions(self):
        missing, ready = self.txid(), self.txid()
        self.accept(missing)
        self.body(ready, b"kchat:1:tip:a:b:c")
        self.accept(ready)
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT transaction_id FROM protocol_indexer.canonical"), ready)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.pending"), 1)
        self.body(missing, b"ciph_msg:1:comm:a:b")
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.canonical WHERE accepting_block IS NOT NULL"), 2)

    def test_acceptance_during_uncommitted_body_cannot_leave_only_observed_event(self):
        txid = self.txid()
        inserted, release = threading.Event(), threading.Event()
        with connect(DSN) as acceptance_writer, connect(DSN) as observer:
            def insert():
                with acceptance_writer.transaction():
                    acceptance_writer.execute("INSERT INTO transactions_acceptances VALUES (%s, %s)", (txid, b"a" * 32))
                    inserted.set()
                    if not release.wait(5):
                        raise RuntimeError("test acceptance writer timed out")
            with ThreadPoolExecutor(max_workers=1) as pool:
                try:
                    with self.db.transaction():
                        self.body(txid, b"k:1:post:a:b:c")
                        future = pool.submit(insert)
                        deadline = time.monotonic() + 3
                        while time.monotonic() < deadline:
                            wait = observer.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (acceptance_writer.info.backend_pid,)).fetchone()
                            if wait and wait["wait_event_type"] == "Lock":
                                break
                            if inserted.is_set():
                                self.fail("acceptance did not wait for the uncommitted durable body queue")
                            time.sleep(0.01)
                        else:
                            self.fail("acceptance writer did not reach the expected row lock")
                    self.assertTrue(inserted.wait(3))
                    # The acceptance writer still owns the durable queue row,
                    # so capture cannot prematurely acknowledge only the body.
                    self.assertFalse(capture_one(self.db))
                finally:
                    release.set()
                future.result(timeout=3)
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT kind FROM protocol_indexer.events"), "accepted")

    def test_late_commits_are_not_lost_behind_a_high_water_mark(self):
        for payload in (b"k:1:post:a:b:c", b"k:1:post:d:e:f"):
            txid = self.txid()
            self.body(txid, payload)
            self.accept(txid)
        adapter = ScanAdapter(Family.KSOCIAL_POSTS)
        with connect(DSN) as slow:
            with slow.transaction():
                self.assertTrue(capture_one(slow))
                self.assertTrue(capture_one(self.db))
                first = adapter.run_one(self.db)
                self.assertIsNotNone(first)
                self.assertIsNone(adapter.run_one(self.db))
            later = adapter.run_one(self.db)
            self.assertLess(later["event_id"], first["event_id"])
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.records"), 2)

    def test_parallel_workers_do_not_apply_two_revisions_out_of_order(self):
        txid = self.txid()
        self.body(txid, b"kchat:1:comm:a:b")
        capture_one(self.db)
        self.accept(txid)
        capture_one(self.db)
        adapter = ScanAdapter(Family.KACHAT_MESSAGES)
        with connect(DSN) as slow:
            with slow.transaction():
                self.assertEqual(adapter.run_one(slow)["revision"], 1)
                self.assertIsNone(adapter.run_one(self.db))
            self.assertEqual(adapter.run_one(self.db)["revision"], 2)

    def test_crash_rolls_back_projection_and_acknowledgement(self):
        self.ingest()
        adapter = ScanAdapter(Family.KACHAT_POSTS)
        with patch("protocol_indexer.journal.project", side_effect=RuntimeError("simulated consumer crash")):
            with self.assertRaises(RuntimeError):
                adapter.run_one(self.db)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.deliveries"), 0)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.records"), 0)
        self.assertIsNotNone(adapter.run_one(self.db))

    def test_backfill_cannot_resurrect_a_revoked_transaction(self):
        txid = self.ingest()
        old = self.db.execute("SELECT * FROM protocol_indexer.canonical").fetchone()
        adapter = ScanAdapter(Family.KACHAT_POSTS)
        adapter.run_one(self.db)
        self.db.execute("DELETE FROM transactions_acceptances WHERE transaction_id = %s", (txid,))
        capture_one(self.db)
        adapter.run_one(self.db)
        from protocol_indexer.journal import project
        project(self.db, Family.KACHAT_POSTS, old, "backfill")
        record = self.db.execute("SELECT revision, accepting_block FROM protocol_indexer.records").fetchone()
        self.assertEqual(record, {"revision": 2, "accepting_block": None})

    def test_reorg_is_detected_after_raw_body_retention(self):
        txid = self.ingest()
        adapter = ScanAdapter(Family.KACHAT_POSTS)
        adapter.run_one(self.db)
        self.assertEqual(prune_source(self.db, 3000), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM transactions"), 0)
        self.assertEqual(self.scalar("SELECT count(*) FROM transactions_acceptances"), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.pending"), 0)
        self.db.execute("DELETE FROM transactions_acceptances WHERE block_hash = %s", (b"a" * 32,))
        capture_one(self.db)
        self.assertEqual(adapter.run_one(self.db)["kind"], "revoked")
        self.assertIsNone(self.scalar("SELECT accepting_block FROM protocol_indexer.records"))
        self.accept(txid, b"z" * 32)
        capture_one(self.db)
        event = adapter.run_one(self.db)
        self.assertEqual(event["kind"], "accepted")
        self.assertFalse(event["notify_allowed"])

    def test_retention_preserves_unprocessed_protocol_bodies(self):
        txid = self.txid()
        self.body(txid, b"kchat:1:tip:a:b:c")
        self.accept(txid)
        other = self.txid()
        self.body(other, b"non-protocol-payload")
        self.accept(other)
        self.assertEqual(prune_source(self.db, 3000), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM transactions"), 1)
        capture_one(self.db)
        self.assertEqual(prune_source(self.db, 3000), 1)

    def test_null_payload_plain_transfers_are_pruned(self):
        txid = self.txid()
        self.body(txid, None)
        self.accept(txid)
        self.assertEqual(prune_source(self.db, 3000), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM transactions"), 0)
        self.assertEqual(self.scalar("SELECT count(*) FROM transactions_acceptances"), 0)

    def test_ordinary_chain_wakeups_use_bulk_cleanup_instead_of_six_adapters(self):
        self.db.execute("""INSERT INTO transactions_acceptances
            SELECT decode(lpad(to_hex(i), 64, '0'), 'hex'), decode(repeat('ab',32), 'hex')
            FROM generate_series(1, 10000) i""")
        self.db.execute("""INSERT INTO transactions (transaction_id, payload, block_time)
            SELECT decode(lpad(to_hex(i), 64, '0'), 'hex'), NULL, 2000
            FROM generate_series(1, 10000) i""")
        self.assertEqual(discard_unrelated(self.db), 5000)
        self.assertEqual(discard_unrelated(self.db), 5000)
        self.assertEqual(discard_unrelated(self.db), 0)
        self.assertFalse(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.canonical"), 0)

    def test_old_history_and_reacceptance_never_qualify_for_new_notification(self):
        self.ingest(timestamp=500)
        self.assertFalse(self.scalar("SELECT notify_allowed FROM protocol_indexer.events"))
        new = self.ingest(timestamp=2000)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed"), 1)
        self.db.execute("DELETE FROM transactions_acceptances WHERE transaction_id = %s", (new,))
        capture_one(self.db)
        self.accept(new, b"z" * 32)
        capture_one(self.db)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed"), 1)

    def test_unknown_protocol_actions_remain_retained_and_unprojected(self):
        self.ingest(b"kchat:1:future_action:\xffbinary")
        self.assertIsNone(self.scalar("SELECT family FROM protocol_indexer.events"))
        for family in Family:
            self.assertIsNone(ScanAdapter(family).run_one(self.db))
            BackfillAdapter(family).run_page(self.db)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.records"), 0)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.canonical"), 1)

    def test_conflicting_payload_rolls_back_without_acknowledging(self):
        txid = self.ingest()
        self.db.execute("UPDATE transactions SET payload = %s WHERE transaction_id = %s", (b"kchat:1:post:changed", txid))
        with self.assertRaisesRegex(ValueError, "conflicting payload"):
            capture_one(self.db)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.pending"), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.events"), 1)

    def test_network_binding_and_restart_are_idempotent(self):
        self.ingest()
        install(self.db, "testnet-10")
        self.assertFalse(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT activated_at_ms FROM protocol_indexer.config"), 1000)
        with self.assertRaisesRegex(ValueError, "another network"):
            install(self.db, "mainnet")
        self.assertEqual(self.scalar("SELECT network FROM protocol_indexer.config"), "testnet-10")

    def test_existing_source_inventory_is_seeded_after_install(self):
        self.db.execute("DROP SCHEMA protocol_indexer CASCADE")
        txid = self.txid()
        self.body(txid, b"k:1:post:a:b:c", timestamp=500)
        self.accept(txid)
        install(self.db, "mainnet", activated_at_ms=1000)
        self.assertTrue(capture_one(self.db))
        self.assertFalse(self.scalar("SELECT notify_allowed FROM protocol_indexer.events"))


class SourceContractTests(unittest.TestCase):
    def metrics(self):
        return {"commitId": SIMPLY_REVISION, "database": {"databaseName": "shared"}, "settings": {"cli_args": {
            "databaseUrl": "**hidden**",
            "network": "testnet-10", "disable": ["BlocksTable", "BlockParentTable", "AddressesTransactionsTable"],
            "excludeFields": ["TxMass"], "pruning": {"pruneDb": None}, "ignoreSelfSends": None,
        }}}

    def test_configured_mainnet_and_testnet_are_both_supported(self):
        for network in ("mainnet", "testnet-10"):
            metrics = self.metrics()
            metrics["settings"]["cli_args"]["network"] = network
            validate_source(metrics, network, "shared")

    def test_upstream_short_revision_metadata_is_bound_to_the_full_source_pin(self):
        metrics = self.metrics()
        metrics["commitId"] = SIMPLY_REVISION[:7]
        validate_source(metrics, "testnet-10", "shared")
        metrics["commitId"] = SIMPLY_REVISION[:6]
        with self.assertRaisesRegex(ValueError, "revision"):
            validate_source(metrics, "testnet-10", "shared")

    def test_unsafe_upstream_configuration_is_rejected(self):
        for field, value in (
            ("network", "mainnet"), ("disable", ["TransactionAcceptance"]),
            ("disable", ["TransactionsOutputs"]), ("excludeFields", ["TxOutAmount"]),
            ("excludeFields", ["TxPayload"]), ("ignoreSelfSends", ["kaspatest:fixture"]),
            ("pruning", {"pruneDb": "0 * * * *"}),
            ("ignoreCheckpoint", "v"), ("initializeDb", True),
        ):
            metrics = copy.deepcopy(self.metrics())
            metrics["settings"]["cli_args"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                validate_source(metrics, "testnet-10", "shared")
        metrics = self.metrics()
        metrics["commitId"] = "other"
        with self.assertRaises(ValueError):
            validate_source(metrics, "testnet-10", "shared")

    def binding(self):
        return {"schema_version": 1, "network": "testnet-10", "source_revision": SIMPLY_REVISION,
                "source_image_ref": SIMPLY_IMAGE_REF, "source_image_id": "sha256:" + "ab" * 32,
                "source_container": "cd" * 32, "metrics_url": "http://indexer:8501/api/metrics",
                "database": {"host": "database", "port": 5432, "name": "shared",
                             "system_identifier": "123456789", "oid": 16384}}

    def test_verified_binding_supports_upstream_censored_database_url(self):
        validate_source(self.metrics(), "testnet-10", "shared")
        binding = self.binding()
        validate_binding(binding, "testnet-10", binding["database"], binding["metrics_url"])

    def test_same_database_name_on_different_server_does_not_pass_network_preflight(self):
        binding = self.binding()
        for field, value in (("host", "production-db"), ("system_identifier", "other-cluster"), ("oid", 99999)):
            identity = {**binding["database"], field: value}
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "SQL instance"):
                validate_binding(binding, "testnet-10", identity, binding["metrics_url"])

    def test_binding_rejects_wrong_network_source_image_and_metrics_target(self):
        binding = self.binding()
        for field, value in (("network", "mainnet"), ("source_revision", "other"),
                             ("source_image_ref", "sha256:other"), ("source_image_id", "invalid"),
                             ("metrics_url", "http://other/metrics")):
            wrong = {**binding, field: value}
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_binding(wrong, "testnet-10", binding["database"], binding["metrics_url"])


if __name__ == "__main__":
    unittest.main()
