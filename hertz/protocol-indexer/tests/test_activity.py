"""Real source-trigger/retention boundaries for ordinary Funds received."""
from test_postgres import PostgresHarness
from protocol_indexer.journal import install, prune_source


class ActivityTests(PostgresHarness):
    def test_plain_body_acceptance_and_revisions_keep_first_observation(self):
        txid = self.txid()
        self.body(txid, None)
        before = self.db.execute("SELECT * FROM protocol_indexer.activity_transactions").fetchone()
        queued = self.scalar("SELECT queued_at FROM protocol_indexer.activity_pending")
        self.accept(txid)
        self.db.execute("UPDATE transactions SET block_time=2001 WHERE transaction_id=%s", (txid,))
        self.assertEqual(self.db.execute("SELECT * FROM protocol_indexer.activity_transactions").fetchone(), before)
        self.assertEqual(self.scalar("SELECT queued_at FROM protocol_indexer.activity_pending"), queued)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.canonical"), 0)

    def test_acceptance_can_commit_before_body_without_foreign_key_loss(self):
        txid = self.txid()
        self.accept(txid)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_transactions"), 0)
        self.body(txid, None)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_transactions"), 1)

    def test_install_does_not_enqueue_retained_plain_history_or_reset_state(self):
        self.db.execute("DROP TRIGGER protocol_activity_body ON transactions; DROP TRIGGER protocol_activity_acceptance ON transactions_acceptances")
        old = self.txid()
        self.body(old, None)
        self.accept(old)
        install(self.db, "testnet-10")
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 0)
        fresh = self.txid()
        self.body(fresh, None)
        before = self.db.execute("SELECT * FROM protocol_indexer.activity_transactions").fetchone()
        install(self.db, "testnet-10")
        self.assertEqual(self.db.execute("SELECT * FROM protocol_indexer.activity_transactions").fetchone(), before)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.notification_consumers WHERE consumer='native-address-push-v1'"), 0)

    def test_unhandled_body_stays_protected_even_if_pending_was_removed(self):
        txid = self.txid()
        self.body(txid, None)
        self.accept(txid)
        self.db.execute("DELETE FROM protocol_indexer.activity_pending")
        self.assertEqual(prune_source(self.db, 3000), 0)
        self.db.execute("UPDATE protocol_indexer.activity_transactions SET handled=true")
        self.assertEqual(prune_source(self.db, 3000), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 0)

    def test_prune_wakeup_suppression_does_not_escape_its_call_or_hide_vcp(self):
        old, live = self.txid(), self.txid()
        self.body(old, None)
        self.accept(old)
        self.body(live, None, timestamp=4000)
        self.accept(live)
        self.activity_handled()
        with self.db.transaction():
            self.assertEqual(prune_source(self.db, 3000), 1)
            self.assertNotEqual(self.scalar("SELECT current_setting('protocol_indexer.activity_pruning',true)"), "true")
            self.db.execute("DELETE FROM transactions_acceptances WHERE transaction_id=%s", (live,))
            self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 1)

    def test_archive_acceptance_seed_is_quiet_but_real_vcp_still_wakes(self):
        txid = self.txid()
        self.body(txid, None)
        self.activity_handled()
        with self.db.transaction():
            self.db.execute("SELECT set_config('protocol_indexer.archive_seed',%s,true)", (txid.hex(),))
            self.accept(txid)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 0)
        self.db.execute("DELETE FROM transactions_acceptances WHERE transaction_id=%s", (txid,))
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.activity_pending"), 1)
