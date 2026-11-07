"""Real PostgreSQL scheduling; payloads/resolver/native transport are fixtures."""

from protocol_indexer.chats import ChatsConsumer, EvidencePending
from protocol_indexer.jobs import claim_ready
from protocol_indexer.journal import capture_one, connect
from test_chats_consumer import Transport, resolved
from test_postgres import DSN, PostgresHarness


class JobPriorityTests(PostgresHarness):
    def setUp(self):
        super().setUp()
        self.db.execute('UPDATE protocol_indexer.notification_consumers SET activated_at_ms=1000')

    def claim(self, consumer='native-chats-v1'):
        with self.db.transaction():
            row = claim_ready(self.db, consumer)
            return row['transaction_id'] if row else None

    def test_live_kachat_and_kasia_precede_history_then_history_drains_quietly(self):
        history = [self.ingest(b'ciph_msg:1:comm:history', timestamp=999) for _ in range(3)]
        live = [self.ingest(payload) for payload in (b'kchat:1:handshake:live', b'ciph_msg:1:comm:live')]
        transport = Transport()
        consumer = ChatsConsumer('testnet-10', resolved, transport)
        for _ in range(5):
            self.assertEqual(consumer.run_one(self.db), 'delivered')
        delivered = [bytes.fromhex(b['transactions'][0]['transaction']['tx_id']) for b in transport.batches]
        self.assertEqual(delivered, live + history)
        self.assertEqual(len(transport.notifications), 2)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.consumer_jobs WHERE consumer='native-chats-v1'"), 0)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.events WHERE transaction_id=ANY(%s) AND notify_allowed", (history,)), 0)

    def test_deferred_live_proof_yields_to_ready_history_without_acknowledging_live(self):
        history = self.ingest(b'ciph_msg:1:comm:history', timestamp=999)
        live = self.ingest(b'kchat:1:handshake:live')
        def resolve(conn, row):
            if row['transaction_id'] == live:
                raise EvidencePending('fixture_missing_live_proof')
            return resolved(conn, row)
        transport = Transport()
        consumer = ChatsConsumer('testnet-10', resolve, transport)
        self.assertEqual(consumer.run_one(self.db), 'deferred')
        self.assertEqual(consumer.run_one(self.db), 'delivered')
        self.assertEqual(transport.batches[0]['transactions'][0]['transaction']['tx_id'], history.hex())
        self.assertEqual(self.scalar("SELECT transaction_id FROM protocol_indexer.consumer_jobs WHERE consumer='native-chats-v1'"), live)
        self.assertEqual(transport.notifications, [])

    def test_future_live_backoff_does_not_block_ready_history(self):
        history = self.ingest(b'ciph_msg:1:comm:history', timestamp=999)
        live = self.ingest(b'kchat:1:handshake:live')
        self.db.execute("UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp()+interval '1 hour' WHERE transaction_id=%s", (live,))
        self.assertEqual(self.claim(), history)

    def test_each_consumer_uses_the_same_live_priority_without_taking_another_queue(self):
        self.ingest(b'kchat:1:handshake:live')
        history = self.ingest(b'kchat:1:tip:historical', timestamp=999)
        live = self.ingest(b'kchat:1:tip:live')
        self.assertEqual(self.claim('output-evidence-v1'), live)
        self.db.execute("DELETE FROM protocol_indexer.consumer_jobs WHERE consumer='output-evidence-v1' AND transaction_id=%s", (live,))
        self.assertEqual(self.claim('output-evidence-v1'), history)
        self.assertIsNone(self.claim('unconfigured-consumer'))

    def test_revoked_historical_acceptance_precedes_untouched_history_without_push(self):
        old = self.ingest(b'ciph_msg:1:comm:ordinary-history', timestamp=999)
        revoked = self.ingest(b'kchat:1:handshake:revoked-history', timestamp=999)
        self.db.execute('DELETE FROM transactions_acceptances WHERE transaction_id=%s', (revoked,))
        self.assertTrue(capture_one(self.db))
        transport = Transport()
        consumer = ChatsConsumer('testnet-10', resolved, transport)
        self.assertEqual(consumer.run_one(self.db), 'delivered')
        transaction = transport.batches[0]['transactions'][0]['transaction']
        self.assertEqual(transaction['tx_id'], revoked.hex())
        self.assertIsNone(transaction['accepting_block'])
        self.assertEqual(transport.notifications, [])
        self.assertEqual(self.claim(), old)

    def test_priority_uses_configured_epoch_and_includes_its_exact_boundary(self):
        self.db.execute('UPDATE protocol_indexer.config SET activated_at_ms=3000')
        self.ingest(b'ciph_msg:1:comm:old-epoch', timestamp=2000)
        boundary = self.ingest(b'kchat:1:handshake:at-epoch', timestamp=3000)
        self.assertEqual(self.claim(), boundary)

    def test_missing_block_time_is_not_live_priority(self):
        self.ingest(b'ciph_msg:1:comm:no-time', timestamp=None)
        live = self.ingest(b'kchat:1:handshake:live')
        self.assertEqual(self.claim(), live)

    def test_live_update_stays_prioritized_when_its_latest_event_is_quiet(self):
        self.ingest(b'ciph_msg:1:comm:history', timestamp=999)
        live = self.ingest(b'kchat:1:handshake:live')
        self.db.execute('UPDATE transactions SET block_hash=%s WHERE transaction_id=%s', (b'z' * 32, live))
        self.assertTrue(capture_one(self.db))
        event = self.db.execute('SELECT kind,notify_allowed FROM protocol_indexer.events WHERE transaction_id=%s ORDER BY revision DESC LIMIT 1', (live,)).fetchone()
        self.assertEqual(event['kind'], 'updated')
        self.assertFalse(event['notify_allowed'])
        self.assertEqual(self.claim(), live)

    def test_same_priority_keeps_available_time_then_transaction_id_order(self):
        first = self.ingest(b'ciph_msg:1:comm:first', timestamp=999)
        second = self.ingest(b'kchat:1:handshake:second', timestamp=999)
        self.db.execute("UPDATE protocol_indexer.consumer_jobs SET available_at='2000-01-01T00:00:00Z'")
        self.assertEqual(self.claim(), min(first, second))
        self.db.execute("UPDATE protocol_indexer.consumer_jobs SET available_at='1999-01-01T00:00:00Z' WHERE transaction_id=%s", (second,))
        self.assertEqual(self.claim(), second)

    def test_locked_live_job_is_skipped_without_locking_canonical(self):
        history = self.ingest(b'ciph_msg:1:comm:history', timestamp=999)
        live = self.ingest(b'kchat:1:handshake:live')
        with connect(DSN) as other, other.transaction():
            other.execute("SELECT * FROM protocol_indexer.consumer_jobs WHERE consumer='native-chats-v1' AND transaction_id=%s FOR UPDATE", (live,))
            self.assertEqual(self.claim(), history)
        with self.db.transaction():
            self.assertEqual(claim_ready(self.db, 'native-chats-v1')['transaction_id'], live)
            with connect(DSN) as other, other.transaction():
                other.execute("SET LOCAL lock_timeout='100ms'")
                other.execute('SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=%s FOR UPDATE', (live,))
