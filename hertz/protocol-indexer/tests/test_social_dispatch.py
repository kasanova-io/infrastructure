"""One source, per-domain durable wakeups; decoding remains native."""
from protocol_indexer.journal import BackfillAdapter, capture_one
from protocol_indexer.routing import Family
from test_postgres import PostgresHarness


class SocialDispatchTests(PostgresHarness):
    def consumers(self, txid):
        return {row['consumer'] for row in self.db.execute(
            'SELECT consumer FROM protocol_indexer.consumer_jobs WHERE transaction_id=%s', (txid,)).fetchall()}

    def test_dual_posts_actions_and_public_rooms_share_social_consumer_private_messages_stay_private(self):
        for payload in (b'k:1:post:key:sig:body:[]', b'kchat:1:post:key:sig:body:[]',
                        b'k:1:follow:key:sig:follow:target', b'kchat:1:edit:key:sig:post:body:[]',
                        b'kchat:1:bcast:room:body', b'ciph_msg:1:bcast:room:body'):
            txid = self.ingest(payload)
            self.assertEqual(self.consumers(txid), {'native-social-v1'})
        for payload in (b'kchat:1:comm:alias:sealed', b'ciph_msg:1:handshake:sealed'):
            txid = self.ingest(payload)
            self.assertEqual(self.consumers(txid), {'native-chats-v1'})
        # New inbox/DM stays retained until its native decoder upgrade; never
        # route encrypted content into the public Social projection.
        self.assertEqual(self.consumers(self.ingest(b'kchat:1:dm:tag:alias:sealed')), set())

    def test_tip_delivers_to_private_payment_and_public_verified_social_consumers(self):
        txid = self.ingest(b'kchat:1:tip:key:sig:post:author:cipher')
        self.assertEqual(self.consumers(txid), {'native-chats-v1', 'native-social-v1', 'output-evidence-v1'})
        self.db.execute('DELETE FROM transactions_acceptances WHERE transaction_id=%s', (txid,))
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.consumers(txid), {'native-chats-v1', 'native-social-v1', 'output-evidence-v1'})
        self.assertEqual(self.scalar('SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=%s', (txid,)), 2)

    def test_inventory_backfill_queues_existing_social_history_without_duplicate_jobs(self):
        txid = self.ingest(b'k:1:post:key:sig:body:[]')
        self.db.execute("DELETE FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1'")
        adapter = BackfillAdapter(Family.KSOCIAL_POSTS)
        adapter.run_page(self.db)
        adapter.run_page(self.db)
        self.assertEqual(self.consumers(txid), {'native-social-v1'})
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1'"), 1)
