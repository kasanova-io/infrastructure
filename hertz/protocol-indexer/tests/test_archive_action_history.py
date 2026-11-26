import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location('archive_control_history', Path(__file__).parents[1] / 'archive_backfill.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
collect = module.history._chats
POST, ACTION, OTHER = ('a' * 64, 'b' * 64, 'c' * 64)
KEY = '02' + '1' * 64


class AddressCollectionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def scan(self, archive='https://archive', network='testnet-10', filter='six-v1', **extra):
        return collect.collect_explorer('kaspatest:fixture', self.root, archive=archive,
            checkpoint_scope={'archive': archive, 'network': network, 'filter': filter},
            accept_payload=lambda raw: module.classify(raw) is not None,
            page_budget=1, **extra)

    def test_scope_does_not_reuse_completed_chats_network_or_filter(self):
        with patch.object(collect, 'request', return_value=([], {})) as request:
            collect.collect_explorer('kaspatest:fixture', self.root)
            self.scan()
            self.scan(network='testnet-11')
            self.scan(archive='https://other')
            self.scan(filter='six-v2')
            self.scan()
        self.assertEqual(request.call_count, 5)

    def test_bounded_resume_keeps_controls_and_binary_messages(self):
        first = [{'transaction_id': ACTION, 'payload': b'kchat:1:delete:key:sig:target'.hex()},
                 {'transaction_id': POST, 'payload': b'k:1:broadcast:key:sig:profile'.hex()}]
        second = [{'transaction_id': OTHER, 'payload': b'ciph_msg:1:comm:alias:\xff:binary'.hex()}]
        with patch.object(collect, 'request', return_value=(first, {'X-Next-Page-Before': '99'})):
            result = self.scan()
        self.assertFalse(result['done'])
        with patch.object(collect, 'request', return_value=(second, {})) as request:
            result = self.scan()
        self.assertIn('before=99', request.call_args.args[0])
        self.assertTrue(result['done'])
        self.assertEqual({r['transaction_id'] for r in result['rows']}, {POST, ACTION, OTHER})

    def test_nonadvancing_archive_cursor_keeps_unfinished_checkpoint(self):
        with patch.object(collect, 'request', return_value=([], {'X-Next-Page-Before': '99'})):
            self.scan()
            with self.assertRaisesRegex(ValueError, 'did not advance'):
                self.scan()


class ActionHistoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.h = module.ArchiveHistory(self.tmp.name, 'https://source', 'https://archive', 'mainnet')
        self.addCleanup(self.h.db.close)
        self.h.seed = lambda: None
        self.h.status = lambda: None
        self.h.action_discovery = True

    def test_addresses_only_expand_from_verified_receipts_and_both_sides(self):
        self.h.db.executemany('INSERT INTO records(txid,status) VALUES (?,?)', [(POST, 'verified'), (ACTION, 'discovered')])
        module.history.atomic_json(self.h.root / 'verified' / (POST + '.json.gz'), {
            'transaction_id': POST, 'network': 'mainnet', 'payload': b'kchat:1:post:key:sig:body'.hex(),
            'archive_transaction': {'outputs': [{'script_public_key_address': 'kaspa:recipient'}],
                                    'inputs': [{'previous_outpoint_address': 'kaspa:funding'}]}})
        self.h.seed_action_history()
        self.h.seed_action_history()
        addresses = [json.loads(r[0])['address'] for r in self.h.db.execute('SELECT params FROM jobs WHERE route=?', (self.h.address_route,))]
        self.assertEqual(set(addresses), {'kaspa:recipient', 'kaspa:funding'})
        self.assertEqual(self.h.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 4)

    def test_address_empty_page_resume_and_crash_replay_never_authorize_candidates(self):
        self.h.queue_address('kaspa:fixture')
        self.h.db.commit()
        with patch.object(collect, 'collect_explorer', return_value={'rows': [], 'done': False, 'before': 99}):
            self.h.discover(1)
        row = {'transaction_id': ACTION}
        with patch.object(collect, 'collect_explorer', return_value={'rows': [row], 'done': True, 'before': 99}):
            with patch.object(self.h, 'queue_related', side_effect=RuntimeError('interrupted DB commit')):
                with self.assertRaisesRegex(RuntimeError, 'interrupted'):
                    self.h.discover(1)
            self.assertEqual(self.h.db.execute('SELECT count(*) FROM records').fetchone()[0], 0)
            self.h.discover(1)
        self.assertEqual(self.h.db.execute('SELECT txid,status FROM records').fetchone(), (ACTION, 'discovered'))
        self.assertFalse(list((self.h.root / 'verified').iterdir()))

    def test_engagement_adapter_uses_action_id_and_rejects_opaque_cursor_cycles(self):
        self.h.add_job('/get-post-engagement', {'postId': POST}, 'engagement')
        self.h.db.commit()
        def page(cursor):
            return {'engagement': [{'actionTxId': ACTION, 'actorPubkey': KEY, 'kind': 'upvote', 'timestamp': 100}],
                    'pagination': {'hasMore': True, 'nextCursor': cursor}}
        for cursor in ('opaque-A', 'opaque-B'):
            self.h.db.execute("UPDATE jobs SET done=1 WHERE route!='/get-post-engagement'")
            self.h.db.commit()
            with patch.object(self.h, 'cache', return_value=page(cursor)):
                self.h.discover(1)
        self.h.db.execute("UPDATE jobs SET done=1 WHERE route!='/get-post-engagement'")
        self.h.db.commit()
        with patch.object(self.h, 'cache', return_value=page('opaque-A')):
            with self.assertRaisesRegex(ValueError, 'cycle'):
                self.h.discover(1)
        self.assertEqual(dict(self.h.db.execute('SELECT txid,status FROM records')), {ACTION: 'discovered'})
        self.assertFalse(self.h.db.execute("SELECT 1 FROM jobs WHERE route='/get-replies'").fetchone())

    def test_tip_ids_are_candidates_not_payment_counts(self):
        self.h.add_job('/get-post-tippers', {'postId': POST}, 'tippers')
        self.h.db.commit()
        with patch.object(self.h, 'cache', return_value={'tippers': [{'id': ACTION, 'userPublicKey': KEY, 'amountSompi': 10000000}],
                     'pagination': {'hasMore': False, 'nextCursor': None}}):
            self.h.discover(1)
        self.assertEqual(self.h.db.execute('SELECT status FROM records').fetchone()[0], 'discovered')

    def test_network_and_source_failure_keep_address_job_unfinished(self):
        with self.assertRaises(ValueError):
            self.h.queue_address('kaspatest:fixture')
        self.h.queue_address('kaspa:fixture')
        self.h.db.commit()
        with patch.object(collect, 'collect_explorer', side_effect=OSError('archive unavailable')):
            with self.assertRaises(OSError):
                self.h.discover(1)
        self.assertEqual(self.h.db.execute('SELECT done,cursor,pages FROM jobs').fetchone(), (0, None, 0))


if __name__ == '__main__':
    unittest.main()
