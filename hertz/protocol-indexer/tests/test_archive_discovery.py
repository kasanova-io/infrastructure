import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    'archive_action_discovery', Path(__file__).parents[1] / 'archive_backfill.py'
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
KEY = '02' + '1' * 64
SENDER = '03' + '2' * 64
POST = 'a' * 64
VOTE = 'b' * 64


class ArchiveActionDiscoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.h = module.ArchiveHistory(self.tmp.name, 'https://source', 'https://archive', 'mainnet')
        self.addCleanup(self.h.db.close)
        self.h.status = lambda: None

    def jobs(self, route):
        return [json.loads(row[0]) for row in self.h.db.execute(
            'SELECT params FROM jobs WHERE route=?', (route,))]

    def test_completed_content_inventory_adds_actions_without_resetting_cursor(self):
        self.h.add_job('/get-posts-watching', {'requesterPubkey': module.history.ANON}, 'posts')
        self.h.db.execute("UPDATE jobs SET done=1,cursor='200_3',pages=2")
        self.h.db.execute('INSERT INTO observations VALUES (?,?,?)',
                          (POST, 'old', json.dumps({'userPublicKey': KEY})))
        self.h.db.commit()
        with patch.object(module.history.History, 'seed'):
            self.h.seed()
            self.h.seed()
        self.assertEqual(self.jobs('/get-notifications'), [{'requesterPubkey': KEY}])
        self.assertEqual(self.h.db.execute(
            "SELECT done,cursor,pages FROM jobs WHERE route='/get-posts-watching'"
        ).fetchone(), (1, '200_3', 2))

    def test_notification_cursor_resume_deduplicates_and_keeps_proofs_pending(self):
        self.h.seed = lambda: None
        self.h.queue_actions(KEY)
        self.h.db.commit()
        row = {'id': VOTE, 'userPublicKey': SENDER, 'contentType': 'vote',
               'contentId': POST, 'timestamp': 100}
        page = {'notifications': [row], 'pagination': {'hasMore': True, 'nextCursor': '100_9'}}
        with patch.object(self.h, 'cache', return_value=page):
            self.h.discover(1)
        job = self.h.db.execute(
            "SELECT cursor,pages,done FROM jobs WHERE route='/get-notifications' AND params=?",
            (json.dumps({'requesterPubkey': KEY}, sort_keys=True),)
        ).fetchone()
        self.assertEqual(job, ('100_9', 1, 0))
        # Finish only this checkpointed job; other generated jobs remain intact.
        self.h.db.execute("UPDATE jobs SET done=1 WHERE route!='/get-notifications' OR params!=?",
                          (json.dumps({'requesterPubkey': KEY}, sort_keys=True),))
        self.h.db.commit()
        with patch.object(self.h, 'cache', return_value={
            'notifications': [row], 'pagination': {'hasMore': False, 'nextCursor': None}
        }) as cache:
            self.h.discover(1)
        self.assertIn('before=100_9', cache.call_args.args[2])
        self.assertEqual(dict(self.h.db.execute('SELECT txid,status FROM records')),
                         {VOTE: 'discovered', POST: 'discovered'})
        self.assertEqual(self.h.db.execute('SELECT count(*) FROM observations').fetchone()[0], 1)

    def test_vote_does_not_scan_replies_to_a_vote_transaction(self):
        self.h.queue_related({'id': VOTE, 'userPublicKey': SENDER,
                              'contentType': 'vote', 'contentId': POST}, '/get-notifications')
        replies = self.jobs('/get-replies')
        self.assertNotIn({'post': VOTE, 'requesterPubkey': module.history.ANON}, replies)
        self.assertIn({'post': POST, 'requesterPubkey': module.history.ANON}, replies)
        self.assertIn({'requesterPubkey': SENDER}, self.jobs('/get-notifications'))

    def test_notification_content_keeps_normal_reply_traversal(self):
        for kind in ('post', 'reply', 'quote', 'poll'):
            with self.subTest(kind=kind):
                self.h.queue_related({'id': POST, 'userPublicKey': KEY,
                                      'contentType': kind}, '/get-notifications')
                self.assertIn({'post': POST, 'requesterPubkey': module.history.ANON},
                              self.jobs('/get-replies'))

    def test_malformed_targets_and_author_never_become_candidate_jobs(self):
        for value in (None, '', '../path', 'g' * 64, 42):
            with self.subTest(value=value):
                self.h.queue_actions(value)
                self.h.queue_related({'id': VOTE, 'userPublicKey': KEY,
                                      'contentType': 'vote', 'contentId': value}, '/get-notifications')
        self.assertEqual(list(self.h.db.execute('SELECT txid FROM records')), [])
        self.assertEqual(self.jobs('/get-notifications'), [{'requesterPubkey': KEY}])

    def test_source_failure_leaves_notification_checkpoint_unfinished(self):
        self.h.seed = lambda: None
        self.h.queue_actions(KEY)
        self.h.db.commit()
        with patch.object(self.h, 'cache', side_effect=OSError('source unavailable')):
            with self.assertRaises(OSError):
                self.h.discover(1)
        self.assertEqual(self.h.db.execute('SELECT done,cursor,pages FROM jobs').fetchone(), (0, None, 0))
        self.assertEqual(self.h.db.execute('SELECT count(*) FROM records').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
