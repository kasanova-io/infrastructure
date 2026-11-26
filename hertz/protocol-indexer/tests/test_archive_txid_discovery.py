"""Retained inventories can seed the shared verifier without granting proof."""
import contextlib
import fcntl
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    'archive_txid_discovery', Path(__file__).parents[1] / 'archive_backfill.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class TransactionDiscoveryTests(unittest.TestCase):
    def history(self, network='testnet-10'):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        h = module.ArchiveHistory(root, 'https://source', 'https://archive', network)
        self.addCleanup(h.db.close)
        return h

    def args(self, root, network='testnet-10'):
        return ['archive_backfill.py', 'discover-txids', str(root),
                '--source', 'https://source', '--archive', 'https://archive',
                '--network', network]

    def test_intake_preserves_verified_quarantined_and_observation_state(self):
        h = self.history()
        ids = [format(i, '064x') for i in range(1, 5)]
        h.db.executemany('INSERT INTO records(txid,status,kind,error) VALUES (?,?,?,?)',
                        [(ids[0], 'verified', 'kachat_posts', None),
                         (ids[1], 'quarantined', None, 'original adverse'),
                         (ids[2], 'discovered', None, None)])
        h.db.execute('INSERT INTO observations VALUES (?,?,?)', (ids[0], 'existing', '{"unchanged":true}'))
        h.db.commit()
        before = h.db.execute('SELECT * FROM records ORDER BY txid').fetchall()
        self.assertEqual(h.queue_transactions(ids + [ids[3]]), 1)
        self.assertEqual(h.queue_transactions(ids), 0)
        self.assertEqual(h.db.execute('SELECT * FROM records ORDER BY txid').fetchall(),
                         before + [(ids[3], 'discovered', None, None)])
        self.assertEqual(h.db.execute('SELECT * FROM observations').fetchall(),
                         [(ids[0], 'existing', '{"unchanged":true}')])
        self.assertFalse(list((h.root / 'verified').iterdir()))

    def test_registration_does_not_require_or_release_existing_verifier_lock(self):
        for network in ('mainnet', 'testnet-10'):
            with self.subTest(network=network):
                h = self.history(network)
                txid = 'a' * 64
                output = io.StringIO()
                with (h.root / 'verify.lock').open('a') as owner:
                    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    with patch.object(sys, 'argv', self.args(h.root, network) + ['--txid', txid]), \
                            patch.object(module.ArchiveHistory, 'bind_network') as binding, \
                            patch.object(module.ArchiveHistory, 'verify') as verify, \
                            patch.object(module.ArchiveHistory, 'discover') as discover, \
                            patch.object(module.ArchiveHistory, 'request') as request, \
                            contextlib.redirect_stdout(output):
                        module.main()
                    binding.assert_called_once_with()
                    verify.assert_not_called()
                    discover.assert_not_called()
                    request.assert_not_called()
                    with (h.root / 'verify.lock').open('a') as other:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.assertEqual(json.loads(output.getvalue()), {
                    'queued': 1, 'candidates': 1, 'verification_performed': False, 'canonical_imported': 0})
                self.assertEqual(h.db.execute('SELECT txid,status,kind,error FROM records').fetchall(),
                                 [(txid, 'discovered', None, None)])

    def test_wrong_current_network_never_queues_candidates(self):
        h = self.history()
        with patch.object(sys, 'argv', self.args(h.root) + ['--txid', 'a' * 64]), \
                patch.object(module.ArchiveHistory, 'bind_network', side_effect=ValueError('network mismatch')):
            with self.assertRaisesRegex(ValueError, 'network mismatch'):
                module.main()
        self.assertEqual(h.db.execute('SELECT count(*) FROM records').fetchone()[0], 0)

    def test_existing_verify_cli_keeps_explicit_quarantine_retry_and_budget(self):
        for retry in (False, True):
            with self.subTest(retry=retry):
                h = self.history()
                verified, quarantined, fresh = [format(i, '064x') for i in range(1, 4)]
                h.db.executemany('INSERT INTO records(txid,status,kind,error) VALUES (?,?,?,?)',
                                [(verified, 'verified', 'kachat_posts', None),
                                 (quarantined, 'quarantined', None, 'original adverse')])
                h.db.commit()
                args = self.args(h.root)
                args[1] = 'verify'
                args += ['--txid', verified, '--txid', quarantined, '--txid', fresh,
                         '--limit', '1', '--workers', '2']
                if retry:
                    args.append('--retry-quarantine')
                with patch.object(sys, 'argv', args), \
                        patch.object(module.ArchiveHistory, 'bind_network'), \
                        patch.object(module.ArchiveHistory, 'verify') as verify, \
                        patch.object(module.ArchiveHistory, 'status'):
                    module.main()
                verify.assert_called_once_with(1, retry, [quarantined, fresh] if retry else [fresh], 2)
                self.assertEqual(h.db.execute('SELECT status,error FROM records WHERE txid=?',
                                             (quarantined,)).fetchone(), ('quarantined', 'original adverse'))

    def test_intake_still_respects_existing_discovery_owner(self):
        h = self.history()
        with (h.root / 'discover.lock').open('a') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(sys, 'argv', self.args(h.root) + ['--txid', 'a' * 64]), \
                    patch.object(module.ArchiveHistory, 'bind_network') as binding:
                with self.assertRaises(BlockingIOError):
                    module.main()
            binding.assert_not_called()
        self.assertEqual(h.db.execute('SELECT count(*) FROM records').fetchone()[0], 0)

    def test_invalid_or_unbounded_input_is_rejected_before_any_insert(self):
        h = self.history()
        for ids in ([], None, 'a' * 64, [None], [True], ['wrong'],
                    ['a' * 64] * 4097, ['a' * 64, 'invalid']):
            with self.subTest(ids=str(ids)[:40]), self.assertRaises(ValueError):
                h.queue_transactions(ids)
        self.assertEqual(h.db.execute('SELECT count(*) FROM records').fetchone()[0], 0)
        self.assertEqual(h.queue_transactions([format(i, '064x') for i in range(4096)]), 4096)

    def test_failed_insert_rolls_back_entire_candidate_batch(self):
        h = self.history()
        h.db.execute("CREATE TRIGGER fail_second BEFORE INSERT ON records WHEN NEW.txid='" + 'b' * 64 +
                     "' BEGIN SELECT RAISE(ABORT, 'interrupted'); END")
        h.db.commit()
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'interrupted'):
            h.queue_transactions(['a' * 64, 'b' * 64])
        self.assertEqual(h.db.execute('SELECT count(*) FROM records').fetchone()[0], 0)

    def test_invalid_cli_combinations_never_create_inventory(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory())) / 'not-created'
        for tail in ([], ['--txid', 'invalid'], ['--txid', 'a' * 64, '--limit', '1'],
                     ['--txid', 'a' * 64, '--retry-quarantine'], ['--txid', 'a' * 64, '--workers', '2']):
            with self.subTest(tail=tail), patch.object(sys, 'argv', self.args(root) + tail), \
                    contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    module.main()
                self.assertEqual(error.exception.code, 2)
                self.assertFalse(root.exists())


if __name__ == '__main__':
    unittest.main()
