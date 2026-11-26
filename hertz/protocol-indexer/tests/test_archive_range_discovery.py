"""Range discovery cannot skip pages, widen scope, or authorize archive candidates."""
import contextlib
import copy
import gzip
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location('archive_blue_discovery', Path(__file__).parents[1] / 'archive_backfill.py')
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)


def row(number=1, payload=b'kchat:1:post:key:sig:body', score=100):
    return {'transaction_id': format(number, '064x'), 'payload': payload.hex() if payload is not None else None,
            'is_accepted': True, 'accepting_block_blue_score': score}


class ArchiveRangeTests(unittest.TestCase):
    def history(self, network='testnet-10', options=None):
        root = self.enterContext(tempfile.TemporaryDirectory())
        h = module.ArchiveHistory(root, 'https://source', 'https://archive', network)
        self.addCleanup(h.db.close)
        h.range_discovery = True
        h.status = lambda: None
        if options is not None: h.request_options = options
        return h

    def state(self, h, jid):
        return h.db.execute('SELECT cursor,pages,done FROM jobs WHERE id=?', (jid,)).fetchone()

    def test_all_six_families_keep_exact_ids_and_binary_messages_as_unverified_candidates(self):
        h = self.history()
        payloads = [b'kchat:1:post:key:sig:body', b'kchat:1:comm:alias:\xff:opaque',
                    b'kchat:1:tip:key:sig:target:author:sealed', b'ciph_msg:1:comm:alias:\x00:opaque',
                    b'k:1:post:key:sig:body', b'k:1:vote:key:sig:target:upvote:author']
        rows = [row(i + 1, payload) for i, payload in enumerate(payloads)]
        rows += [row(7, b'unrelated:payload'), row(8, None)]
        jid = h.queue_blue_range(100, 200)
        with patch.object(h, 'request', return_value=list(reversed(rows))) as request:
            h.discover(1, job_ids=[jid])
        self.assertEqual(json.loads(request.call_args.kwargs['data']), {'acceptingBlueScores': {'gte': 100, 'lt': 200}})
        self.assertEqual(dict(h.db.execute('SELECT txid,status FROM records')),
                         {format(i, '064x'): 'discovered' for i in range(1, 7)})
        self.assertFalse(list((h.root / 'verified').iterdir()))
        self.assertEqual(self.state(h, jid), (None, 1, 1))
        self.assertEqual(h.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)

    def test_empty_pages_advance_and_reopening_preserves_exact_remainder(self):
        h = self.history(); jid = h.queue_blue_range(100, 305)
        with patch.object(h, 'request', return_value=[]): h.discover(1, job_ids=[jid])
        self.assertEqual(self.state(h, jid), ('200', 1, 0))
        other = module.ArchiveHistory(h.root, h.source, h.archive, h.network)
        self.addCleanup(other.db.close); other.range_discovery = True; other.status = lambda: None
        self.assertEqual(other.queue_blue_range(100, 305), jid)
        with patch.object(other, 'request', return_value=[row(score=299)]) as request:
            other.discover(1, job_ids=[jid])
        self.assertEqual(json.loads(request.call_args.kwargs['data'])['acceptingBlueScores'], {'gte': 200, 'lt': 300})
        with patch.object(other, 'request', return_value=[]) as request:
            other.discover(1, job_ids=[jid])
        self.assertEqual(json.loads(request.call_args.kwargs['data'])['acceptingBlueScores'], {'gte': 300, 'lt': 305})
        self.assertEqual(self.state(other, jid), (None, 3, 1))

    def test_failed_db_commit_replays_valid_cached_page_without_losing_candidates(self):
        h = self.history(); jid = h.queue_blue_range(100, 200)
        with patch.object(h, 'request', return_value=[row()]), patch.object(h, 'queue_related', side_effect=RuntimeError('interrupted')):
            with self.assertRaises(RuntimeError): h.discover(1, job_ids=[jid])
        self.assertEqual(self.state(h, jid), (None, 0, 0))
        self.assertEqual(h.db.execute('SELECT count(*) FROM records').fetchone()[0], 0)
        with patch.object(h, 'request', side_effect=AssertionError('cached replay must not fetch')):
            h.discover(1, job_ids=[jid])
        self.assertEqual(dict(h.db.execute('SELECT txid,status FROM records')), {format(1, '064x'): 'discovered'})

    def test_invalid_or_incomplete_response_never_advances_or_keeps_a_partial_page(self):
        missing = row(); del missing['payload']
        for rows in [None, {}, [None], [missing], [row(), row()], [{**row(), 'transaction_id': 'g' * 64}],
                     [{**row(), 'is_accepted': False}], [{**row(), 'is_accepted': 1}],
                     [{**row(), 'accepting_block_blue_score': True}], [row(score=99)], [row(score=200)],
                     [{**row(), 'payload': 42}], [{**row(), 'payload': 'not-hex'}]]:
            with self.subTest(rows=rows):
                h = self.history(); jid = h.queue_blue_range(100, 200)
                with patch.object(h, 'request', return_value=rows), self.assertRaises(ValueError):
                    h.discover(1, job_ids=[jid])
                self.assertEqual(self.state(h, jid), (None, 0, 0))
                self.assertEqual(h.db.execute('SELECT count(*) FROM records').fetchone()[0], 0)
                self.assertFalse(list((h.root / 'pages').iterdir()))

    def test_boundaries_reject_zero_bool_and_unsupported_width(self):
        h = self.history()
        for args in [(0, 200, 100), (100, 100, 100), (-1, 100, 1), (100, 200, 101),
                     (100, 200, 0), (True, 200, 1), (100, 2**63, 1), (None, 200, 1)]:
            with self.subTest(args=args), self.assertRaises(ValueError): h.queue_blue_range(*args)
        self.assertEqual(h.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 0)

    def test_explicit_range_does_not_process_or_complete_unrelated_existing_jobs(self):
        h = self.history(); h.add_job('/get-users', {'unrelated': True}, 'posts'); h.db.commit()
        jid = h.queue_blue_range(100, 200)
        with patch.object(h, 'request', return_value=[]): h.discover(0, job_ids=[jid])
        self.assertEqual(h.db.execute("SELECT cursor,pages,done FROM jobs WHERE route='/get-users'").fetchone(), (None, 0, 0))

    def test_invalid_explicit_job_filter_fails_before_seed_or_network(self):
        h = self.history()
        for ids in [[], 'a' * 64, ['wrong'], [None], [True], ['a' * 64] * 4097]:
            with self.subTest(ids=str(ids)[:30]), patch.object(h, 'seed') as seed:
                with self.assertRaises(ValueError): h.discover(1, job_ids=ids)
                seed.assert_not_called()

    def test_cursor_page_disagreement_does_not_skip_a_range(self):
        for cursor, pages in [('150', 1), ('200', 0), ('400', 3), ('+200', 1), ('٢٠٠', 1), ('99', 0)]:
            with self.subTest(cursor=cursor, pages=pages):
                h = self.history(); jid = h.queue_blue_range(100, 400)
                h.db.execute('UPDATE jobs SET cursor=?,pages=? WHERE id=?', (cursor, pages, jid)); h.db.commit()
                with patch.object(h, 'request', side_effect=AssertionError('no HTTP')), self.assertRaises(ValueError):
                    h.discover(1, job_ids=[jid])
                self.assertEqual(self.state(h, jid), (cursor, pages, 0))

    def test_cached_network_or_range_binding_cannot_advance_another_scope(self):
        for key, bad in [('network', 'mainnet'), ('archive', 'https://other'), ('gte', 101), ('lt', 300), ('scope', 'chat-only')]:
            with self.subTest(key=key):
                h = self.history(); jid = h.queue_blue_range(100, 200)
                with patch.object(h, 'request', return_value=[row()]), patch.object(h, 'queue_related', side_effect=RuntimeError()):
                    with self.assertRaises(RuntimeError): h.discover(1, job_ids=[jid])
                path = h.root / 'pages' / (jid + '-0.json.gz'); value = module.history.load(path)
                value['binding'][key] = bad; module.history.atomic_json(path, value)
                with patch.object(h, 'request', side_effect=AssertionError('no HTTP')), self.assertRaises(ValueError):
                    h.discover(1, job_ids=[jid])
                self.assertEqual(self.state(h, jid), (None, 0, 0))

    def test_bounded_transport_options_restore_after_failure_and_never_loosen_a_caller_bound(self):
        options = {'attempts': 1, 'timeout': 10, 'max_bytes': 2048}
        h = self.history(options=options); jid = h.queue_blue_range(100, 200)
        def fail(*args, **kwargs):
            self.assertEqual(h.request_options, options)
            raise OSError('offline')
        with patch.object(h, 'request', side_effect=fail), self.assertRaises(OSError): h.discover(1, job_ids=[jid])
        self.assertIs(h.request_options, options)
        self.assertEqual(self.state(h, jid), (None, 0, 0))
        other = self.history(); jid = other.queue_blue_range(100, 200)
        def bounded(*args, **kwargs):
            self.assertEqual(other.request_options['max_bytes'], 8 * 1024**2)
            return []
        with patch.object(other, 'request', side_effect=bounded): other.discover(1, job_ids=[jid])
        self.assertFalse(hasattr(other, 'request_options'))

    def test_oversized_cache_is_rejected_before_decoding_or_commit(self):
        h = self.history(); jid = h.queue_blue_range(100, 200)
        path = h.root / 'pages' / (jid + '-0.json.gz')
        with gzip.open(path, 'wb') as stream: stream.write(b' ' * (16 * 1024**2 + 1))
        with self.assertRaisesRegex(ValueError, 'cache exceeds'): h.discover(1, job_ids=[jid])
        self.assertEqual(self.state(h, jid), (None, 0, 0))

    def test_cli_rejects_invalid_range_before_creating_inventory(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory())) / 'not-created'
        base = ['archive_backfill.py', 'discover-range', str(root), '--source', 'https://source', '--archive', 'https://archive', '--network', 'testnet-10']
        for args in [[], ['--blue-from', '0', '--blue-to', '200'], ['--blue-from', '100', '--blue-to', '200', '--blue-step', '101']]:
            with self.subTest(args=args), patch.object(sys, 'argv', base + args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error: module.main()
                self.assertEqual(error.exception.code, 2)
                self.assertFalse(root.exists())

    def test_cli_network_gate_precedes_any_range_job_or_fetch(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        args = ['archive_backfill.py', 'discover-range', str(root), '--source', 'https://source', '--archive', 'https://archive', '--network', 'testnet-10', '--blue-from', '100', '--blue-to', '200']
        with patch.object(sys, 'argv', args), patch.object(module.ArchiveHistory, 'bind_network', side_effect=ValueError('network mismatch')), patch.object(module.ArchiveHistory, 'request') as request:
            with self.assertRaisesRegex(ValueError, 'network mismatch'): module.main()
            request.assert_not_called()
        h = module.ArchiveHistory(root, 'https://source', 'https://archive', 'testnet-10'); self.addCleanup(h.db.close)
        self.assertEqual(h.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 0)


if __name__ == '__main__': unittest.main()
