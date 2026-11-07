"""Reuse checked history without making it an acceptance or notification source."""
from contextlib import closing
import copy
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from protocol_indexer.archive import checked_receipt, verify_archive
from protocol_indexer.chats import EvidencePending
from test_archive import ACCEPT, INFO, TX, evidence


class SharedHistoryCacheTests(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).parents[1] / 'chats_worker.py'
        spec = importlib.util.spec_from_file_location('shared_history_cache_test_worker', path)
        self.worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.worker)
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.source = self.directory / 'history'
        self.source.mkdir()
        for name in ['blocks', 'verified']:
            (self.source / name).mkdir()
        with closing(sqlite3.connect(self.source / 'inventory.sqlite')) as db, db:
            db.executescript('CREATE TABLE config(id integer primary key,value text); CREATE TABLE records(txid text primary key,status text);')
            db.execute('INSERT INTO config VALUES(1,?)', (json.dumps({
                'source': 'https://public-discovery', 'archive': 'https://archive', 'network': 'testnet-10'}),))
            db.execute('INSERT INTO config VALUES(3,?)', ('six-protocol-archive-evidence-v1',))
            db.execute('INSERT INTO records VALUES(?,?)', (TX, 'verified'))
        self.values = evidence(b'ciph_msg:1:comm:\xff\x00:sealed:tail')
        self.receipt = verify_archive(TX, 'testnet-10', INFO, *self.values)
        self.receipt['archive'] = 'https://archive'
        self.save('verified', TX, self.receipt)
        self.save('blocks', ACCEPT, self.values[1])

    def save(self, folder, key, value):
        path = self.source / folder / (key + '.json.gz')
        self.worker.history.atomic_json(path, value)
        return path

    def resolver(self, **kwargs):
        resolver = self.worker.Resolver(self.directory / 'delivery', 'https://archive',
                                        'https://archive', 'testnet-10',
                                        history_cache=self.source, **kwargs)
        self.addCleanup(resolver.db.close)
        resolver.network_info = INFO
        return resolver

    def test_checked_body_and_header_reuse_need_no_http_or_cache_copy(self):
        resolver = self.resolver()
        original = {p: p.read_bytes() for p in [self.source / 'inventory.sqlite',
                    self.source / 'verified' / (TX + '.json.gz'), self.source / 'blocks' / (ACCEPT + '.json.gz')]}
        with patch.object(resolver, 'request', side_effect=AssertionError('already checked in shared inventory')):
            self.assertEqual(resolver.collect_body(TX), checked_receipt(self.receipt, TX, 'testnet-10', with_input=False))
            self.assertEqual(resolver.verified_header(ACCEPT), self.values[1])
        self.assertFalse(any((resolver.root / 'verified').iterdir()))
        self.assertFalse(any((resolver.root / 'blocks').iterdir()))
        self.assertEqual(original, {p: p.read_bytes() for p in original})

    def test_delivery_cache_stays_first(self):
        resolver = self.resolver()
        self.worker.history.atomic_json(resolver.root / 'verified' / (TX + '.json.gz'), self.receipt)
        self.worker.history.atomic_json(resolver.root / 'blocks' / (ACCEPT + '.json.gz'), self.values[1])
        with patch.object(resolver, 'history_value', side_effect=AssertionError('own cache wins')):
            self.assertEqual(resolver.collect_body(TX)['payload'], self.receipt['payload'])
            self.assertEqual(resolver.verified_header(ACCEPT), self.values[1])

    def test_existing_constructor_without_shared_cache_uses_archive(self):
        resolver = self.worker.Resolver(self.directory / 'legacy', 'https://archive', 'https://archive', 'testnet-10')
        self.addCleanup(resolver.db.close)
        resolver.network_info = INFO
        with patch.object(resolver, 'request', side_effect=self.values) as request:
            self.assertEqual(resolver.collect_body(TX)['payload'], self.receipt['payload'])
        self.assertEqual(request.call_count, 3)

    def test_wrong_profile_or_scope_is_rejected_before_own_cache_creation(self):
        for change in ['network', 'archive', 'scope', 'missing', 'not_object']:
            with self.subTest(change=change):
                with closing(sqlite3.connect(self.source / 'inventory.sqlite')) as db, db:
                    profile = {'source': 'https://public-discovery', 'archive': 'https://archive', 'network': 'testnet-10'}
                    if change in {'network', 'archive'}:
                        profile[change] = 'other'
                    db.execute('INSERT OR REPLACE INTO config VALUES(1,?)', (json.dumps([] if change == 'not_object' else profile),))
                    db.execute('INSERT OR REPLACE INTO config VALUES(3,?)', ('wrong' if change == 'scope' else 'six-protocol-archive-evidence-v1',))
                    if change == 'missing':
                        db.execute('DELETE FROM config WHERE id=1')
                with self.assertRaises(ValueError):
                    self.resolver()
                self.assertFalse((self.directory / 'delivery').exists())

    def test_mainnet_profile_is_supported_without_a_network_default(self):
        with closing(sqlite3.connect(self.source / 'inventory.sqlite')) as db, db:
            db.execute('UPDATE config SET value=? WHERE id=1', (json.dumps({
                'source': 'https://different-discovery', 'archive': 'https://mainnet-archive', 'network': 'mainnet'}),))
        resolver = self.worker.Resolver(self.directory / 'mainnet', 'https://mainnet-archive',
                                        'https://mainnet-archive', 'mainnet', history_cache=self.source)
        self.addCleanup(resolver.db.close)
        self.assertEqual(resolver.network, 'mainnet')

    def test_runtime_profile_drift_never_uses_the_other_network(self):
        resolver = self.resolver()
        with closing(sqlite3.connect(self.source / 'inventory.sqlite')) as db, db:
            db.execute('UPDATE config SET value=? WHERE id=1', (json.dumps({
                'archive': 'https://archive', 'network': 'mainnet'}),))
        with patch.object(resolver, 'request', side_effect=AssertionError('configuration fault')):
            with self.assertRaises(EvidencePending):
                resolver.collect_body(TX)
            with self.assertRaises(ValueError):
                resolver.verified_header(ACCEPT)

    def test_unverified_or_missing_source_record_does_not_authorize_a_body(self):
        resolver = self.resolver()
        for status in ['discovered', 'quarantined', None]:
            with self.subTest(status=status):
                with closing(sqlite3.connect(self.source / 'inventory.sqlite')) as db, db:
                    if status is None:
                        db.execute('DELETE FROM records WHERE txid=?', (TX,))
                    else:
                        db.execute('UPDATE records SET status=? WHERE txid=?', (status, TX))
                self.assertIsNone(resolver.history_value('verified', TX))

    def test_bad_source_body_is_reacquired_without_quarantining_another_owner(self):
        for change in ['payload', 'network', 'source', 'notify', 'rpc', 'primitive']:
            with self.subTest(change=change):
                resolver = self.resolver()
                bad = copy.deepcopy(self.receipt)
                if change == 'payload':
                    bad['payload'] += 'ff'
                elif change == 'network':
                    bad['network'] = 'mainnet'
                elif change == 'source':
                    bad['archive'] = 'https://other'
                elif change == 'notify':
                    bad['notify_allowed'] = True
                elif change == 'rpc':
                    bad['rpc_transaction']['outputs'][0]['amount'] += 1
                else:
                    bad = []
                path = self.save('verified', TX, bad)
                before = path.read_bytes()
                # Shared header is valid; body and full containing block still
                # come from the unchanged normal verifier.
                with patch.object(resolver, 'request', side_effect=[self.values[0], self.values[2]]) as request:
                    self.assertEqual(resolver.collect_body(TX)['payload'], self.receipt['payload'])
                self.assertEqual(request.call_count, 2)
                self.assertEqual(path.read_bytes(), before)
                self.assertFalse(any((resolver.root / 'quarantine').iterdir()))
                (resolver.root / 'verified' / (TX + '.json.gz')).unlink()
                resolver.db.close()

    def test_malformed_or_oversized_source_files_fall_back_without_owner_writes(self):
        resolver = self.resolver()
        path = self.source / 'blocks' / (ACCEPT + '.json.gz')
        for content in ['gzip', 'json', 'oversized', 'shape', 'identity', 'timestamp']:
            with self.subTest(content=content):
                if content == 'gzip':
                    path.write_bytes(b'not gzip')
                elif content == 'json':
                    import gzip
                    path.write_bytes(gzip.compress(b'not json'))
                elif content == 'oversized':
                    import gzip
                    path.write_bytes(gzip.compress(b' ' * (8 * 1024**2 + 1)))
                else:
                    value = copy.deepcopy(self.values[1])
                    if content == 'shape':
                        value = []
                    elif content == 'identity':
                        value['verboseData']['hash'] = 'ff' * 32
                    else:
                        value['header']['timestamp'] = None
                    self.save('blocks', ACCEPT, value)
                before = path.read_bytes()
                with patch.object(resolver, 'request', return_value=self.values[1]) as request:
                    self.assertEqual(resolver.verified_header(ACCEPT), self.values[1])
                request.assert_called_once()
                self.assertEqual(path.read_bytes(), before)
                self.assertFalse(any((resolver.root / 'quarantine').iterdir()))
                (resolver.root / 'blocks' / (ACCEPT + '.json.gz')).unlink()

    def test_shared_symlink_is_not_read(self):
        resolver = self.resolver()
        path = self.source / 'blocks' / (ACCEPT + '.json.gz')
        saved = path.with_name('elsewhere.gz')
        path.rename(saved)
        path.symlink_to(saved)
        self.assertIsNone(resolver.history_value('blocks', ACCEPT))

    def test_excessively_nested_json_falls_back_for_body_and_header(self):
        import gzip
        resolver = self.resolver()
        nested = gzip.compress(b'[' * 10000 + b'0' + b']' * 10000)
        for folder, key in [('verified', TX), ('blocks', ACCEPT)]:
            with self.subTest(folder=folder):
                path = self.source / folder / (key + '.json.gz')
                path.write_bytes(nested)
                responses = [self.values[0], self.values[2]] if folder == 'verified' else [self.values[1]]
                with patch.object(resolver, 'request', side_effect=responses) as request:
                    value = resolver.collect_body(TX) if folder == 'verified' else resolver.verified_header(ACCEPT)
                self.assertEqual(value['payload'] if folder == 'verified' else value, self.receipt['payload'] if folder == 'verified' else self.values[1])
                self.assertEqual(request.call_count, len(responses))
                self.assertEqual(path.read_bytes(), nested)
                self.assertFalse(any((resolver.root / 'quarantine').iterdir()))

    def test_native_refresh_bypasses_both_body_caches(self):
        resolver = self.resolver()
        self.worker.history.atomic_json(resolver.root / 'verified' / (TX + '.json.gz'), self.receipt)
        lookup = resolver.history_value

        def headers_only(folder, key):
            self.assertNotEqual(folder, 'verified', 'native rejected body must not reappear from another cache')
            return lookup(folder, key)

        with patch.object(resolver, 'history_value', side_effect=headers_only), \
             patch.object(resolver, 'request', side_effect=[self.values[0], self.values[2]]) as request:
            self.assertEqual(resolver.collect_body(TX, use_cache=False)['payload'], self.receipt['payload'])
        self.assertEqual(request.call_count, 2)


if __name__ == '__main__':
    unittest.main()
