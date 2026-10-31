"""Bounded reused HTTP policy, explicit batch resume and preflight ordering."""
import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from protocol_indexer.archive import verify_archive
from protocol_indexer.backfill import AcceptanceObservation
from protocol_indexer.chats import EvidencePending
from test_archive import evidence, TX, ACCEPT, INFO
from test_postgres import PostgresHarness, DSN

spec = importlib.util.spec_from_file_location('archive_import_test', Path(__file__).parents[1] / 'import_archive.py')
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)


class CurrentObservationTests(unittest.TestCase):
    def resolver(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        resolver = module.ImportResolver(root, 'https://archive', 'https://archive', 'testnet-10')
        self.addCleanup(resolver.db.close)
        return resolver

    def test_bounded_lookup_attempts_once_and_preserves_cooldown_on_configured_archive(self):
        resolver = self.resolver()
        helper = module.Resolver.__mro__[1].request.__globals__['_chats']
        error = urllib.error.HTTPError('https://archive/transactions/' + TX, 429, 'limited', {'Retry-After': '60'}, io.BytesIO(b''))
        with patch.object(helper.urllib.request, 'urlopen', side_effect=error) as request, \
             patch.object(helper.time, 'sleep'):
            for _ in range(2):
                with self.assertRaises(EvidencePending): resolver.request('https://archive/transactions/' + TX)
            self.assertEqual(request.call_count, 1)
            self.assertEqual(request.call_args.kwargs['timeout'], 10)
        self.assertGreater(float((resolver.root / 'rate-details.txt').read_text()), time.time() + 50)

    def test_wrapper_cooldown_defers_without_waiting_or_making_http_call(self):
        resolver = self.resolver()
        helper = module.Resolver.__mro__[1].request.__globals__['_chats']
        (resolver.root / 'source-rate.txt').write_text(str(time.time() + 60))
        value = verify_archive(TX, 'testnet-10', INFO, *evidence())
        with patch.object(helper.urllib.request, 'urlopen', side_effect=AssertionError('cooldown must defer')):
            with self.assertRaises(EvidencePending): module.current_observation(resolver, value)

    def test_current_lookup_requires_exact_payload_id_and_explicit_acceptance_boolean(self):
        resolver = self.resolver()
        value = verify_archive(TX, 'testnet-10', INFO, *evidence())
        good = evidence()[0]
        for bad in ([], {**good, 'is_accepted': 1}, {**good, 'accepting_block_hash': None},
                    {**good, 'transaction_id': 'ff' * 32}, {**good, 'payload': '00'}):
            with patch.object(resolver, 'request', return_value=bad):
                with self.assertRaises((EvidencePending, ValueError)):
                    module.current_observation(resolver, value)
        with patch.object(resolver, 'request', return_value={**good, 'is_accepted': False}):
            self.assertIsNone(module.current_observation(resolver, value).accepting_block)

    def test_wall_deadline_interrupts_slow_body_and_redirect_to_slow_body(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/redirect':
                    self.send_response(302); self.send_header('Location', '/slow'); self.end_headers(); return
                self.send_response(200); self.end_headers()
                try:
                    for byte in b'{"value":"' + b'x' * 100 + b'"}':
                        self.wfile.write(bytes([byte])); self.wfile.flush(); time.sleep(0.02)
                except (BrokenPipeError, ConnectionResetError): pass
            def log_message(self, *_): pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            for path in ('/slow', '/redirect'):
                resolver = self.resolver()
                resolver.request_deadline_seconds = 0.1
                before = signal.getsignal(signal.SIGALRM)
                start = time.monotonic()
                with self.assertRaises(EvidencePending): resolver.request(f'http://127.0.0.1:{server.server_port}' + path)
                self.assertLess(time.monotonic() - start, 1)
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0, 0))
                self.assertEqual(signal.getsignal(signal.SIGALRM), before)
        finally:
            server.shutdown(); server.server_close(); thread.join()


class ImportCliTests(PostgresHarness):
    def test_source_gate_runs_before_cache_or_public_archive_access(self):
        with tempfile.TemporaryDirectory() as root:
            args = ['import_archive.py', '--network', 'testnet-10', '--archive', 'https://archive',
                    '--cache', root, '--batch', str(Path(root) / 'not-read.jsonl')]
            with patch.object(sys, 'argv', args), patch.dict(os.environ, {'PROTOCOL_DATABASE_URL': DSN}), \
                 patch.object(module, 'check_source', side_effect=ValueError('unhooked source')) as gate, \
                 patch.object(module, 'ImportResolver', side_effect=AssertionError('gate must run first')):
                with self.assertRaisesRegex(ValueError, 'unhooked'): module.main()
            self.assertTrue(gate.call_args.kwargs['require_archive_import'])

    def test_batch_limit_has_explicit_resume_cursor_and_never_replays_earlier_lines(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root); batch = root / 'batch.jsonl'
            receipts = []
            for _ in range(3):
                txid = self.txid().hex(); values = evidence()
                values[0]['transaction_id'] = txid
                values[2]['transactions'][0]['verboseData']['transactionId'] = txid
                receipts.append(verify_archive(txid, 'testnet-10', INFO, *values))
            batch.write_text(''.join(json.dumps(value) + '\n' for value in receipts))
            base = ['import_archive.py', '--network', 'testnet-10', '--archive', 'https://archive',
                    '--cache', str(root / 'cache'), '--batch', str(batch), '--limit', '1', '--after-line']
            def current(resolver, value):
                return AcceptanceObservation('testnet-10', value['transaction_id'], value['payload'], ACCEPT)
            for start, expected_cursor, ended in ((1, 2, False), (2, 3, True)):
                output = io.StringIO()
                with patch.object(sys, 'argv', base + [str(start)]), patch.dict(os.environ, {'PROTOCOL_DATABASE_URL': DSN}), \
                     patch.object(module, 'check_source'), patch.object(module.ImportResolver, 'refresh_network'), \
                     patch.object(module, 'current_observation', side_effect=current), contextlib.redirect_stdout(output):
                    module.main()
                summary = json.loads(output.getvalue().splitlines()[-1])
                self.assertEqual(summary['next_after_line'], expected_cursor)
                self.assertEqual(summary['end_of_batch'], ended)
                self.assertEqual(summary['counts']['imported'], 1)
            self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical'), 2)
            self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical WHERE transaction_id=%s',
                                         (bytes.fromhex(receipts[0]['transaction_id']),)), 0)

    def test_deferred_first_record_survives_cursor_and_imports_on_resume(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root); batch = root / 'batch.jsonl'; receipts = []
            for _ in range(2):
                txid = self.txid().hex(); values = evidence()
                values[0]['transaction_id'] = txid
                values[2]['transactions'][0]['verboseData']['transactionId'] = txid
                receipts.append(verify_archive(txid, 'testnet-10', INFO, *values))
            batch.write_text(''.join(json.dumps(value) + '\n' for value in receipts))
            args = ['import_archive.py', '--network', 'testnet-10', '--archive', 'https://archive',
                    '--cache', str(root / 'cache'), '--batch', str(batch), '--limit', '2', '--after-line']
            unavailable = True
            def current(resolver, value):
                if unavailable and value['transaction_id'] == receipts[0]['transaction_id']:
                    raise EvidencePending('transient archive outage')
                return AcceptanceObservation('testnet-10', value['transaction_id'], value['payload'], ACCEPT)
            with patch.dict(os.environ, {'PROTOCOL_DATABASE_URL': DSN}), patch.object(module, 'check_source'), \
                 patch.object(module.ImportResolver, 'refresh_network'), patch.object(module, 'current_observation', side_effect=current):
                output = io.StringIO()
                with patch.object(sys, 'argv', args + ['0']), contextlib.redirect_stdout(output):
                    with self.assertRaises(SystemExit) as exit_: module.main()
                    self.assertEqual(exit_.exception.code, 2)
                summary = json.loads(output.getvalue().splitlines()[-1])
                self.assertEqual((summary['next_after_line'], summary['pending']), (2, 1))
                self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical'), 1)
                self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.archive_jobs'), 1)
                # Explicit retry-time advance in this isolated test; no production policy bypass.
                self.db.execute('UPDATE protocol_indexer.archive_jobs SET available_at=clock_timestamp()')
                unavailable = False
                output = io.StringIO()
                with patch.object(sys, 'argv', args + ['2']), contextlib.redirect_stdout(output): module.main()
                summary = json.loads(output.getvalue().splitlines()[-1])
                self.assertEqual((summary['staged'], summary['pending'], summary['counts']['imported']), (0, 0, 1))
                self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical'), 2)
