"""Actual PostgreSQL queue/fence tests; public HTTP remains an explicit fixture."""

import contextlib
import importlib.util
import io
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import patch

import psycopg
from psycopg.types.json import Jsonb

from protocol_indexer.archive import bind_output_evidence, verify_archive
from protocol_indexer.backfill import source_body
from protocol_indexer.chats import EvidencePending
from protocol_indexer.evidence import CONSUMER, EvidenceConsumer
from protocol_indexer.journal import BackfillAdapter, capture_one, connect
from protocol_indexer.routing import Family
from test_archive import evidence, INFO, TX
from test_postgres import PostgresHarness, DSN


class EvidenceTests(PostgresHarness):
    def setUp(self):
        super().setUp()
        # Explicit native admission fixture. Original SDK admission is exercised
        # separately by the Rust/PostgreSQL contract and genuine chain sample.
        self.db.execute('DROP SCHEMA IF EXISTS social_shared CASCADE; CREATE SCHEMA social_shared')
        self.db.execute('''CREATE TABLE social_shared.config(singleton boolean PRIMARY KEY DEFAULT true,network text,source_id text);
            CREATE TABLE social_shared.actions(transaction_id bytea PRIMARY KEY,revision bigint,status text);
            INSERT INTO social_shared.config(network,source_id) SELECT network,journal_id::text FROM protocol_indexer.config''')

    def admit(self, txid):
        self.db.execute("UPDATE protocol_indexer.consumer_jobs SET last_error='verified_tip_output_pending:'||(SELECT revision::text FROM protocol_indexer.canonical WHERE transaction_id=%s) WHERE consumer='native-social-v1' AND transaction_id=%s", (txid, txid))

    def ingest(self, *args, **kwargs):
        txid = super().ingest(*args, **kwargs)
        self.admit(txid)
        return txid

    def consumer(self, resolve):
        identity = str(self.scalar('SELECT journal_id FROM protocol_indexer.config'))
        return EvidenceConsumer('testnet-10', identity, resolve)

    def jobs(self):
        return self.scalar('SELECT count(*) FROM protocol_indexer.consumer_jobs WHERE consumer=%s', (CONSUMER,))

    def test_live_and_inventory_queue_the_same_job_once(self):
        txid = self.ingest(b'kchat:1:tip:key:sig:post:author:cipher')
        self.assertEqual(self.jobs(), 1)
        self.db.execute('DELETE FROM protocol_indexer.consumer_jobs WHERE consumer=%s', (CONSUMER,))
        adapter = BackfillAdapter(Family.KACHAT_ACTIONS)
        adapter.run_page(self.db); adapter.run_page(self.db)
        self.assertEqual(self.jobs(), 1)
        self.assertEqual(self.scalar('SELECT transaction_id FROM protocol_indexer.consumer_jobs WHERE consumer=%s', (CONSUMER,)), txid)

    def test_proof_is_committed_and_canonical_lock_released_before_waking_native(self):
        txid = self.ingest(b'kchat:1:tip:key:sig:post:author:cipher')
        self.db.execute("UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp()+interval '1 hour' WHERE consumer='native-social-v1'")
        real_execute = self.db.execute
        def execute(query, *args, **kwargs):
            if query.lstrip().startswith('UPDATE protocol_indexer.consumer_jobs') and "WHERE consumer='native-social-v1'" in query:
                # A second native connection can see proof and lock canonical
                # at the exact post-commit wake boundary, without deadlocking.
                with connect(DSN) as native:
                    native.execute("SET lock_timeout='1s'")
                    with native.transaction():
                        self.assertEqual(native.execute('SELECT receipt FROM protocol_indexer.evidence WHERE transaction_id=%s', (txid,)).fetchone()['receipt'], {'fixture': True})
                        native.execute('SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=%s FOR UPDATE', (txid,))
            return real_execute(query, *args, **kwargs)
        def resolve(conn, row):
            conn.execute('INSERT INTO protocol_indexer.evidence VALUES (%s,%s,clock_timestamp())', (txid, Jsonb({'fixture': True})))
        with patch.object(self.db, 'execute', side_effect=execute):
            self.assertEqual(self.consumer(resolve).run_one(self.db), 'stored')
        self.assertEqual(self.jobs(), 0)
        self.assertTrue(self.scalar("SELECT available_at<=clock_timestamp() FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1'"))

    def test_deferred_evidence_keeps_job_and_next_ready_tip_progresses(self):
        first = self.ingest(b'kchat:1:tip:missing')
        second = self.ingest(b'kchat:1:tip:ready')
        seen = []
        def resolve(conn, row):
            if row['transaction_id'] == first:
                raise EvidencePending('missing')
            seen.append(row['transaction_id'])
        consumer = self.consumer(resolve)
        self.assertEqual(consumer.run_one(self.db), 'deferred')
        self.assertEqual(consumer.run_one(self.db), 'stored')
        self.assertEqual(seen, [second]); self.assertEqual(self.jobs(), 1)
        self.assertEqual(consumer.run_one(self.db), None)

    def test_revocation_during_http_commits_freely_and_retains_latest_job(self):
        txid = self.ingest(b'kchat:1:tip:fixture')
        def resolve(conn, row):
            def revoke():
                with connect(DSN) as writer:
                    writer.execute('DELETE FROM transactions_acceptances WHERE transaction_id=%s', (txid,))
                    self.assertTrue(capture_one(writer))
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(revoke).result(timeout=3)
        consumer = self.consumer(resolve)
        self.assertEqual(consumer.run_one(self.db), 'changed')
        self.assertEqual(self.jobs(), 1)
        consumer.resolve = lambda *_: self.fail('revoked transaction must not request output evidence')
        self.assertEqual(consumer.run_one(self.db), 'inactive')
        self.assertEqual(self.jobs(), 0)

    def test_acceptance_after_inactive_ack_requeues_the_tip(self):
        txid = self.ingest(b'kchat:1:tip:fixture')
        self.db.execute('DELETE FROM transactions_acceptances WHERE transaction_id=%s', (txid,))
        capture_one(self.db)
        self.assertEqual(self.consumer(lambda *_: self.fail('no HTTP')).run_one(self.db), 'inactive')
        self.accept(txid); capture_one(self.db); self.admit(txid)
        self.assertEqual(self.jobs(), 1)
        self.assertEqual(self.consumer(lambda *_: None).run_one(self.db), 'stored')

    def test_network_identity_and_outer_transaction_cannot_ack(self):
        self.ingest(b'kchat:1:tip:fixture')
        for network, source_id in [('testnet-11', 'unused'), ('testnet-10', 'another-journal')]:
            with self.assertRaises(ValueError):
                EvidenceConsumer(network, source_id, lambda *_: self.fail('no HTTP')).run_one(self.db)
        with self.db.transaction(), self.assertRaises(ValueError):
            self.consumer(lambda *_: self.fail('no HTTP')).run_one(self.db)
        self.assertEqual(self.jobs(), 1)

    def test_operational_sql_failure_rolls_back_without_ack(self):
        self.ingest(b'kchat:1:tip:fixture')
        def broken(conn, row):
            conn.execute('SELECT * FROM not_a_table')
        with self.assertRaises(psycopg.errors.UndefinedTable):
            self.consumer(broken).run_one(self.db)
        self.assertEqual(self.jobs(), 1)
        self.assertIsNone(self.scalar('SELECT last_error FROM protocol_indexer.consumer_jobs WHERE consumer=%s', (CONSUMER,)))

    def test_no_sdk_admission_or_stale_admission_never_calls_archive(self):
        txid = self.ingest(b'kchat:1:tip:fixture')
        consumer = self.consumer(lambda *_: self.fail('no archive before native admission'))
        for marker in (None, 'tip_evidence_or_target_pending', 'verified_tip_output_pending:0'):
            self.db.execute("UPDATE protocol_indexer.consumer_jobs SET last_error=%s WHERE consumer='native-social-v1'", (marker,))
            self.db.execute('UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp() WHERE consumer=%s', (CONSUMER,))
            self.assertEqual(consumer.run_one(self.db), 'deferred')
        self.assertEqual(self.jobs(), 1)

    def test_terminal_native_invalid_wrong_parent_and_already_materialized_need_no_http(self):
        for status in ('unsupported_or_invalid', 'verified', 'verified'):
            txid = self.ingest(b'kchat:1:tip:terminal')
            self.db.execute("DELETE FROM protocol_indexer.consumer_jobs WHERE consumer='native-social-v1' AND transaction_id=%s", (txid,))
            self.db.execute('INSERT INTO social_shared.actions VALUES(%s,1,%s)', (txid, status))
            self.assertEqual(self.consumer(lambda *_: self.fail('terminal native decision')).run_one(self.db), 'settled')
        self.assertEqual(self.jobs(), 0)

    def test_unready_or_mismatched_native_source_never_calls_archive(self):
        self.ingest(b'kchat:1:tip:fixture')
        consumer = self.consumer(lambda *_: self.fail('no archive'))
        self.db.execute("UPDATE social_shared.config SET source_id='another-source'")
        with self.assertRaisesRegex(ValueError, 'native source/network mismatch'):
            consumer.run_one(self.db)
        self.db.execute('DROP SCHEMA social_shared CASCADE')
        self.assertEqual(consumer.run_one(self.db), 'deferred')
        self.assertEqual(self.jobs(), 1)

    def test_cli_source_and_network_gate_precede_cache_and_http(self):
        import evidence_worker as worker
        with tempfile.TemporaryDirectory() as root:
            args = ['evidence_worker.py', '--network', 'testnet-10', '--archive', 'https://archive', '--cache', root, '--once']
            with patch.object(sys, 'argv', args), patch.dict(os.environ, {'PROTOCOL_DATABASE_URL': DSN}), \
                    patch.object(worker, 'ImportResolver', side_effect=AssertionError('no cache or HTTP')):
                with patch.object(worker, 'check_source', side_effect=ValueError('source mismatch')):
                    with self.assertRaisesRegex(ValueError, 'source mismatch'): worker.main()
                self.db.execute("UPDATE protocol_indexer.config SET network='testnet-11'")
                with patch.object(worker, 'check_source'):
                    with self.assertRaisesRegex(ValueError, 'journal network mismatch'): worker.main()

    def test_cli_once_reuses_bounded_resolver_and_closes_its_cache(self):
        import evidence_worker as worker
        from unittest.mock import MagicMock
        self.ingest(b'kchat:1:tip:fixture')
        with tempfile.TemporaryDirectory() as root:
            args = ['evidence_worker.py', '--network', 'testnet-10', '--archive', 'https://archive', '--cache', root, '--once']
            resolver = MagicMock()
            output = io.StringIO()
            with patch.object(sys, 'argv', args), patch.dict(os.environ, {'PROTOCOL_DATABASE_URL': DSN}), \
                    patch.object(worker, 'ImportResolver', return_value=resolver), patch.object(worker, 'check_source'), \
                    contextlib.redirect_stdout(output):
                worker.main()
            resolver.refresh_network.assert_called_once()
            resolver.resolve_outputs.assert_called_once()
            resolver.db.close.assert_called_once()
            self.assertIn('"status": "stored"', output.getvalue())
            self.assertEqual(self.jobs(), 0)


class OutputBindingTests(unittest.TestCase):
    def fixture(self):
        receipt = verify_archive(TX, 'testnet-10', INFO, *evidence())
        return {'transaction_id': bytes.fromhex(TX), 'source_transaction': source_body(receipt)}, receipt

    def test_full_versions_preserved_and_no_input0_or_funding_identity_required(self):
        row, receipt = self.fixture()
        receipt['rpc_transaction']['outputs'][0]['scriptPublicKey']['version'] = 65535
        row['source_transaction']['inputs'] = []
        self.assertIs(bind_output_evidence(row, receipt), receipt)
        self.assertNotIn('input0', receipt)

    def test_both_configured_networks_accept_only_matching_address_prefix(self):
        for network, prefix in [('mainnet', 'kaspa:'), ('testnet-10', 'kaspatest:')]:
            values = evidence()
            values[0]['inputs'][0]['previous_outpoint_address'] = prefix + 'funding'
            values[0]['outputs'][0]['script_public_key_address'] = prefix + 'author'
            values[2]['transactions'][0]['outputs'][0]['verboseData']['scriptPublicKeyAddress'] = prefix + 'author'
            receipt = verify_archive(TX, network, {'networkName': 'kaspa-' + network}, *values)
            row = {'transaction_id': bytes.fromhex(TX), 'source_transaction': source_body(receipt)}
            self.assertIs(bind_output_evidence(row, receipt), receipt)

    def test_cache_storage_failure_is_operational_and_not_evidence_pending(self):
        import chats_worker as worker
        with tempfile.TemporaryDirectory() as root:
            resolver = worker.Resolver(root, 'https://archive', 'https://archive', 'testnet-10')
            try:
                with patch.object(worker.history, 'atomic_json', side_effect=OSError('disk write')):
                    with self.assertRaisesRegex(RuntimeError, 'storage failure'):
                        resolver.save(resolver.root / 'quarantine' / 'fixture.json.gz', {})
            finally:
                resolver.db.close()

    def test_mismatched_outputs_indexes_amounts_scripts_and_identity_fail(self):
        for change in ('id', 'payload', 'count', 'index', 'bool-index', 'amount', 'bool-amount', 'script', 'address', 'version'):
            row, receipt = self.fixture()
            out = row['source_transaction']['outputs'][0]
            if change == 'id': row['transaction_id'] = b'x' * 32
            elif change == 'payload': row['source_transaction']['payload'] = '\\x00'
            elif change == 'count': row['source_transaction']['outputs'] = []
            elif change == 'index': out['index'] = 1
            elif change == 'bool-index': out['index'] = False
            elif change == 'amount': out['amount'] += 1
            elif change == 'bool-amount': out['amount'] = True
            elif change == 'script': out['script_public_key'] = '\\x00'
            elif change == 'address': out['script_public_key_address'] = 'kaspa:wrong'
            else: receipt['rpc_transaction']['outputs'][0]['scriptPublicKey']['version'] = True
            with self.subTest(change=change), self.assertRaises(ValueError):
                bind_output_evidence(row, receipt)

    def test_resolver_reuses_checked_database_proof_without_funding_or_http_lookup(self):
        row, receipt = self.fixture()
        # Public output proofs ignore any private input0 enrichment, including
        # one unusable by the old private sender-address projection.
        receipt['input0'] = {'unresolved_nonstandard_funding': True}
        spec = importlib.util.spec_from_file_location('output_worker_test', Path(__file__).parents[1] / 'chats_worker.py')
        worker = importlib.util.module_from_spec(spec); spec.loader.exec_module(worker)
        class Connection:
            def execute(self, sql, params):
                self.sql = sql
                self.params = params
                return self
            def fetchone(self): return {'receipt': receipt}
        with tempfile.TemporaryDirectory() as root:
            resolver = worker.Resolver(root, 'https://archive', 'https://archive', 'testnet-10')
            try:
                with patch.object(resolver, 'request', side_effect=AssertionError('unexpected HTTP')):
                    conn = Connection(); resolver.resolve_outputs(conn, row)
                self.assertIn('INSERT INTO protocol_indexer.evidence', conn.sql)
                self.assertNotIn('input0', conn.params[1].obj)
            finally:
                resolver.db.close()

    def test_cold_output_proof_fetch_does_not_request_prior_input_or_current_accepting_header(self):
        from chats_worker import Resolver
        row, receipt = self.fixture()
        values = evidence()
        # A full body with nonstandard version is valid receipt evidence; the
        # native payment verifier will independently decide whether it pays.
        values[2]['transactions'][0]['outputs'][0]['scriptPublicKey']['version'] = 42
        receipt = verify_archive(TX, 'testnet-10', INFO, *values)
        row['accepting_block'] = b'z' * 32
        class Connection:
            def execute(self, sql, params):
                self.sql, self.params = sql, params
                return self
            def fetchone(self): return None
        with tempfile.TemporaryDirectory() as root:
            resolver = Resolver(root, 'https://archive', 'https://archive', 'testnet-10')
            resolver.network_info = INFO
            try:
                with patch.object(resolver, 'request', side_effect=values) as request:
                    conn = Connection(); resolver.resolve_outputs(conn, row)
                self.assertEqual(request.call_count, 3)
                self.assertEqual(request.call_args_list[0].args[0], 'https://archive/transactions/' + TX + '?resolve_previous_outpoints=light')
                stored = conn.params[1].obj
                self.assertNotIn('input0', stored)
                self.assertEqual(stored['rpc_transaction']['outputs'][0]['scriptPublicKey']['version'], 42)
            finally:
                resolver.db.close()
