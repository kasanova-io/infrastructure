"""Actual PostgreSQL archive/live races; archive observations are explicit fixtures."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time

from protocol_indexer.archive import verify_archive
from protocol_indexer.backfill import AcceptanceObservation, import_receipt
from protocol_indexer.chats import EvidencePending
from protocol_indexer.journal import capture_one, connect
from protocol_indexer.routing import Family
from test_archive import evidence, INFO, TX, ACCEPT
from test_postgres import PostgresHarness, DSN


def receipt(payload=b'kchat:1:tip:key:sig:post:author:cipher'):
    return verify_archive(TX, 'testnet-10', INFO, *evidence(payload))


def observed(value, block=ACCEPT):
    return AcceptanceObservation('testnet-10', TX, value['payload'], block)


class BackfillTests(PostgresHarness):
    def initial_gap(self, value, timestamp=None):
        self.db.execute('UPDATE protocol_indexer.config SET activated_at_ms=20000')
        self.body(bytes.fromhex(value['transaction_id']), bytes.fromhex(value['payload']),
                  timestamp=value['block_time'] if timestamp is None else timestamp)
        self.assertTrue(capture_one(self.db))
        row = self.db.execute('SELECT * FROM protocol_indexer.canonical WHERE transaction_id=%s',
                              (bytes.fromhex(value['transaction_id']),)).fetchone()
        self.assertFalse(row['ever_accepted'])
        self.assertIsNone(row['accepting_block'])
        return row

    def assert_writer_blocked(self, pid, future):
        deadline = time.monotonic() + 2
        while not self.scalar('SELECT %s=ANY(pg_blocking_pids(%s))', (self.db.info.backend_pid, pid)):
            if future.done() or time.monotonic() > deadline:
                self.fail('competing live writer did not wait for importer lock')
            time.sleep(0.01)
        self.assertFalse(future.done())

    def test_all_six_families_use_same_quiet_journal_and_exact_binary_body(self):
        for payload in (b'kchat:1:post:key:sig:body:[]', b'kchat:1:handshake:\xff\x00:binary',
                        b'kchat:1:tip:key:sig:post:author:cipher', b'ciph_msg:1:comm:alias:\xff\x00',
                        b'k:1:reply:key:sig:post:body:[]', b'k:1:follow:key:sig:follow:author'):
            # Distinct explicit fixture ID without recreating a real chain tx.
            values = evidence(payload)
            txid = self.txid().hex()
            values[0]['transaction_id'] = txid
            values[2]['transactions'][0]['verboseData']['transactionId'] = txid
            value = verify_archive(txid, 'testnet-10', INFO, *values)
            observation = AcceptanceObservation('testnet-10', txid, payload.hex(), ACCEPT)
            self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observation), 'imported')
            row = self.db.execute('SELECT * FROM protocol_indexer.canonical WHERE transaction_id=%s', (bytes.fromhex(txid),)).fetchone()
            self.assertEqual(row['source_transaction']['payload'], '\\x' + payload.hex())
            self.assertEqual(row['source_transaction']['outputs'][0]['script_public_key_address'], 'author')
            self.assertEqual(row['accepting_block'], bytes.fromhex(ACCEPT))
            self.assertTrue(row['ever_accepted'])
        self.assertEqual(self.scalar('SELECT count(DISTINCT family) FROM protocol_indexer.canonical'), len(Family))
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions'), 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'), 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.pending'), 0)

    def test_duplicate_archive_cannot_resurrect_existing_revoked_canonical(self):
        value = receipt(); txid = bytes.fromhex(TX)
        import_receipt(self.db, value, 'testnet-10', lambda: observed(value))
        self.db.execute('SELECT protocol_indexer.remove_source_acceptances(%s)', ([bytes.fromhex(ACCEPT)],))
        self.assertTrue(capture_one(self.db))
        self.assertIsNone(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'))
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'existing')
        while capture_one(self.db): pass
        self.assertIsNone(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT revision FROM protocol_indexer.canonical'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)

    def test_initial_capture_gap_recovers_all_six_families_quietly_and_idempotently(self):
        for payload in (b'kchat:1:post:key:sig:body:[]', b'kchat:1:handshake:\xff\x00:binary',
                        b'kchat:1:tip:key:sig:post:author:cipher', b'ciph_msg:1:comm:alias:\xff\x00',
                        b'k:1:reply:key:sig:post:body:[]', b'k:1:follow:key:sig:follow:author'):
            values = evidence(payload); txid = self.txid().hex()
            values[0]['transaction_id'] = txid
            values[2]['transactions'][0]['verboseData']['transactionId'] = txid
            value = verify_archive(txid, 'testnet-10', INFO, *values)
            old = self.initial_gap(value)
            observation = AcceptanceObservation('testnet-10', txid, value['payload'], ACCEPT)
            self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observation), 'imported')
            row = self.db.execute('SELECT * FROM protocol_indexer.canonical WHERE transaction_id=%s',
                                  (bytes.fromhex(txid),)).fetchone()
            self.assertEqual(row['source_transaction'], old['source_transaction'])
            self.assertEqual(row['revision'], 2)
            self.assertTrue(row['ever_accepted'])
            self.assertEqual(row['accepting_block'], bytes.fromhex(ACCEPT))
            self.assertTrue(capture_one(self.db))
            self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observation), 'existing')
            self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar('SELECT count(DISTINCT family) FROM protocol_indexer.canonical'), len(Family))
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.evidence'), len(Family))
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'), 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical WHERE revision<>2'), 0)

    def test_initial_gap_keeps_raw_body_wakeup_and_current_source_fields(self):
        value = receipt(); old = self.initial_gap(value)
        self.db.execute('UPDATE transactions SET mass=42 WHERE transaction_id=%s', (bytes.fromhex(TX),))
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'imported')
        self.assertEqual(self.scalar('SELECT source_transaction FROM protocol_indexer.canonical'), old['source_transaction'])
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.pending'), 1)
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar("SELECT (source_transaction->>'mass')::bigint FROM protocol_indexer.canonical"), 42)
        self.assertEqual(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'), bytes.fromhex(ACCEPT))
        self.assertEqual(self.scalar('SELECT revision FROM protocol_indexer.canonical'), 3)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'), 0)

    def test_initial_gap_survives_raw_retention(self):
        value = receipt(); self.initial_gap(value)
        self.db.execute('DELETE FROM transactions WHERE transaction_id=%s', (bytes.fromhex(TX),))
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'imported')
        self.assertTrue(capture_one(self.db))
        self.assertTrue(self.scalar('SELECT ever_accepted FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions'), 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'), 0)

    def test_post_activation_source_or_archive_body_is_not_reconciled(self):
        value = receipt(); self.initial_gap(value, timestamp=20000)
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'existing')
        self.assertFalse(self.scalar('SELECT ever_accepted FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)
        self.db.execute("UPDATE protocol_indexer.canonical SET source_transaction=jsonb_set(source_transaction,'{block_time}','12345')")
        values = evidence(); values[2]['header']['timestamp'] = '20000'
        newer = verify_archive(TX, 'testnet-10', INFO, *values)
        self.assertEqual(import_receipt(self.db, newer, 'testnet-10', lambda: observed(newer)), 'existing')
        self.assertFalse(self.scalar('SELECT ever_accepted FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)

    def test_initial_gap_never_overrides_pending_live_removal(self):
        value = receipt(); self.initial_gap(value)
        self.accept(bytes.fromhex(TX), bytes.fromhex(ACCEPT))
        self.db.execute('DELETE FROM transactions_acceptances WHERE transaction_id=%s', (bytes.fromhex(TX),))
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'existing')
        self.assertTrue(capture_one(self.db))
        self.assertFalse(self.scalar('SELECT ever_accepted FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)

    def test_initial_gap_never_overrides_recorded_removed_block(self):
        value = receipt(); self.initial_gap(value)
        self.db.execute('SELECT protocol_indexer.remove_source_acceptances(%s)', ([bytes.fromhex(ACCEPT)],))
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'existing')
        self.assertFalse(self.scalar('SELECT ever_accepted FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)

    def test_initial_gap_requires_second_current_acceptance_read(self):
        value = receipt(); self.initial_gap(value)
        answers = iter([observed(value), observed(value, None)])
        with self.assertRaises(EvidencePending):
            import_receipt(self.db, value, 'testnet-10', lambda: next(answers))
        self.assertFalse(self.scalar('SELECT ever_accepted FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT revision FROM protocol_indexer.canonical'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.evidence'), 0)

    def test_live_removal_after_initial_gap_import_revokes_normally(self):
        value = receipt(); self.initial_gap(value); calls = 0; future = None
        ready = threading.Event(); writer_pid = []
        with ThreadPoolExecutor(max_workers=1) as pool:
            def observe():
                nonlocal calls, future
                calls += 1
                if calls == 2:
                    def remove():
                        with connect(DSN) as writer:
                            writer_pid.append(writer.info.backend_pid); ready.set()
                            return writer.execute('SELECT protocol_indexer.remove_source_acceptances(%s) AS count',
                                                  ([bytes.fromhex(ACCEPT)],)).fetchone()['count']
                    future = pool.submit(remove)
                    self.assertTrue(ready.wait(2))
                    self.assert_writer_blocked(writer_pid[0], future)
                return observed(value)
            self.assertEqual(import_receipt(self.db, value, 'testnet-10', observe), 'imported')
            self.assertEqual(future.result(timeout=3), 1)
        self.assertTrue(capture_one(self.db))
        self.assertIsNone(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'))
        self.assertTrue(self.scalar('SELECT ever_accepted FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'), 0)

    def test_live_raw_body_and_pending_revocation_win_before_capture(self):
        value = receipt(); txid = bytes.fromhex(TX)
        self.body(txid, bytes.fromhex(value['payload']))
        self.accept(txid)
        self.db.execute('DELETE FROM transactions_acceptances WHERE transaction_id=%s', (txid,))
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'live_pending')
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)
        self.assertTrue(capture_one(self.db))
        self.assertIsNone(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'))

    def test_pending_tombstone_without_body_is_not_overwritten_by_seed(self):
        value = receipt(); txid = bytes.fromhex(TX)
        self.accept(txid)
        self.db.execute('DELETE FROM transactions_acceptances WHERE transaction_id=%s', (txid,))
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'imported')
        self.assertIsNone(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)

    def test_block_removed_before_unknown_transaction_is_discovered_stays_revoked(self):
        value = receipt()
        self.db.execute('SELECT protocol_indexer.remove_source_acceptances(%s)', ([bytes.fromhex(ACCEPT)],))
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.pending'), 0)
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'imported')
        self.assertIsNone(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances'), 0)

    def test_current_live_acceptance_outweighs_archive_and_old_block_removal(self):
        value = receipt(); txid = bytes.fromhex(TX)
        self.db.execute('SELECT protocol_indexer.remove_source_acceptances(%s)', ([bytes.fromhex(ACCEPT)],))
        self.accept(txid, block=b'c' * 32)
        self.assertEqual(import_receipt(self.db, value, 'testnet-10', lambda: observed(value)), 'imported')
        self.assertEqual(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'), b'c' * 32)

    def test_fresh_archive_change_rolls_back_seed_body_and_trigger_scope(self):
        value = receipt()
        answers = iter([observed(value), observed(value, None)])
        with self.assertRaises(EvidencePending): import_receipt(self.db, value, 'testnet-10', lambda: next(answers))
        for table in ('transactions_acceptances', 'protocol_indexer.canonical', 'protocol_indexer.pending', 'protocol_indexer.acceptance_blocks'):
            self.assertEqual(self.scalar('SELECT count(*) FROM ' + table), 0)
        self.accept(bytes.fromhex(TX))
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.pending'), 1)

    def test_vcp_removal_waits_for_import_guard_then_revokes_durable_body(self):
        value = receipt(); calls = 0; future = None
        ready = threading.Event(); writer_pid = []
        with ThreadPoolExecutor(max_workers=1) as pool:
            def observe():
                nonlocal calls, future
                calls += 1
                if calls == 2:
                    def remove():
                        with connect(DSN) as writer:
                            writer_pid.append(writer.info.backend_pid); ready.set()
                            return writer.execute('SELECT protocol_indexer.remove_source_acceptances(%s) AS count', ([bytes.fromhex(ACCEPT)],)).fetchone()['count']
                    future = pool.submit(remove)
                    self.assertTrue(ready.wait(2))
                    self.assert_writer_blocked(writer_pid[0], future)
                return observed(value)
            self.assertEqual(import_receipt(self.db, value, 'testnet-10', observe), 'imported')
            self.assertEqual(future.result(timeout=3), 1)
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar('SELECT revision FROM protocol_indexer.canonical'), 2)
        self.assertIsNone(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'))
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'), 0)

    def test_concurrent_live_acceptance_b_replaces_archive_seed_a_after_wait(self):
        value = receipt(); calls = 0; future = None
        ready = threading.Event(); writer_pid = []
        with ThreadPoolExecutor(max_workers=1) as pool:
            def observe():
                nonlocal calls, future
                calls += 1
                if calls == 2:
                    def accept_b():
                        with connect(DSN) as writer:
                            writer_pid.append(writer.info.backend_pid); ready.set()
                            return writer.execute('SELECT protocol_indexer.insert_source_acceptances(%s,%s) AS count',
                                ([bytes.fromhex(TX)], [b'e' * 32])).fetchone()['count']
                    future = pool.submit(accept_b)
                    self.assertTrue(ready.wait(2))
                    self.assert_writer_blocked(writer_pid[0], future)
                return observed(value)  # Archive still reports A both times.
            self.assertEqual(import_receipt(self.db, value, 'testnet-10', observe), 'imported')
            self.assertEqual(future.result(timeout=3), 1)
        self.assertFalse(self.scalar('SELECT is_archive_seed FROM transactions_acceptances'))
        self.assertTrue(capture_one(self.db))
        self.assertEqual(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'), b'e' * 32)
        self.assertEqual(self.scalar('SELECT revision FROM protocol_indexer.canonical'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'), 0)

    def test_source_bulk_deduplicates_and_normal_live_conflicts_keep_upstream_semantics(self):
        txid = bytes.fromhex(TX)
        self.db.execute('SELECT protocol_indexer.insert_source_acceptances(%s,%s)', ([txid, txid], [b'a' * 32, b'b' * 32]))
        self.assertEqual(self.scalar('SELECT block_hash FROM transactions_acceptances'), b'a' * 32)
        self.db.execute('SELECT protocol_indexer.insert_source_acceptances(%s,%s)', ([txid], [b'c' * 32]))
        self.assertEqual(self.scalar('SELECT block_hash FROM transactions_acceptances'), b'a' * 32)
        self.db.execute('SELECT protocol_indexer.remove_source_acceptances(%s)', ([b'a' * 32],))
        self.db.execute('SELECT protocol_indexer.insert_source_acceptances(%s,%s)', ([txid], [b'c' * 32]))
        self.assertEqual(self.scalar('SELECT block_hash FROM transactions_acceptances'), b'c' * 32)

    def test_wrong_observation_identity_network_or_payload_cannot_import(self):
        value = receipt()
        for observation in (AcceptanceObservation('testnet-11', TX, value['payload'], ACCEPT),
                            AcceptanceObservation('testnet-10', 'ff' * 32, value['payload'], ACCEPT),
                            AcceptanceObservation('testnet-10', TX, '00', ACCEPT)):
            with self.assertRaises(ValueError): import_receipt(self.db, value, 'testnet-10', lambda: observation)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical'), 0)

    def test_configured_mainnet_history_uses_same_import_without_network_exclusion(self):
        values = evidence()
        values[0]['inputs'][0]['previous_outpoint_address'] = 'kaspa:sender'
        values[0]['outputs'][0]['script_public_key_address'] = 'kaspa:author'
        values[2]['transactions'][0]['outputs'][0]['verboseData']['scriptPublicKeyAddress'] = 'kaspa:author'
        value = verify_archive(TX, 'mainnet', {'networkName': 'kaspa-mainnet'}, *values)
        self.db.execute("UPDATE protocol_indexer.config SET network='mainnet'")
        observation = AcceptanceObservation('mainnet', TX, value['payload'], ACCEPT)
        self.assertEqual(import_receipt(self.db, value, 'mainnet', lambda: observation), 'imported')
        self.assertEqual(self.scalar('SELECT accepting_block FROM protocol_indexer.canonical'), bytes.fromhex(ACCEPT))
