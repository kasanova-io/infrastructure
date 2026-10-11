import unittest
from contextlib import closing
import json
import gzip
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from types import SimpleNamespace
import sqlite3
import tempfile
import shutil
from pathlib import Path

from analytics_ingress import Journal

from backup import ingestion_drained, parse_group_lags, journal_inventory, snapshot_journal, main, capacity_requirements, require_live_capacity, PRODUCTION_RESERVE_BYTES, owned_stack_ready


class ConsumerDrainTest(unittest.TestCase):
    def test_recognizes_all_group_offsets_including_nonzero_backlog(self):
        output = 'GROUP group1\nSTATE Stable\nTOTAL-LAG 0\n\nGROUP clickhouse-ingestion\nTOTAL-LAG 7\n'
        self.assertEqual(parse_group_lags(output, ['group1', 'clickhouse-ingestion']),
                         {'group1': 0, 'clickhouse-ingestion': 7})

    def test_missing_group_cannot_be_reported_as_drained(self):
        with self.assertRaises(RuntimeError):
            parse_group_lags('GROUP group1\nTOTAL-LAG 0\n', ['group1', 'clickhouse-ingestion'])

    def test_unknown_or_duplicate_offsets_cannot_be_reported_as_drained(self):
        for output in ['GROUP group1\nTOTAL-LAG -\n',
                       'GROUP group1\nTOTAL-LAG 0\nTOTAL-LAG 0\n',
                       'TOTAL-LAG 0\n', 'GROUP group1\nSTATE Empty\n']:
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                parse_group_lags(output, ['group1'])

    def test_preview_offsets_do_not_hide_storage_backlog(self):
        lags = {'group1': 0, 'clickhouse-ingestion': 0,
                'clickhouse-ingestion-historical': 0, 'livestream': 3}
        self.assertTrue(ingestion_drained(lags))
        lags['clickhouse-ingestion-historical'] = 1
        self.assertFalse(ingestion_drained(lags))
        lags['clickhouse-ingestion-historical'] = 0
        lags['unrecognized-new-consumer'] = 1
        self.assertFalse(ingestion_drained(lags))

    def test_missing_ingestion_group_cannot_be_reported_as_drained(self):
        with self.assertRaises(RuntimeError):
            ingestion_drained({'livestream': 3})


class LiveJournalBackupTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.source = self.path / 'ingress.sqlite'
        routes = {'dev': {'project_id': 2, 'project_token': 'test', 'legacy_app': 0}}
        journal = Journal(self.source, routes)
        self.writer = sqlite3.connect(self.source)
        self.addCleanup(self.writer.close)
        self.writer.execute('PRAGMA journal_mode=WAL')
        self.writer.execute('PRAGMA wal_autocheckpoint=0')
        for index in range(2):
            journal.accept({'api_key': 'dev', 'events': [{'event_type': 'backup_test',
                'device_id': 'existing-device', 'user_id': 'existing-user',
                'time': 1700000000123, 'insert_id': 'test-' + str(index),
                'event_properties': {'nested': {'value': 9007199254740991, 'boolean': True}}}]})
        self.writer.execute("UPDATE records SET sent=1 WHERE id=(SELECT id FROM records ORDER BY id LIMIT 1)")
        self.writer.execute("INSERT INTO history VALUES ('2','old-source')")
        self.writer.commit()

    def test_consolidates_committed_wal_and_restores_pending_original_rows(self):
        self.assertTrue(Path(str(self.source) + '-wal').exists())
        snapshot = self.path / 'snapshot.sqlite'
        expected = snapshot_journal(self.source, snapshot)
        self.assertEqual(expected['pending'], 1)
        self.assertEqual(expected['tables']['records']['rows'], 2)
        self.assertEqual(expected['tables']['identities']['rows'], 1)
        self.assertEqual(expected['tables']['history']['rows'], 1)
        restored = self.path / 'restored.sqlite'
        shutil.copyfile(snapshot, restored)
        self.assertEqual(journal_inventory(restored), expected)
        with closing(sqlite3.connect(restored)) as con:
            records = con.execute('SELECT raw,native,sent FROM records').fetchall()
        self.assertEqual(sorted(row[2] for row in records), [0, 1])
        for raw, native, _ in records:
            original, event = json.loads(raw), json.loads(native)
            self.assertEqual(original['event_properties']['nested']['value'], 9007199254740991)
            self.assertEqual(event['properties']['nested'], original['event_properties']['nested'])
        self.assertEqual(snapshot.stat().st_mode & 0o777, 0o600)

    def test_restore_detects_changed_envelope_or_pending_status(self):
        snapshot = self.path / 'snapshot.sqlite'
        expected = snapshot_journal(self.source, snapshot)
        with closing(sqlite3.connect(snapshot)) as con, con:
            con.execute("UPDATE records SET raw='changed',sent=1 WHERE sent=0")
        self.assertNotEqual(journal_inventory(snapshot), expected)

    def test_preserves_existing_snapshot_on_retry_and_rejects_other_schema(self):
        snapshot = self.path / 'snapshot.sqlite'
        snapshot_journal(self.source, snapshot)
        before = snapshot.read_bytes()
        with self.assertRaises(FileExistsError):
            snapshot_journal(self.source, snapshot)
        self.assertEqual(snapshot.read_bytes(), before)
        invalid = self.path / 'other.sqlite'
        with closing(sqlite3.connect(invalid)) as con, con:
            con.execute('CREATE TABLE unrelated(value TEXT)')
        with self.assertRaises(RuntimeError):
            journal_inventory(invalid)

    def test_live_scope_cannot_borrow_offline_import_certificate(self):
        with self.assertRaisesRegex(RuntimeError, 'mutually exclusive'):
            main(Path('offline.json'), live=True)


class LiveCapacityTest(unittest.TestCase):
    def test_same_filesystem_budgets_checkpoint_and_simultaneous_restore(self):
        gib = 1024**3
        self.assertEqual(capacity_requirements([(1, 10*gib), (1, 12*gib)]),
                         {1: 22*gib + PRODUCTION_RESERVE_BYTES})
        self.assertEqual(capacity_requirements([(1, 10*gib), (2, 12*gib)]),
                         {1: 10*gib + PRODUCTION_RESERVE_BYTES,
                          2: 12*gib + PRODUCTION_RESERVE_BYTES})

    def test_runtime_and_docker_volume_filesystems_are_distinct_allocations(self):
        gib = 1024**3
        self.assertEqual(capacity_requirements([(1, 11*gib), (2, 2*gib), (3, 12*gib)]),
                         {1: 11*gib + PRODUCTION_RESERVE_BYTES,
                          2: 2*gib + PRODUCTION_RESERVE_BYTES,
                          3: 12*gib + PRODUCTION_RESERVE_BYTES})
        self.assertEqual(capacity_requirements([(1, 11*gib), (2, 2*gib), (1, 12*gib)]),
                         {1: 23*gib + PRODUCTION_RESERVE_BYTES,
                          2: 2*gib + PRODUCTION_RESERVE_BYTES})

    def test_real_incompressible_archive_and_restore_are_both_reserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / 'incompressible'
            original.write_bytes(os.urandom(2*1024**2))
            archive = root / 'archive.gz'
            with original.open('rb') as src, gzip.open(archive, 'wb') as dst:
                shutil.copyfileobj(src, dst)
            with patch('backup.shutil.disk_usage', return_value=SimpleNamespace(free=100*1024**3)):
                plan = require_live_capacity(root / 'checkpoints', root / 'restores', root / 'docker', original.stat().st_size, 0, 0)
            self.assertTrue(plan['all_copies_share_filesystem'])
            self.assertGreaterEqual(plan['checkpoint_bytes_budgeted'], archive.stat().st_size)
            self.assertGreaterEqual(plan['restore_volume_bytes_budgeted'], original.stat().st_size)

    def test_old_single_copy_free_space_now_rejected_before_pause(self):
        gib = 1024**3
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch('backup.shutil.disk_usage', return_value=SimpleNamespace(free=int(12.5*gib))):
                with self.assertRaisesRegex(RuntimeError, 'checkpoint plus isolated restore'):
                    require_live_capacity(root, root / 'restores', root / 'docker', 10*gib, 0, 0)


class LiveRestartHealthTest(unittest.TestCase):
    def test_running_state_and_real_gateway_ingress_http_required(self):
        class Handler(BaseHTTPRequestHandler):
            ingress_code = 503
            paths = []
            def log_message(self, *args):
                pass
            def do_GET(self):
                self.paths.append(self.path)
                self.send_response(self.ingress_code if self.path.endswith('ingest/health') else 200)
                self.end_headers()
                self.wfile.write(b'{"status":"ok"}')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            record = {'Config': {'Labels': {'com.docker.compose.project': 'kasanova_posthog',
                       'com.docker.compose.service': 'analytics-ingress'}},
                      'State': {'Running': True, 'Restarting': False, 'OOMKilled': False}}
            host = 'http://127.0.0.1:' + str(server.server_port)
            with patch('backup.run', side_effect=lambda args: json.dumps([record]).encode()):
                self.assertFalse(owned_stack_ready('owned-ingress', host))
                Handler.ingress_code = 200
                self.assertTrue(owned_stack_ready('owned-ingress', host))
                self.assertIn('/_health', Handler.paths)
                self.assertIn('/kasanova-ingest/health', Handler.paths)
                record['State']['Restarting'] = True
                previous = list(Handler.paths)
                self.assertFalse(owned_stack_ready('owned-ingress', host))
                self.assertEqual(Handler.paths, previous)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == '__main__':
    unittest.main()
