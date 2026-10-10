import unittest
from contextlib import closing
import json
import sqlite3
import tempfile
import shutil
from pathlib import Path

from analytics_ingress import Journal

from backup import ingestion_drained, parse_group_lags, journal_inventory, snapshot_journal, main


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


if __name__ == '__main__':
    unittest.main()
