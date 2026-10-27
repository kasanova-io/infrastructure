from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from export_cloud_source import journal_export_program


class JournalExportTest(unittest.TestCase):
    def test_real_process_streams_consistent_wal_snapshot_with_original_rows_unchanged(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = root / 'original.sqlite'
            con = sqlite3.connect(original)
            try:
                con.execute('PRAGMA journal_mode=WAL')
                con.execute('CREATE TABLE records(id TEXT PRIMARY KEY,raw BLOB,sent INTEGER,cloud_destination TEXT)')
                con.execute('INSERT INTO records VALUES (?,?,?,?)', ('same-id', b'exact-original' * 200000, 0, 'same-cloud'))
                con.commit()
                self.assertTrue(Path(str(original) + '-wal').exists())
                exported = root / 'exported.sqlite'
                with exported.open('wb') as stream:
                    subprocess.run([sys.executable, '-c', journal_export_program(original)],
                                   stdout=stream, stderr=subprocess.PIPE, check=True, timeout=20)
                with sqlite3.connect(exported) as restored:
                    self.assertEqual(restored.execute('PRAGMA integrity_check').fetchall(), [('ok',)])
                    self.assertEqual(restored.execute('SELECT * FROM records').fetchall(),
                                     con.execute('SELECT * FROM records').fetchall())
            finally:
                con.close()

    def test_missing_source_is_never_created_or_reported_as_an_empty_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / 'absent.sqlite'
            result = subprocess.run([sys.executable, '-c', journal_export_program(source)],
                                    capture_output=True, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, b'')
            self.assertFalse(source.exists())


if __name__ == '__main__':
    unittest.main()
