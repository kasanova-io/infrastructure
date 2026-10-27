import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from analytics_ingress import Journal
from backup_cloud_journal import snapshot


class CloudBackupTest(unittest.TestCase):
    def test_live_wal_pending_records_identity_and_private_runtime_are_restorable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            routes = {'fixture': {'project_id': 2, 'project_token': 'native-fixture',
                      'cloud': {'host': 'https://us.i.posthog.com', 'project_id': 102,
                                'project_token': 'cloud-fixture'}}}
            config = root / 'private.json'
            config.write_text(json.dumps({'routes': routes}))
            runtime = root / 'runtime'
            runtime.mkdir()
            for name in ('analytics_ingress.py', 'native_cloud_proxy.py', 'compose.cloud.yaml',
                         'Caddyfile.cloud', '.env'):
                (runtime / name).write_text('fixture-' + name)
            source = root / 'journal.sqlite'
            journal = Journal(source, routes)
            journal.accept({'api_key': 'fixture', 'events': [{
                'event_type': 'backup-fixture', 'device_id': 'device', 'user_id': 'wallet',
                'insert_id': 'original', 'time': 1791662400123,
            }]})
            # Keep an actual WAL connection open across the online copy.
            connection = sqlite3.connect(source)
            try:
                output = root / 'snapshot'
                result = snapshot(source, config, runtime, output)
                self.assertEqual(result['state'], 'verified')
                self.assertTrue(result['independent_restore_verified'])
                self.assertEqual(result['journal']['pending'], 1)
                restored = Journal(output / 'ingress.sqlite', routes)
                with restored.connect() as con:
                    self.assertEqual(con.execute('SELECT sent,cloud_destination FROM records').fetchall(), [(0, None)])
                    self.assertEqual(con.execute('SELECT * FROM identities').fetchall(), [('2', 'device', 'wallet')])
                self.assertEqual((output / 'ingress.json').read_bytes(), config.read_bytes())
                self.assertEqual((output / 'ingress.json').stat().st_mode & 0o777, 0o600)
                with self.assertRaises(FileExistsError):
                    snapshot(source, config, runtime, output)
            finally:
                connection.close()

    def test_missing_source_is_not_created(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(RuntimeError, 'Missing or aliased'):
                snapshot(root / 'missing', root / 'config', root, root / 'out')
            self.assertFalse((root / 'missing').exists())


if __name__ == '__main__':
    unittest.main()
