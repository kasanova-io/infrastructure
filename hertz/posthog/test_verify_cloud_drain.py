import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from verify_cloud_drain import verify


class CloudDrainTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = self.root / 'routes.json'
        self.config.write_text(json.dumps({'routes': {
            'fixture': {'project_id': 2, 'project_token': 'native-fixture',
                        'cloud': {'host': 'https://us.i.posthog.com',
                                  'project_id': 102, 'project_token': 'cloud-fixture'}}}}))
        self.journal = self.root / 'journal.sqlite'
        with sqlite3.connect(self.journal) as con:
            con.execute('CREATE TABLE records(project TEXT, cloud_destination TEXT)')

    def check_records(self, records):
        with sqlite3.connect(self.journal) as con:
            con.executemany('INSERT INTO records VALUES(?,?)', records)
        before = self.journal.read_bytes()
        result = verify(self.journal, self.config)
        self.assertEqual(before, self.journal.read_bytes())
        return result

    def test_only_the_configured_destination_counts_as_delivered(self):
        result = self.check_records([
            ('2', 'https://us.i.posthog.com:102'),
            ('2', None),
            ('2', 'https://us.i.posthog.com:999'),
            ('2', 'https://eu.i.posthog.com:102'),
        ])
        self.assertFalse(result['drained'])
        self.assertEqual(result['pending_or_wrong_destination'], 3)

    def test_unmapped_projects_prevent_retirement(self):
        result = self.check_records([('999', 'https://us.i.posthog.com:102')])
        self.assertFalse(result['drained'])
        self.assertEqual(result['unmapped_records'], 1)

    def test_bound_deliveries_and_empty_journal_are_drained(self):
        self.assertTrue(verify(self.journal, self.config)['drained'])
        self.assertTrue(self.check_records([('2', 'https://us.i.posthog.com:102')])['drained'])

    def test_missing_source_is_never_created(self):
        missing = self.root / 'missing.sqlite'
        with self.assertRaisesRegex(ValueError, 'Missing or aliased'):
            verify(missing, self.config)
        self.assertFalse(missing.exists())
