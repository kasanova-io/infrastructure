import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from analytics_ingress import Journal


class IngressTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.routes = {key: {'project_id': project, 'project_token': 'token-' + key,
                             'legacy_app': 734469 if project == 1 else 0}
                       for key, project in [('prod', 1), ('dev', 2)]}
        self.path = Path(self.tmp.name) / 'journal.sqlite'
        self.journal = Journal(self.path, self.routes)
        self.event = {'event_type': 'wallet_created', 'device_id': 'device-123',
                      'user_id': 'user-123', 'insert_id': 'original-insert-id',
                      'time': 1791662400123, 'session_id': 1791662400000,
                      'event_properties': {'large': 9007199254740991, 'decimal': 0.1,
                                           'nested': {'truth': True, 'nil': None}}}

    def test_restart_retry_preserves_fields_and_original(self):
        self.journal.accept({'api_key': 'prod', 'events': [self.event]})
        restarted = Journal(self.path, self.routes)
        retried = dict(self.event, attempts=10)
        restarted.accept({'api_key': 'prod', 'events': [retried]})
        with restarted.connect() as con:
            rows = con.execute('SELECT raw,native,sent FROM records').fetchall()
        self.assertEqual(len(rows), 1)
        original, event, sent = rows[0][0], json.loads(rows[0][1]), rows[0][2]
        self.assertEqual(json.loads(original), self.event)
        self.assertEqual(sent, 0)
        self.assertEqual(event['event'], self.event['event_type'])
        self.assertEqual(event['distinct_id'], 'user-123')
        for key, value in self.event['event_properties'].items():
            self.assertEqual(event['properties'][key], value)

    def test_environment_isolation_and_atomic_rejection(self):
        for key in self.routes:
            self.journal.accept({'api_key': key, 'events': [self.event]})
        with self.journal.connect() as con:
            self.assertEqual(con.execute('SELECT count(DISTINCT id) FROM records').fetchone()[0], 2)
        good = dict(self.event, insert_id='new-event')
        with self.assertRaises(ValueError):
            self.journal.accept({'api_key': 'prod', 'events': [good, {'event_type': 'bad'}]})
        self.assertEqual(self.journal.counts(), {0: 2})

    def test_failed_delivery_retains_journal(self):
        self.journal.accept({'api_key': 'dev', 'events': [self.event]})
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=OSError('offline')):
            with self.assertRaises(OSError):
                self.journal.deliver('http://posthog')
        self.assertEqual(self.journal.counts(), {0: 1})

    def test_replay_session_and_identify_set(self):
        event = copy.deepcopy(self.event)
        event['event_type'] = '$identify'
        event['extra'] = {'kasanova_posthog_session_id': '0197e7b4-c550-7000-8000-000000000001'}
        event['user_properties'] = {'$set': {'environment': 'PROD'}}
        self.journal.accept({'api_key': 'prod', 'events': [event]})
        with self.journal.connect() as con:
            native = json.loads(con.execute('SELECT native FROM records').fetchone()[0])
        self.assertEqual(native['properties']['$set'], {'environment': 'PROD'})
        self.assertEqual(native['properties']['$session_id'], event['extra']['kasanova_posthog_session_id'])


if __name__ == '__main__':
    unittest.main()
