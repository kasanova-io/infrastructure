import datetime as dt
import gzip
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from analytics_ingress import Journal
from cloud_reconciliation import export_cloud, verify_snapshot
from export_cloud_source import file_sha
from prepare_amplitude_import import time_us


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.snapshot = self.root / 'native'
        self.cloud = self.root / 'cloud'
        self.snapshot.mkdir()
        self.cloud.mkdir()
        self.routes = self.root / 'routes.json'
        self.routes.write_text(json.dumps({'routes': {'dev': {'project_id': 2, 'legacy_app': 'old-dev',
                                                          'project_token': 'old-token', 'environment': 'DEV',
                    'cloud': {'host': 'https://us.i.posthog.com', 'project_id': 102, 'project_token': 'cloud-dev'}}}}))
        journal = Journal(self.snapshot / 'ingress.sqlite', json.loads(self.routes.read_text())['routes'])
        journal.accept({'api_key': 'dev', 'events': [{'event_type': 'screen_viewed', 'device_id': 'same-device',
            'insert_id': 'same-source-id', 'time': 1791662400123,
            'event_properties': {'exact': [True, 1, None, 0.10000000000000002]}}]})
        with journal.connect() as con:
            self.event = json.loads(con.execute('SELECT native FROM records').fetchone()[0])
        row = {key: self.event[key] for key in ('uuid', 'event', 'distinct_id')}
        row.update(timestamp_us=time_us(self.event['timestamp']),
                   properties=json.dumps({**self.event['properties'], '$backend_added': 'source-only'}))
        with gzip.open(self.snapshot / 'dev-events.ndjson.gz', 'wt') as stream:
            stream.write(json.dumps(row) + '\n')
        files = {name: {'sha256': file_sha(self.snapshot / name)}
                 for name in ['dev-events.ndjson.gz', 'ingress.sqlite']}
        (self.snapshot / 'manifest.json').write_text(json.dumps({
            'state': 'source_export_complete', 'files': files,
            'projects': {'dev': {'rows': 1, 'source_project_id': 2}}}))
        self.record = [self.event['event'], self.event['distinct_id'], self.event['timestamp'], self.event['properties']]

    def seal_cloud(self, records, environment='dev'):
        path = self.cloud / 'events-000000.ndjson.gz'
        with gzip.open(path, 'wt') as stream:
            for row in records:
                stream.write(json.dumps(row) + '\n')
        (self.cloud / 'manifest.json').write_text(json.dumps({
            'state': 'cloud_export_complete', 'environment': environment, 'rows': len(records),
            'cloud_project_id': 102, 'api_host': 'https://us.posthog.com',
            'pages': [{'file': path.name, 'sha256': file_sha(path), 'rows': len(records)}]}))

    def test_exact_native_journal_wire_fields_pass_without_backend_metadata_or_writes(self):
        self.seal_cloud([[self.event['uuid'], self.record]])
        before = file_sha(self.snapshot / 'ingress.sqlite')
        result = verify_snapshot(self.snapshot, self.cloud, 'dev', self.routes, self.root / 'verified')
        self.assertEqual(result['state'], 'verified')
        self.assertEqual(result['matched_source_uuid_headers_timestamps_typed_properties_sessions'], 1)
        self.assertEqual(result['events_sent'], 0)
        self.assertEqual(file_sha(self.snapshot / 'ingress.sqlite'), before)

    def test_typed_change_and_missing_uuid_fail_without_tolerance(self):
        altered = json.loads(json.dumps(self.record))
        altered[3]['exact'][0] = 1
        self.seal_cloud([[self.event['uuid'], altered]])
        result = verify_snapshot(self.snapshot, self.cloud, 'dev', self.routes, self.root / 'failed')
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(result['field_mismatches'], 1)
        self.seal_cloud([])
        result = verify_snapshot(self.snapshot, self.cloud, 'dev', self.routes, self.root / 'missing')
        self.assertEqual(result['missing_or_different'], 1)

    def test_changed_export_or_wrong_environment_is_rejected(self):
        self.seal_cloud([[self.event['uuid'], self.record]], environment='prod')
        with self.assertRaisesRegex(RuntimeError, 'environment'):
            verify_snapshot(self.snapshot, self.cloud, 'dev', self.routes, self.root / 'wrong-env')
        self.seal_cloud([[self.event['uuid'], self.record]])
        with (self.cloud / 'events-000000.ndjson.gz').open('ab') as stream:
            stream.write(b'changed')
        with self.assertRaisesRegex(RuntimeError, 'Cloud page changed'):
            verify_snapshot(self.snapshot, self.cloud, 'dev', self.routes, self.root / 'changed')

    def test_cloud_export_keyset_is_stable_and_resumes_only_same_project(self):
        path = self.root / 'export'
        client = SimpleNamespace(project={'id': 123}, config={'api_host': 'https://us.posthog.com'}, query=unittest.mock.Mock(side_effect=[
            [[self.event['uuid'], self.record]], []]))
        with patch('cloud_reconciliation.CloudClient', return_value=client):
            result = export_cloud('not-read-in-unit-test', 'dev', path)
            self.assertEqual(result['rows'], 1)
            self.assertIn("toString(uuid) > '" + self.event['uuid'], client.query.call_args.args[0])
            client.query.reset_mock()
            self.assertEqual(export_cloud('not-read-in-unit-test', 'dev', path)['rows'], 1)
            client.query.assert_not_called()
            client.project = {'id': 456}
            with self.assertRaisesRegex(RuntimeError, 'destination changed'):
                export_cloud('not-read-in-unit-test', 'dev', path)

    def test_wrong_cloud_project_or_api_origin_cannot_certify_matching_fields(self):
        for key, value in [('cloud_project_id', 999999), ('api_host', 'https://eu.posthog.com')]:
            self.seal_cloud([[self.event['uuid'], self.record]])
            path = self.cloud / 'manifest.json'
            manifest = json.loads(path.read_text())
            manifest[key] = value
            path.write_text(json.dumps(manifest))
            with self.subTest(key=key), self.assertRaisesRegex(RuntimeError, 'project/origin'):
                verify_snapshot(self.snapshot, self.cloud, 'dev', self.routes, self.root / key)

    def test_mismatched_then_matching_duplicate_cannot_hide_a_field_failure(self):
        wrong = json.loads(json.dumps(self.record))
        wrong[3]['exact'][0] = 1
        self.seal_cloud([[self.event['uuid'], wrong], [self.event['uuid'], self.record]])
        with self.assertRaisesRegex(RuntimeError, 'repeated'):
            verify_snapshot(self.snapshot, self.cloud, 'dev', self.routes, self.root / 'duplicate')


if __name__ == '__main__':
    unittest.main()
