import datetime as dt
import gzip
import json
from pathlib import Path
import tempfile
import unittest

from export_cloud_source import event_inventory, file_sha
from prepare_amplitude_import import EPOCH, time_us
from prepare_cloud_import import envelope, prepare


class CloudImportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        self.now = dt.datetime(2026, 10, 12, tzinfo=dt.timezone.utc)
        self.properties = {'number': 0.10000000000000002, 'large': 9007199254740991,
                           'nil': None, 'bool': True, '$lib': 'existing-library',
                           '$ip': '', '$session_id': '0197e7b4-c550-7000-8000-000000000001',
                           '__amplitude_original_record': '{"original":true}\n'}

    def row(self, stamp, uuid):
        return {'uuid': uuid, 'event': 'existing_event', 'distinct_id': 'existing-identity',
                'timestamp_us': str(time_us(stamp.isoformat())),
                'properties': json.dumps(self.properties)}

    def seal(self, rows):
        p = self.source / 'prod-events.ndjson.gz'
        with gzip.open(p, 'wt') as stream:
            for row in rows: stream.write(json.dumps(row) + '\n')
        receipt = {'state': 'source_export_complete', 'files': {p.name: {'sha256': file_sha(p)}},
                   'projects': {'prod': {'source_project_id': 1, 'rows': len(rows)}}}
        (self.source / 'manifest.json').write_text(json.dumps(receipt))

    def test_preserves_all_fields_and_48_hour_boundary_without_lib_override(self):
        boundary = self.now - dt.timedelta(hours=48)
        rows = [self.row(boundary - dt.timedelta(days=400), '00000000-0000-4000-8000-000000000001'),
                self.row(boundary, '00000000-0000-4000-8000-000000000002'),
                self.row(boundary + dt.timedelta(microseconds=1), '00000000-0000-4000-8000-000000000003')]
        self.seal(rows)
        output = self.root / 'prepared'
        receipt = prepare(self.source, output, self.now)
        self.assertEqual(receipt['projects']['prod']['historical_eligible'], 2)
        self.assertEqual(receipt['projects']['prod']['recent'], 1)
        with gzip.open(output / 'prod-events.ndjson.gz', 'rt') as stream:
            events = [json.loads(line) for line in stream]
        for expected, event in zip(rows, events):
            self.assertEqual(event, envelope(expected))
            self.assertEqual(event['properties'], self.properties)
            self.assertEqual(time_us(event['timestamp']), int(expected['timestamp_us']))

    def test_refuses_changed_snapshot_before_creating_destination(self):
        self.seal([self.row(self.now, '00000000-0000-4000-8000-000000000001')])
        with (self.source / 'prod-events.ndjson.gz').open('ab') as stream: stream.write(b'changed')
        with self.assertRaises(RuntimeError): prepare(self.source, self.root / 'prepared', self.now)
        self.assertFalse((self.root / 'prepared').exists())

    def test_duplicate_uuid_and_nonchronological_source_are_rejected(self):
        older = self.row(self.now - dt.timedelta(days=1), '00000000-0000-4000-8000-000000000001')
        newer = self.row(self.now, '00000000-0000-4000-8000-000000000002')
        for rows in [[older, older], [newer, older]]:
            self.seal(rows)
            with self.assertRaises(Exception): event_inventory(self.source / 'prod-events.ndjson.gz')

    def test_rejects_cloud_identity_truncation_and_preserves_zero_timestamp(self):
        row = self.row(EPOCH, '00000000-0000-4000-8000-000000000001')
        self.assertEqual(time_us(envelope(row)['timestamp']), 0)
        row['distinct_id'] = 'x' * 201
        with self.assertRaises(RuntimeError): envelope(row)


if __name__ == '__main__':
    unittest.main()
