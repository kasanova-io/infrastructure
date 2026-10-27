import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from native_people_archive import NativePeopleArchive
from daily_refill import profile_replay_preflight, verify_replay_memberships


class NativeArchiveTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.person = {'id': 'native', 'distinct_ids': ['device'], 'is_identified': False,
                       'created_at': '2026-10-10T00:00:00Z', 'properties': {'current': True}}
        raw = gzip.compress(json.dumps({'project': {'id': 2}, 'collections': {'persons': [self.person]}}).encode())
        (self.root / 'dev-metadata-final.json.gz').write_bytes(raw)
        self.manifest = {'state': 'final_native_tail_export_complete', 'public_native_detached': True,
                         'native_pipeline_drained': True, 'projects': {'dev': {'source_project_id': 2}},
                         'files': {'dev-metadata-final.json.gz': {'sha256': hashlib.sha256(raw).hexdigest()}}}

    def archive(self):
        raw = json.dumps(self.manifest).encode()
        (self.root / 'manifest.json').write_bytes(raw)
        return NativePeopleArchive(self.root, 'dev', hashlib.sha256(raw).hexdigest())

    def test_sealed_real_source_can_run_existing_guards_without_native_API(self):
        source = self.archive()
        cloud = {**self.person, 'id': 'cloud'}
        event = {'event': '$screen', 'distinct_id': 'device', 'timestamp': '2026-10-11T00:00:00Z', 'properties': {}}
        with patch('daily_refill.HertzClient', side_effect=AssertionError('retired native contacted')), \
                patch('daily_refill.cloud_persons', return_value=[cloud]):
            before = profile_replay_preflight(None, 2, [event], self.root, source_archive=source)
            self.assertEqual(before, {'cloud': cloud})
            self.assertEqual(verify_replay_memberships(None, 2, [event], self.root, source_archive=source), {'cloud': cloud})
            with self.assertRaisesRegex(RuntimeError, 'another project'):
                profile_replay_preflight(None, 1, [event], self.root, source_archive=source)

    def test_changed_partial_or_unbound_sources_fail_closed(self):
        source = self.archive()
        with self.assertRaisesRegex(RuntimeError, 'another project'):
            source.people(1)
        for key, value in [('state', 'exporting'), ('public_native_detached', False), ('native_pipeline_drained', False)]:
            original = self.manifest[key]
            self.manifest[key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.archive()
            self.manifest[key] = original
        with self.assertRaisesRegex(RuntimeError, 'manifest changed'):
            NativePeopleArchive(self.root, 'dev', '0' * 64)
        (self.root / 'dev-metadata-final.json.gz').write_bytes(b'changed')
        with self.assertRaisesRegex(RuntimeError, 'metadata changed'):
            self.archive()
