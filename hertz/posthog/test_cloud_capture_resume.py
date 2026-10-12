import datetime as dt
import gzip
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from export_cloud_source import file_sha
from import_cloud_events import import_events


class FakeClient:
    sent = []
    fail = False
    drift = False
    mutex = threading.Lock()
    def __init__(self, credentials, environment, evidence):
        self.project = {'id': 101, 'token': 'different' if self.drift and threading.current_thread() is not threading.main_thread() else 'test'}
        self.config = {}
    def capture(self, events, historical):
        index = int(events[0]['uuid'].split('-')[-1])
        if self.fail and index == 100:
            raise OSError('temporary offline')
        with self.mutex:
            self.sent.append((index, historical))


class CaptureResumeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.prepared = self.root/'prepared';self.prepared.mkdir()
        source=self.prepared/'prod-events.ndjson.gz'
        with gzip.open(source,'wt') as stream:
            for index in range(250):
                stream.write(json.dumps({'uuid':f'00000000-0000-4000-8000-{index:012d}',
                    'event':'existing','distinct_id':'existing','timestamp':'2020-01-01T00:00:00Z',
                    'properties':{'value':0.10000000000000002}})+'\n')
        (self.prepared/'manifest.json').write_text(json.dumps({'state':'prepared_not_imported',
            'projects':{'prod':{'rows':250,'sha256':file_sha(source)}}}))
        self.plan=self.root/'plan.json';self.plan.write_text(json.dumps({'environment':'prod',
            'cloud_project_id':101,'source_manifest_sha256':file_sha(self.prepared/'manifest.json'),
            'restore_source_and_newer_live_person_properties':True}))
        self.evidence=self.root/'evidence';self.evidence.mkdir()
        FakeClient.sent=[];FakeClient.fail=False;FakeClient.drift=False

    def run_capture(self,workers):
        with patch('import_cloud_events.CloudClient',FakeClient):
            return import_events(self.prepared,'prod',self.root/'credentials',self.evidence,self.plan,True,workers)

    def test_parallel_failure_keeps_contiguous_checkpoint_and_reuses_later_receipts(self):
        FakeClient.fail=True
        with self.assertRaises(OSError):self.run_capture(4)
        state=json.loads((self.evidence/'capture-state.json').read_text())
        self.assertEqual(state['acknowledged_records'],100)
        sent_before=list(FakeClient.sent);self.assertTrue((self.evidence/'batch-000002.json').exists())
        FakeClient.fail=False
        result=self.run_capture(4)
        self.assertEqual(result['acknowledged_records'],250)
        self.assertEqual(sorted(index for index,_ in FakeClient.sent),[0,100,200])
        self.assertFalse(result['persisted_fields_verified'])
        self.assertFalse(result['person_profiles_verified'])
        self.assertTrue(all(historical for _,historical in FakeClient.sent))

    def test_destination_token_drift_is_rejected_before_any_capture_receipt(self):
        FakeClient.drift=True
        with self.assertRaises(RuntimeError):self.run_capture(4)
        self.assertEqual(FakeClient.sent,[])
        self.assertEqual(list(self.evidence.glob('batch-*.json')),[])
        self.assertEqual(json.loads((self.evidence/'capture-state.json').read_text())['acknowledged_records'],0)

    def test_resume_rejects_changed_receipt_and_source(self):
        self.run_capture(1)
        p=self.evidence/'batch-000001.json';receipt=json.loads(p.read_text());receipt['batch_sha256']='modified';p.write_text(json.dumps(receipt))
        with self.assertRaises(RuntimeError):self.run_capture(4)
        self.assertEqual(len(FakeClient.sent),3)
        with (self.prepared/'prod-events.ndjson.gz').open('ab') as f:f.write(b'changed')
        with self.assertRaises(RuntimeError):self.run_capture(1)


if __name__=='__main__':unittest.main()
