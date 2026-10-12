import copy
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analytics_ingress import Journal
from prepare_amplitude_import import convert


class HistoryJournalTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.routes={'prod':{'project_id':1,'project_token':'hertz','legacy_app':734469,
            'cloud':{'host':'https://us.i.posthog.com','project_id':101,'project_token':'cloud'}}}
        self.path=Path(self.temp.name)/'journal.sqlite';self.journal=Journal(self.path,self.routes)
        self.original={'uuid':'00000000-0000-4000-8000-000000000001','amplitude_id':1,'app':734469,
            '$insert_id':'original-retry-id','event_type':'wallet_created','event_time':'2020-01-01T01:00:00Z',
            'event_properties':{'value':0.10000000000000002,'nested':[True,None]},'user_properties':{},'device_id':'device','session_id':-1}
        self.native=convert(json.dumps(self.original).encode(),{'canonical_map':{'1':'1'},'user_ids':{'1':'wallet'}},{})
        self.sdk={'event_type':'wallet_created','device_id':'device','user_id':'wallet','insert_id':'original-retry-id',
            'time':1577840400000,'session_id':-1,'event_properties':self.original['event_properties']}

    def test_restart_outage_and_sdk_retry_have_one_durable_owner(self):
        self.assertEqual(self.journal.enqueue_history([self.native],1),[self.native])
        restarted=Journal(self.path,self.routes);restarted.accept({'api_key':'prod','events':[self.sdk]})
        self.assertEqual(restarted.counts(),{0:1})
        with patch('analytics_ingress.urllib.request.urlopen',side_effect=OSError('outage')):
            with self.assertRaises(OSError):restarted.deliver_cloud()
        self.assertEqual(restarted.counts(),{0:1})
        with restarted.connect() as con:
            raw,native=con.execute('SELECT raw,native FROM records').fetchone()
            self.assertEqual(json.loads(raw),self.original);self.assertEqual(json.loads(native),self.native)
            self.assertEqual(con.execute('SELECT count(*) FROM history').fetchone()[0],1)
        with patch('analytics_ingress.urllib.request.urlopen',return_value=io.BytesIO(b'{"status":1}')) as send:
            self.assertEqual(restarted.deliver_cloud(),1)
            self.assertTrue(json.loads(send.call_args.args[0].data)['historical_migration'])

    def test_previously_committed_sdk_wins_without_second_source_uuid(self):
        self.journal.accept({'api_key':'prod','events':[self.sdk]})
        queued=self.journal.enqueue_history([self.native],1)
        self.assertEqual(len(queued),1);self.assertNotEqual(queued[0]['uuid'],self.native['uuid'])
        self.assertEqual(self.journal.counts(),{0:1})
        with self.journal.connect() as con:self.assertEqual(con.execute('SELECT count(*) FROM history').fetchone()[0],0)

    def test_real_amplitude_timezone_free_utc_preserves_occurrence_and_raw_bytes(self):
        original={**self.original,'event_time':'2026-10-10 18:00:00.774000'}
        raw=json.dumps(original).encode()
        native=convert(raw,{'canonical_map':{'1':'1'},'user_ids':{'1':'wallet'}},{})
        self.assertEqual(self.journal.enqueue_history([native],1),[native])
        self.assertEqual(native['timestamp'],'2026-10-10T18:00:00.774000+00:00')
        with self.journal.connect() as con:
            stored=json.loads(con.execute('SELECT native FROM records').fetchone()[0])
            self.assertEqual(stored['properties']['__amplitude_original_record'],raw.decode())

    def test_timezone_free_utc_still_rejects_changed_occurrence(self):
        original={**self.original,'event_time':'2026-10-10 18:00:00.774000'}
        native=convert(json.dumps(original).encode(),{'canonical_map':{'1':'1'},'user_ids':{'1':'wallet'}},{})
        native['timestamp']='2026-10-10T18:00:00.775000+00:00'
        with self.assertRaisesRegex(ValueError,'occurrence timestamp differs'):
            self.journal.enqueue_history([native],1)
        self.assertEqual(self.journal.counts(),{})

    def test_second_invalid_record_rolls_back_native_ownership_and_retry_binding(self):
        bad=copy.deepcopy(self.native);bad['event']='wrong-name'
        with self.assertRaises(ValueError):self.journal.enqueue_history([self.native,bad],1)
        self.assertEqual(self.journal.counts(),{})
        with self.journal.connect() as con:
            self.assertEqual(con.execute('SELECT count(*) FROM history').fetchone()[0],0)
            self.assertEqual(con.execute('SELECT count(*) FROM identities').fetchone()[0],0)

    def test_current_and_historical_cloud_batches_keep_separate_ack_state(self):
        self.journal.enqueue_history([self.native],1)
        recent={**self.sdk,'insert_id':'recent','time':int(dt.datetime.now(dt.timezone.utc).timestamp()*1000)}
        self.journal.accept({'api_key':'prod','events':[recent]})
        def send(request,timeout):
            body=json.loads(request.data)
            if not body.get('historical_migration'):raise OSError('recent outage')
            return io.BytesIO(b'{"status":1}')
        with patch('analytics_ingress.urllib.request.urlopen',side_effect=send):
            with self.assertRaises(OSError):self.journal.deliver_cloud()
        with self.journal.connect() as con:
            self.assertEqual(con.execute('SELECT count(*) FROM records WHERE cloud_destination IS NOT NULL').fetchone()[0],1)
        with patch('analytics_ingress.urllib.request.urlopen',return_value=io.BytesIO(b'{"status":1}')) as send:
            self.assertEqual(self.journal.deliver_cloud(),1)
            self.assertNotIn('historical_migration',json.loads(send.call_args.args[0].data))

if __name__=='__main__':unittest.main()
