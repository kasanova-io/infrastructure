import datetime as dt
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import daily_refill


class DailyRefillTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.identity=self.root/'identity.json';self.identity.write_text(json.dumps({'canonical_map':{'1':'1'},'user_ids':{'1':'existing-wallet-id'},'missing_profile_ids':[]}))
        self.config={'amplitude':{'project_id':734469},'hertz_projects':{'prod':{'id':1},'dev':{'id':2}},'identity_map':str(self.identity)}

    def event(self,index,insert):
        return json.dumps({'uuid':f'00000000-0000-4000-8000-{index:012d}','amplitude_id':1,'app':734469,
            '$insert_id':insert,'event_type':'screen_viewed','event_time':'2026-10-10T18:00:00Z',
            'event_properties':{'decimal':0.10000000000000002,'nested':[True,None]},'user_properties':{},
            'device_id':'existing-device','session_id':1791658800000}).encode()

    def test_uuid_and_original_insert_id_dedup_preserves_existing_native_session(self):
        raw=[self.event(1,'a'),self.event(2,'already-owned-sdk'),self.event(3,'new')]
        source_uuid='00000000-0000-4000-8000-000000000001';native_session='019a1234-0000-7000-8000-000000000001'
        key=daily_refill.session_key(json.loads(raw[2]));archive=SimpleNamespace()
        with patch.object(daily_refill,'export_window',return_value=(archive,dt.datetime(2026,10,11,tzinfo=dt.timezone.utc))), \
             patch.object(daily_refill,'entries',return_value=iter(raw)), \
             patch.object(daily_refill,'extend_identity',side_effect=lambda identity,*args:identity), \
             patch.object(daily_refill,'source_ids',return_value=({source_uuid},{'already-owned-sdk'},{key:native_session})), \
             patch.object(daily_refill,'verify_hertz') as verify, \
             patch.object(daily_refill,'source_profile_snapshot',return_value=[]), \
             patch.object(daily_refill,'source_profile_catchup'):
            config={**self.config,'ingress_config':str(self.root/'ingress.json'),'journal':str(self.root/'journal.sqlite')}
            (self.root/'ingress.json').write_text(json.dumps({'routes':{'prod':{'project_id':1,'legacy_app':734469,'project_token':'fixture'}}}))
            _,count,_=daily_refill.refill_amplitude(config,{},self.root)
        self.assertEqual(count,1);event=verify.call_args.args[1][0]
        self.assertEqual(event['uuid'],'00000000-0000-4000-8000-000000000003')
        self.assertEqual(event['distinct_id'],'existing-wallet-id')
        self.assertEqual(event['properties']['$session_id'],native_session)
        self.assertEqual(event['properties']['decimal'],0.10000000000000002)
        self.assertEqual(event['properties']['__amplitude_original_record'],raw[2].decode())
        self.assertEqual(verify.call_count,1)

    def test_pending_journal_original_insert_is_deduplicated_before_hertz_delivery(self):
        import sqlite3
        journal=self.root/'journal.sqlite'
        from contextlib import closing
        with closing(sqlite3.connect(journal)) as con:
            con.executescript('CREATE TABLE records(project TEXT,id TEXT,raw TEXT,sent INTEGER);CREATE TABLE history(project TEXT,source_id TEXT);')
            con.execute('INSERT INTO records VALUES (?,?,?,0)',('1','sdk-native-uuid',json.dumps({'insert_id':'pending-sdk'})))
            con.execute('INSERT INTO history VALUES (?,?)',('1','already-imported'));con.commit()
        with patch.object(daily_refill,'source_rows',return_value=iter([])):
            uuids,inserts,_=daily_refill.source_ids(1,journal)
        self.assertIn('pending-sdk',inserts);self.assertIn('already-imported',inserts);self.assertIn('sdk-native-uuid',uuids)

    def test_nonempty_historical_person_snapshot_stops_before_any_capture(self):
        item=json.loads(self.event(3,'new'));item['user_properties']={'app_version':'old'};raw=[json.dumps(item).encode()]
        with patch.object(daily_refill,'export_window',return_value=(object(),dt.datetime.now(dt.timezone.utc))), \
             patch.object(daily_refill,'entries',return_value=iter(raw)), \
             patch.object(daily_refill,'extend_identity',side_effect=lambda identity,*args:identity), \
             patch.object(daily_refill,'source_ids',return_value=(set(),set(),{})), \
             patch.object(daily_refill,'Journal') as capture:
            with self.assertRaisesRegex(RuntimeError,'nonempty person snapshots'):
                daily_refill.refill_amplitude({**self.config,'journal':str(self.root/'journal.sqlite')},{},self.root)
        capture.assert_not_called()

    def test_existing_cloud_uuid_with_wrong_field_is_repaired_and_read_back(self):
        row={'uuid':'00000000-0000-4000-8000-000000000003','event':'screen_viewed','distinct_id':'wallet-id','timestamp_us':daily_refill.time_us('2026-10-10T18:00:00Z'),'properties':'{"value":0.1}'}
        class Client:
            project={'id':123};config={'api_host':'https://us.posthog.com'}
            def __init__(self,*args):self.record=['screen_viewed','wallet-id','2026-10-10T18:00:00Z',{'value':0.2}];self.captured=[]
            def query(self,sql):
                assert 'argMax' in sql and 'created_at' in sql
                return [[row['uuid'],self.record]]
            def capture(self,events,**kwargs):
                self.captured.extend(events);e=events[0];self.record=[e['event'],e['distinct_id'],e['timestamp'],e['properties']]
        client=Client();config={**self.config,'cloud_credentials':'private','native_delta_floor':'2026-10-10T00:00:00+00:00'}
        with patch.object(daily_refill,'CloudClient',return_value=client), \
             patch.object(daily_refill,'source_query',return_value=[row]), \
             patch.object(daily_refill,'HertzClient'), \
             patch.object(daily_refill,'source_people',return_value=[]), \
             patch.object(daily_refill,'cloud_persons',return_value=[]):
            result=daily_refill.cloud_gap(config,{'native_cutoff':'2026-10-10T00:00:00+00:00'},'prod',self.root,dt.datetime.now(dt.timezone.utc))
        self.assertEqual(result['existing_versions_repaired_and_verified'],1)
        self.assertEqual(result['missing_events_captured_and_verified'],0);self.assertEqual(len(client.captured),1)
        receipt=json.loads((self.root/'reconciliation-000000.json').read_text());self.assertTrue(receipt['persisted_source_wire_fields_verified'])

    def test_sdk_owned_native_payload_verifies_without_amplitude_envelope(self):
        sdk={'event_type':'screen_viewed','device_id':'device','user_id':'wallet',
            'insert_id':'retry','time':1791655200000,'event_properties':{'value':0.1},'session_id':-1}
        native=daily_refill.Journal.convert(sdk,json.dumps(sdk),'00000000-0000-4000-8000-000000000001','wallet',{'legacy_app':734469})
        row={'uuid':native['uuid'],'event':native['event'],'distinct_id':native['distinct_id'],'timestamp_us':str(daily_refill.time_us(native['timestamp'])),'properties':json.dumps(native['properties'])}
        with patch.object(daily_refill,'source_query',return_value=[row]):
            self.assertEqual(daily_refill.verify_hertz(1,[native]),[row])

    def test_faithful_live_sdk_event_is_not_replayed_for_source_backend_metadata(self):
        sdk={'event_type':'screen_viewed','device_id':'device','user_id':'wallet',
            'insert_id':'retry','time':1791655200000,'event_properties':{'value':0.1},'user_properties':{'$set':{'latest':'kept'}},'session_id':-1}
        native=daily_refill.Journal.convert(sdk,json.dumps(sdk),'00000000-0000-4000-8000-000000000001','wallet',{'legacy_app':734469})
        source_props={**native['properties'],'$ip':'source-backend','$transformations_succeeded':['source']}
        row={'uuid':native['uuid'],'event':native['event'],'distinct_id':native['distinct_id'],'timestamp_us':daily_refill.time_us(native['timestamp']),'properties':json.dumps(source_props)}
        ingress=self.root/'ingress.json';ingress.write_text(json.dumps({'routes':{'prod':{'project_id':1,'legacy_app':734469}}}))
        from unittest.mock import MagicMock
        client=MagicMock();client.query.return_value=[[native['uuid'],[native['event'],native['distinct_id'],native['timestamp'],{**native['properties'],'$ip':'cloud-backend'}]]]
        config={**self.config,'ingress_config':str(ingress),'cloud_credentials':'private','native_delta_floor':'2026-10-10T00:00:00+00:00'}
        with patch.object(daily_refill,'CloudClient',return_value=client),patch.object(daily_refill,'source_query',return_value=[row]):
            result=daily_refill.cloud_gap(config,{'native_cutoff':'2026-10-10T00:00:00+00:00'},'prod',self.root,dt.datetime.now(dt.timezone.utc))
        client.capture.assert_not_called();self.assertEqual(result['existing_versions_repaired_and_verified'],0)

    def test_failed_cloud_leg_does_not_advance_any_shared_watermark_or_identity_map(self):
        runtime=self.root/'runtime';(runtime/'operations').mkdir(parents=True)
        state_path=self.root/'state.json';state={'amplitude_project_id':734469,'amplitude_upload_cutoff':'2026-10-10T17:00:00+00:00','native_cutoff':'2026-10-12T01:00:00+00:00'}
        state_path.write_text(json.dumps(state));config={**self.config,'runtime':str(runtime),'state':str(state_path),'hertz_ingestion_host':'http://127.0.0.1:18084'};config_path=self.root/'config.json';config_path.write_text(json.dumps(config));before=self.identity.read_bytes()
        with patch.object(daily_refill,'export_window',return_value=(object(),dt.datetime(2026,10,12,tzinfo=dt.timezone.utc))), \
             patch.object(daily_refill,'refill_amplitude',return_value=(dt.datetime(2026,10,12,tzinfo=dt.timezone.utc),1,{'new':'not_committed'})), \
             patch.object(daily_refill,'cloud_gap',side_effect=[{'source_events_checked':1},RuntimeError('Cloud offline')]):
            with self.assertRaises(RuntimeError):daily_refill.run(config_path)
        self.assertEqual(json.loads(state_path.read_text()),state)
        self.assertEqual(self.identity.read_bytes(),before)


if __name__=='__main__':unittest.main()
