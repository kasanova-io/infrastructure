import datetime as dt
import hashlib
import json
import unittest
import uuid

from prepare_amplitude_import import RAW, SHA, USER, convert, session_key, session_uuid, time_us

class ImportContractTest(unittest.TestCase):
    def setUp(self):
        self.source={'app':1,'amplitude_id':10,'uuid':'00000000-0000-4000-8000-000000000001',
                     'event_type':'session_start','event_time':'2025-09-04 06:49:07.265123',
                     'session_id':1756968547265,'device_id':'device-A',
                     'event_properties':{'null':None,'nested':{'values':[True,42,1.25,'EMPTY']},'zero':0},
                     'user_properties':{'blank':'EMPTY','plan':'free'}}
        self.identity={'canonical_map':{'10':'11'},'user_ids':{'11':'existing-app-user-id'}}
    def native(self):
        raw=(json.dumps(self.source,separators=(',',':'))+'\n').encode()
        key=session_key(self.source)
        return raw,convert(raw,self.identity,{key:time_us(self.source['event_time'])})
    def test_preserves_session_start_source_bytes_and_all_typed_properties(self):
        raw,n=self.native()
        self.assertEqual(n['event'],'session_start')
        self.assertEqual(n['properties'][RAW].encode(),raw)
        self.assertEqual(n['properties'][SHA],hashlib.sha256(raw).hexdigest())
        for k,v in self.source['event_properties'].items():self.assertEqual(n['properties'][k],v)
        self.assertEqual(n['properties'][USER],self.source['user_properties'])
    def test_source_merge_map_and_utc_microseconds(self):
        _,n=self.native()
        self.assertEqual(n['distinct_id'],'existing-app-user-id')
        self.assertEqual(n['uuid'],self.source['uuid'])
        self.assertEqual(n['timestamp'],'2025-09-04T06:49:07.265123+00:00')
        self.assertEqual(time_us(n['timestamp']),time_us(self.source['event_time']))
    def test_session_v7_is_stable_and_not_after_first_event(self):
        k=session_key(self.source);first=time_us(self.source['event_time'])-10000000
        a=uuid.UUID(session_uuid(k,first))
        self.assertEqual(a.version,7)
        self.assertLessEqual(a.int>>80,first//1000)
        self.assertEqual(str(a),session_uuid(k,first))
        other=dict(self.source,device_id='another-device')
        self.assertNotEqual(str(a),session_uuid(session_key(other),first))
    def test_anonymous_canonical_ids_remain_separate(self):
        self.identity['user_ids']={}
        _,n=self.native();self.assertEqual(n['distinct_id'],'amplitude:1:11')
    def test_cannot_overwrite_source_properties_or_truncate_ids(self):
        self.source['event_properties'][RAW]='source-value'
        with self.assertRaises(ValueError):self.native()
        del self.source['event_properties'][RAW]
        self.identity['user_ids']['11']='x'*201
        with self.assertRaises(ValueError):self.native()
    def test_no_session_keeps_source_minus_one_and_omits_native_session(self):
        self.source['session_id']=-1
        _,n=self.native();self.assertNotIn('$session_id',n['properties'])
        self.assertEqual(json.loads(n['properties'][RAW])['session_id'],-1)

if __name__=='__main__':unittest.main()
