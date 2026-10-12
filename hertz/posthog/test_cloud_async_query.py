import io,json,tempfile,unittest,urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cloud_client import CloudClient

ID='0123456789abcdef0123456789abcdef'

class AsyncQueryTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.credentials=self.root/'cloud.json';self.credentials.write_text(json.dumps({'region':'us','api_host':'https://us.posthog.com','ingestion_host':'https://us.i.posthog.com','personal_api_key':'private-fixture','projects':{'prod':{'id':301455,'token':'fixture'},'dev':{'id':659455,'token':'fixture-dev'}}}))
        self.client=CloudClient(self.credentials,'prod',self.root/'evidence')
        self.uuid_patch=patch('cloud_client.uuid.uuid4',return_value=SimpleNamespace(hex=ID));self.uuid_patch.start();self.addCleanup(self.uuid_patch.stop)

    def status(self,complete=False,error=False,result=None,**extra):
        return {'query_status':{'id':ID,'team_id':301455,'complete':complete,'error':error,'results':result,**extra}}

    def test_pending_stale_inline_result_never_certifies_and_poll_does_not_resubmit(self):
        node={'kind':'HogQLQuery','query':'SELECT 1'};final={'results':[[1]],'columns':['one'],'clickhouse':'native compiled SQL'}
        first=self.status();first['results']=[['stale-cache']]
        with patch.object(self.client,'request',side_effect=[first,self.status(),self.status(True,result=final)]) as request,patch('cloud_client.time.sleep'):
            self.assertEqual(self.client.query_node(node),final)
        calls=request.call_args_list;self.assertEqual(len(calls),3)
        self.assertEqual(calls[0].args[1],{'query':node,'refresh':'force_async','client_query_id':ID})
        self.assertFalse(calls[0].kwargs['retry']);self.assertEqual(calls[0].kwargs['method'],'POST')
        self.assertTrue(all(c.args==('/api/projects/301455/query/'+ID+'/',) for c in calls[1:]))
        self.assertTrue(all(c.kwargs['timeout']==30 for c in calls))
        self.assertTrue((self.root/'evidence/queries'/ID/'success.json').exists())

    def test_wrong_or_unsafe_id_wrong_team_and_error_never_return_results(self):
        bad=[self.status(True,result={'results':[[1]]},id='../../other-project'),self.status(True,result={'results':[[1]]},id='different'),self.status(True,result={'results':[[1]]},team_id=1),self.status(True,True,{'results':[[1]]})]
        for index,response in enumerate(bad):
            client=CloudClient(self.credentials,'prod',self.root/f'case-{index}')
            with patch.object(client,'request',side_effect=[response,{'message':'cancelled'}]) as request,self.assertRaises(RuntimeError):
                client.query_node({'kind':'HogQLQuery','query':'SELECT 1'})
            self.assertEqual(request.call_count,2)
            self.assertEqual(request.call_args.args,('/api/projects/301455/query/'+ID+'/',))
            self.assertEqual(request.call_args.kwargs['method'],'DELETE')
            self.assertFalse((self.root/f'case-{index}'/'queries'/ID/'success.json').exists())

    def test_complete_flag_and_underlying_result_shape_are_required(self):
        for index,response in enumerate([self.status(True,result=None),self.status(True,result={'results':None}),self.status(True,result={'results':[[1]],'error':True}),self.status(complete=1,result={'results':[[1]]}),self.status(True,result={'results':[[1]],'query_status':{}}),self.status(True,result={'results':[[1]],'query_status':{'complete':None,'error':False}})]):
            client=CloudClient(self.credentials,'prod',self.root/f'invalid-{index}')
            with patch.object(client,'request',side_effect=[response,{}]),self.assertRaises(RuntimeError):client.query_node({'kind':'HogQLQuery','query':'SELECT 1'})

    def test_deadline_cancels_only_the_planned_job_and_never_submits_twice(self):
        clock=[0.0]
        def sleep(seconds):clock[0]+=seconds
        def response(path,*args,**kwargs):
            if kwargs.get('method')=='DELETE':return {'message':'cancelled'}
            return self.status()
        with patch('cloud_client.time.monotonic',side_effect=lambda:clock[0]),patch('cloud_client.time.sleep',side_effect=sleep),patch.object(self.client,'request',side_effect=response) as request,self.assertRaises(TimeoutError):
            self.client.query_node({'kind':'HogQLQuery','query':'SELECT 1'},deadline_seconds=60)
        self.assertEqual(sum(c.kwargs.get('method')=='POST' for c in request.call_args_list),1)
        self.assertEqual(request.call_args.kwargs['method'],'DELETE');self.assertLessEqual(clock[0],60)
        receipt=json.loads((self.root/'evidence/queries'/ID/'failure.json').read_text())
        self.assertTrue(receipt['no_completed_result_certified']);self.assertTrue(receipt['no_poll_resubmission'])

    def test_ambiguous_submission_is_not_retried_and_known_client_id_is_cancelled(self):
        with patch.object(self.client,'request',side_effect=[TimeoutError('ambiguous'),{}]) as request,self.assertRaises(TimeoutError):
            self.client.query_node({'kind':'HogQLQuery','query':'SELECT 1'})
        self.assertEqual(request.call_count,2);self.assertFalse(request.call_args_list[0].kwargs['retry']);self.assertEqual(request.call_args.kwargs['method'],'DELETE')

    def test_retry_sleep_and_http_timeout_fit_deadline(self):
        clock=[0.0];timeouts=[]
        def opener(request,timeout):
            timeouts.append(timeout)
            raise urllib.error.HTTPError(request.full_url,503,'busy',{'Retry-After':'600'},io.BytesIO(b'{}'))
        with patch('cloud_client.time.monotonic',side_effect=lambda:clock[0]),patch('cloud_client.time.sleep',side_effect=lambda seconds:clock.__setitem__(0,clock[0]+seconds)),patch.object(self.client.opener,'open',side_effect=opener),self.assertRaises(TimeoutError):
            self.client.request('/api/projects/301455/query/'+ID+'/',timeout=30,deadline=5)
        self.assertEqual(timeouts,[5]);self.assertEqual(clock[0],5)

    def test_body_stream_cannot_run_indefinitely_past_query_deadline(self):
        clock=[0.0]
        class Response:
            url='https://us.posthog.com/api/projects/301455/query/';status=200
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read1(self,size):clock[0]+=2;return b' '
        with patch('cloud_client.time.monotonic',side_effect=lambda:clock[0]),patch.object(self.client.opener,'open',return_value=Response()),self.assertRaises(TimeoutError):
            self.client.request('/api/projects/301455/query/',retry=False,timeout=30,deadline=5)
        self.assertLessEqual(clock[0],7)

    def test_existing_sql_api_delegates_and_returns_only_result_rows(self):
        with patch.object(self.client,'query_node',return_value={'results':[[1]],'columns':['one']}) as query:
            self.assertEqual(self.client.query('SELECT 1'),[[1]])
        query.assert_called_once_with({'kind':'HogQLQuery','query':'SELECT 1'})

    def test_streaming_http_error_body_is_also_deadline_bounded(self):
        clock=[0.0]
        class Stream:
            def read1(self,size):clock[0]+=2;return b' '
            def close(self):pass
        error=urllib.error.HTTPError('https://us.posthog.com/api/projects/301455/query/'+ID+'/',503,'busy',{},Stream())
        with patch('cloud_client.time.monotonic',side_effect=lambda:clock[0]),patch.object(self.client.opener,'open',side_effect=error),self.assertRaises(TimeoutError):
            self.client.request('/api/projects/301455/query/'+ID+'/',timeout=30,deadline=5)
        self.assertLessEqual(clock[0],7)

if __name__=='__main__':unittest.main()
