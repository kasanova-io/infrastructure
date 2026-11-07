"""Coalescing keeps every post-lock observation fresh and every SQL import unchanged."""
from concurrent.futures import ThreadPoolExecutor
import contextlib
import copy
import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from protocol_indexer.archive import verify_archive
from protocol_indexer.backfill import deliver_one, stage_receipt
from protocol_indexer.backfill_batch import BatchedObservations
from protocol_indexer.chats import EvidencePending
from protocol_indexer.journal import connect
from test_archive import evidence, TX, ACCEPT, INFO
from test_postgres import PostgresHarness, DSN

spec=importlib.util.spec_from_file_location('batch_archive_test',Path(__file__).parents[1]/'import_archive.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class BatchQueueTests(unittest.TestCase):
    def test_every_second_callback_gets_a_new_lookup(self):
        calls=[]
        def fetch(values,budget):
            self.assertGreater(budget,0)
            calls.append([r['transaction_id'] for r in values])
            return {r['transaction_id']:len(calls) for r in values}
        observer=BatchedObservations(fetch,lambda r,v:v,window=.05)
        barrier=threading.Barrier(5)
        def run(index):
            barrier.wait()
            value={'transaction_id':str(index)}
            return observer.observe(value),observer.observe(value)
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs=[pool.submit(run,i) for i in range(4)];barrier.wait()
            try:observer.dispatch(jobs)
            finally:observer.close()
            answers=[job.result(timeout=1) for job in jobs]
        self.assertTrue(all(second>first for first,second in answers),answers)
        self.assertLess(len(calls),8,calls)
        self.assertFalse(observer.active)

    def test_request_enqueued_during_http_never_uses_that_inflight_response(self):
        started=threading.Event();calls=[];observer=None
        def fetch(values,budget):
            calls.append([r['transaction_id'] for r in values])
            if len(calls)==1:
                started.set();end=time.monotonic()+1
                while observer.pending.empty():
                    self.assertLess(time.monotonic(),end);time.sleep(.001)
            return {r['transaction_id']:len(calls) for r in values}
        observer=BatchedObservations(fetch,lambda r,v:v,window=0)
        value={'transaction_id':TX}
        def late():
            self.assertTrue(started.wait(1));return observer.observe(value)
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs=[pool.submit(observer.observe,value),pool.submit(late)]
            try:observer.dispatch(jobs)
            finally:observer.close()
            self.assertEqual([j.result() for j in jobs],[1,2])
        self.assertEqual(len(calls),2)

    def test_expired_result_is_not_delivered_and_future_race_does_not_crash_dispatch(self):
        def fetch(values,budget):
            time.sleep(.08);return {TX:'too late'}
        observer=BatchedObservations(fetch,lambda r,v:v,timeout=.04,window=0)
        with ThreadPoolExecutor(max_workers=1) as pool:
            job=pool.submit(observer.observe,{'transaction_id':TX})
            try:observer.dispatch([job])
            finally:observer.close()
            with self.assertRaises(EvidencePending):job.result()

    def test_close_releases_waiter_and_rejects_subsequent_callback(self):
        observer=BatchedObservations(lambda *_:self.fail('no network'),lambda r,v:v)
        with ThreadPoolExecutor(max_workers=1) as pool:
            job=pool.submit(observer.observe,{'transaction_id':TX});end=time.monotonic()+1
            while observer.pending.empty():
                self.assertLess(time.monotonic(),end);time.sleep(.001)
            observer.close()
            with self.assertRaises(EvidencePending):job.result(timeout=1)
        with self.assertRaises(EvidencePending):observer.observe({'transaction_id':TX})

    def test_expected_lookup_errors_release_all_waiters(self):
        for error in [EvidencePending('offline'),OSError('offline'),ValueError('bad response')]:
            with self.subTest(error=type(error).__name__):
                def fetch(*_):raise error
                observer=BatchedObservations(fetch,lambda r,v:v)
                with ThreadPoolExecutor(max_workers=2) as pool:
                    jobs=[pool.submit(observer.observe,{'transaction_id':str(i)}) for i in range(2)]
                    try:observer.dispatch(jobs)
                    finally:observer.close()
                    for job in jobs:
                        with self.assertRaises((EvidencePending,ValueError)):job.result(timeout=1)

    def test_dispatcher_cannot_move_bounded_signal_deadline_into_worker_thread(self):
        observer=BatchedObservations(lambda *_:{},lambda r,v:v)
        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.assertRaisesRegex(RuntimeError,'main-thread'):pool.submit(observer.dispatch,[]).result()


class BatchReadTests(unittest.TestCase):
    def setUp(self):
        root=self.enterContext(tempfile.TemporaryDirectory())
        self.resolver=module.ImportResolver(root,'https://source','https://archive','testnet-10')
        self.addCleanup(self.resolver.db.close)
        self.value=verify_archive(TX,'testnet-10',INFO,*evidence())

    def test_exact_id_mapping_handles_reversed_results_and_deduplicates_requested_ids(self):
        other=copy.deepcopy(self.value);other['transaction_id']='ab'*32
        rows=[{'transaction_id':other['transaction_id']},{'transaction_id':TX}]
        with patch.object(self.resolver,'request',return_value=rows) as request:
            result=module.current_observations(self.resolver,[self.value,other,self.value],2)
        self.assertEqual(set(result),{TX,other['transaction_id']})
        self.assertEqual(json.loads(request.call_args.kwargs['data']),{'transactionIds':sorted(result)})
        self.assertIn('/transactions/search?fields=transaction_id,payload,is_accepted,accepting_block_hash',request.call_args.args[0])
        self.assertEqual(self.resolver.request_deadline_seconds,12)

    def test_missing_id_is_unavailable_not_rejected(self):
        with patch.object(self.resolver,'request',return_value=[]):
            found=module.current_observations(self.resolver,[self.value],1)
        with self.assertRaises(EvidencePending):module.observation_from_detail('testnet-10',self.value,found.get(TX))

    def test_foreign_duplicate_or_nonobject_rows_fail_the_whole_lookup(self):
        for rows in [None,{},[None],[{'transaction_id':'ff'*32}],
                     [{'transaction_id':TX},{'transaction_id':TX}]]:
            with self.subTest(rows=rows),patch.object(self.resolver,'request',return_value=rows):
                with self.assertRaises(EvidencePending):module.current_observations(self.resolver,[self.value],1)

    def test_per_record_payload_and_acceptance_validation_is_the_existing_interpreter(self):
        good=evidence()[0]
        for bad in [[],{**good,'is_accepted':1},{**good,'payload':'00'},
                    {**good,'transaction_id':'ff'*32},{**good,'accepting_block_hash':None}]:
            with self.subTest(bad=bad):
                with self.assertRaises((EvidencePending,ValueError)):module.observation_from_detail('testnet-10',self.value,bad)
        rejected=module.observation_from_detail('testnet-10',self.value,{**good,'is_accepted':False})
        self.assertIsNone(rejected.accepting_block)

    def test_invalid_group_or_deadline_is_rejected_before_http(self):
        for values,budget in [([],1),([self.value],0),([self.value],13),([self.value],True),
                              ([{**self.value,'transaction_id':None}],1)]:
            with self.subTest(values=values,budget=budget),patch.object(self.resolver,'request',side_effect=AssertionError('no HTTP')):
                with self.assertRaises(ValueError):module.current_observations(self.resolver,values,budget)

    def test_documented_read_post_uses_shared_http_policy_and_exact_bounded_body(self):
        captured=[]
        value=self.value
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body=self.rfile.read(int(self.headers['Content-Length']))
                captured.append({'path':self.path,'body':json.loads(body),
                                 'content_type':self.headers.get('Content-Type'),
                                 'cache_control':self.headers.get('Cache-Control')})
                response=json.dumps([{'transaction_id':TX,'payload':value['payload'],
                                      'is_accepted':True,'accepting_block_hash':ACCEPT}]).encode()
                self.send_response(200);self.end_headers();self.wfile.write(response)
            def log_message(self,*_):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            self.resolver.archive=f'http://127.0.0.1:{server.server_port}'
            rows=module.current_observations(self.resolver,[value],2)
            observation=module.observation_from_detail('testnet-10',value,rows[TX])
            self.assertEqual(observation.accepting_block,ACCEPT)
            self.assertEqual(captured[0]['body'],{'transactionIds':[TX]})
            self.assertEqual(captured[0]['content_type'],'application/json')
            self.assertEqual(captured[0]['cache_control'],'no-cache')
        finally:
            server.shutdown();server.server_close();thread.join()

    def test_post_slow_stream_uses_remaining_wall_deadline_and_restores_policy(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(200);self.end_headers()
                try:
                    for byte in b'['+b' '*100+b']':
                        self.wfile.write(bytes([byte]));self.wfile.flush();time.sleep(.02)
                except (BrokenPipeError,ConnectionResetError):pass
            def log_message(self,*_):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            self.resolver.archive=f'http://127.0.0.1:{server.server_port}'
            start=time.monotonic()
            with self.assertRaises(EvidencePending):module.current_observations(self.resolver,[self.value],.1)
            self.assertLess(time.monotonic()-start,1)
            self.assertEqual(self.resolver.request_deadline_seconds,12)
        finally:
            server.shutdown();server.server_close();thread.join()


class BatchPostgresTests(PostgresHarness):
    def values(self,count=3):
        values=[]
        for index in range(count):
            txid=self.txid().hex();data=evidence()
            block=format(index+6000,'064x')
            data[0]['transaction_id']=txid;data[0]['accepting_block_hash']=block
            data[1]['verboseData']['hash']=block
            data[2]['transactions'][0]['verboseData']['transactionId']=txid
            value=verify_archive(txid,'testnet-10',INFO,*data)
            self.db.execute('INSERT INTO protocol_indexer.acceptance_blocks(block_hash) VALUES (%s)',(bytes.fromhex(block),))
            stage_receipt(self.db,'ab'*32,index+1,value,'testnet-10');values.append(value)
        return values

    def run_group(self,values,fetch):
        with contextlib.ExitStack() as stack:
            connections=[stack.enter_context(connect(DSN)) for _ in values]
            observer=BatchedObservations(fetch,lambda r,d:module.observation_from_detail('testnet-10',r,d),window=.05)
            with ThreadPoolExecutor(max_workers=len(connections)) as pool:
                jobs=[pool.submit(deliver_one,c,'ab'*32,'testnet-10',observer.observe) for c in connections]
                try:observer.dispatch(jobs)
                finally:observer.close()
                return [j.result(timeout=2) for j in jobs]

    def test_second_lookup_is_after_each_real_sentinel_lock_and_all_history_stays_quiet(self):
        values=self.values();calls={};locked=[]
        def fetch(requests,budget):
            rows={}
            for value in requests:
                txid=value['transaction_id'];calls[txid]=calls.get(txid,0)+1
                if calls[txid]==2:
                    import psycopg
                    with connect(DSN) as other:
                        try:
                            with other.transaction():
                                other.execute('SELECT block_hash FROM protocol_indexer.acceptance_blocks WHERE block_hash=%s FOR UPDATE NOWAIT',
                                              (bytes.fromhex(value['accepting_block']),)).fetchone()
                        except psycopg.errors.LockNotAvailable:
                            locked.append(txid)
                rows[txid]={'transaction_id':txid,'payload':value['payload'],'is_accepted':True,'accepting_block_hash':value['accepting_block']}
            return rows
        results=self.run_group(values,fetch)
        self.assertTrue(all(r['status']=='imported' for r in results))
        self.assertEqual(set(locked),{v['transaction_id'] for v in values})
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical'),len(values))
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.archive_jobs'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'),0)

    def test_changed_second_acceptance_defers_only_that_record_and_never_reuses_hint(self):
        values=self.values();bad=values[0]['transaction_id'];calls={}
        def fetch(requests,budget):
            result={}
            for value in requests:
                txid=value['transaction_id'];calls[txid]=calls.get(txid,0)+1
                accepted=not(txid==bad and calls[txid]==2)
                result[txid]={'transaction_id':txid,'payload':value['payload'],'is_accepted':accepted,
                             'accepting_block_hash':value['accepting_block'] if accepted else None}
            return result
        results=self.run_group(values,fetch)
        self.assertEqual(sum(r['status']=='deferred' for r in results),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical'),len(values)-1)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.canonical WHERE transaction_id=%s',(bytes.fromhex(bad),)),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM transactions_acceptances WHERE transaction_id=%s',(bytes.fromhex(bad),)),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.archive_jobs'),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM protocol_indexer.events WHERE notify_allowed'),0)
