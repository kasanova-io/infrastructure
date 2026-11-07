"""Separate caches must honor one bound archive cadence and Retry-After."""
import concurrent.futures
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error

spec = importlib.util.spec_from_file_location('shared_pacing_history', Path(__file__).parents[2] / 'social-indexer-history/history.py')
history = importlib.util.module_from_spec(spec); spec.loader.exec_module(history)
collect = history._chats


class Response:
    headers = {}
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def read(self, *_): return b'{}'


class SharedArchivePacingTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.archive = 'https://configured-archive.example'
        self.network = 'testnet-10'
        self.shared = self.make('shared', source='https://public-social.example')

    def make(self, name, source=None, *, shared=None, network=None, archive=None):
        h = history.History(self.root / name, source or self.archive, archive or self.archive,
                            network or self.network, shared_rate_root=shared)
        self.addCleanup(h.db.close)
        h.request_max_wait = 0.05
        h.request_options = {'attempts': 1, 'max_wait': 0.05, 'max_bytes': 1024}
        return h

    def test_two_caches_share_429_cooldown_even_after_an_unrelated_client_changes_environment(self):
        a = self.make('a', shared=self.shared.root)
        b = self.make('b', shared=self.shared.root)
        unrelated = self.make('unrelated')
        error = urllib.error.HTTPError(self.archive+'/info/network', 429, 'limited', {'Retry-After': '60'}, io.BytesIO())
        with patch.object(collect.urllib.request, 'urlopen', side_effect=error) as opening:
            with self.assertRaises(RuntimeError): a.request(self.archive+'/info/network')
            cooldown = float((self.shared.root/'archive-rate.txt').read_text())
            with patch.object(collect.time, 'time', return_value=time.time()+2):
                with self.assertRaises(RuntimeError): b.request(self.archive+'/transactions/known')
        self.assertEqual(opening.call_count, 1)
        self.assertGreater(cooldown, time.time()+50)
        self.assertEqual(float((self.shared.root/'archive-rate.txt').read_text()), cooldown)
        for h in (a,b,unrelated):
            self.assertFalse((h.root/'rate-details.txt').exists())

    def test_real_concurrent_clients_share_archive_slot_without_http_burst(self):
        a = self.make('a', shared=self.shared.root)
        b = self.make('b', shared=self.shared.root)
        barrier = threading.Barrier(2)
        def request(h):
            barrier.wait(timeout=2)
            try: return h.request(self.archive+'/info/network')
            except (collect.RatePending, RuntimeError): return 'deferred'
        # The optional inventory is read in each requesting thread with its own RO connection.
        with patch.object(collect.urllib.request, 'urlopen', return_value=Response()) as opening:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                outcomes=list(pool.map(request,[a,b]))
        self.assertEqual(outcomes.count({}),1)
        self.assertEqual(outcomes.count('deferred'),1)
        self.assertEqual(opening.call_count,1)

    def test_archive_equal_to_source_honors_configured_archive_interval(self):
        a=self.make('a',shared=self.shared.root)
        (self.shared.root/'archive-interval.txt').write_text('3.0')
        with patch.object(history.time,'time',return_value=100), patch.object(collect.urllib.request,'urlopen',return_value=Response()):
            a.request(self.archive+'/info/network')
        self.assertEqual(float((self.shared.root/'archive-rate.txt').read_text()),103)
        self.assertFalse((self.shared.root/'source-rate.txt').exists())
        self.assertFalse((a.root/'archive-rate.txt').exists())

    def test_actual_social_source_retains_its_separate_source_bucket(self):
        a=self.make('a',source='https://social.example',shared=self.shared.root)
        (self.shared.root/'archive-interval.txt').write_text('3.0')
        with patch.object(history.time,'time',return_value=100), patch.object(collect.urllib.request,'urlopen',return_value=Response()):
            a.request('https://social.example/get-posts-watching')
        self.assertEqual(float((self.shared.root/'source-rate.txt').read_text()),101)
        self.assertFalse((self.shared.root/'archive-rate.txt').exists())

    def test_configured_testnet_archive_cooldown_is_enforced(self):
        a=self.make('a',shared=self.shared.root)
        (self.shared.root/'archive-rate.txt').write_text(str(time.time()+60))
        with patch.object(collect.urllib.request,'urlopen',side_effect=AssertionError('cooldown must precede HTTP')):
            with self.assertRaises(RuntimeError): a.request(self.archive+'/info/network')

    def test_new_caller_cannot_share_another_network_or_archive_before_own_cache_creation(self):
        for change in [{'network':'mainnet'},{'network':'testnet-11'},{'archive':'https://other-archive.example'}]:
            with self.subTest(change=change):
                own=self.root/'must-not-exist'
                with self.assertRaisesRegex(RuntimeError,'archive/network mismatch'):
                    history.History(own,self.archive,change.get('archive',self.archive),change.get('network',self.network),shared_rate_root=self.shared.root)
                self.assertFalse(own.exists())

    def test_profile_rebinding_cannot_silently_change_pacing_network(self):
        a=self.make('a',shared=self.shared.root)
        self.shared.db.execute('UPDATE config SET value=? WHERE id=1',(json.dumps({'source':'https://public-social.example','archive':self.archive,'network':'testnet-11'}),));self.shared.db.commit()
        with patch.object(collect.urllib.request,'urlopen',side_effect=AssertionError('profile must precede HTTP')):
            with self.assertRaisesRegex(RuntimeError,'archive/network mismatch'):a.request(self.archive+'/info/network')
        self.assertFalse((self.shared.root/'archive-rate.txt').exists())

    def test_missing_or_symlinked_shared_inventory_does_not_create_a_new_pacing_scope(self):
        bad=self.root/'missing'
        for folder in [bad,self.root/'empty']:
            if folder.name=='empty':folder.mkdir()
            with self.assertRaisesRegex(RuntimeError,'existing ordinary inventory'):
                history.History(self.root/'own',self.archive,self.archive,self.network,shared_rate_root=folder)
            self.assertFalse((self.root/'own').exists())
        link=self.root/'linked';link.symlink_to(self.shared.root,target_is_directory=True)
        with self.assertRaises(RuntimeError):self.make('own',shared=link)
        self.assertFalse((self.root/'own').exists())

    def test_unconfigured_clients_keep_their_own_pacing_directory(self):
        a=self.make('a')
        self.make('unrelated')
        with patch.object(collect.urllib.request,'urlopen',return_value=Response()):a.request(self.archive+'/info/network')
        self.assertTrue((a.root/'archive-rate.txt').exists())
        self.assertFalse((a.root/'rate-details.txt').exists())
        self.assertFalse((self.shared.root/'archive-rate.txt').exists())

    def test_explicit_rate_directory_preserves_post_body_across_retry(self):
        body=b'{"transactionIds":["'+b'ab'*32+b'"]}'
        error=urllib.error.HTTPError(self.archive+'/transactions/search',503,'temporary',{},io.BytesIO())
        clock={'now':time.time()}
        def sleep(delay):clock['now']+=delay
        with patch.object(collect.time,'time',side_effect=lambda:clock['now']), patch.object(collect.time,'sleep',side_effect=sleep), patch.object(collect.urllib.request,'urlopen',side_effect=[error,Response()]) as opening:
            self.assertEqual(collect.request(self.archive+'/transactions/search',attempts=2,max_bytes=1024,data=body,
                                            rate_all_hosts=True,rate_root=self.shared.root),({},{}))
        self.assertEqual([c.args[0].data for c in opening.call_args_list],[body,body])
        self.assertTrue((self.shared.root/'rate-details.txt').exists())

    def test_profile_change_after_503_stops_the_next_retry_before_http_or_rate_write(self):
        a=self.make('a',shared=self.shared.root);a.request_options['attempts']=2
        def fail(*args,**kwargs):
            self.shared.db.execute('UPDATE config SET value=? WHERE id=1',(json.dumps({'archive':self.archive,'network':'testnet-11'}),));self.shared.db.commit()
            raise urllib.error.HTTPError(self.archive+'/info/network',503,'retry',{},io.BytesIO())
        with patch.object(collect.time,'sleep'),patch.object(collect.urllib.request,'urlopen',side_effect=fail) as opening:
            with self.assertRaisesRegex(RuntimeError,'archive/network mismatch'):a.request(self.archive+'/info/network')
        self.assertEqual(opening.call_count,1)
        self.assertLess(float((self.shared.root/'archive-rate.txt').read_text()),time.time()+3)

    def test_profile_change_during_429_closes_error_and_refuses_wrong_network_cooldown(self):
        a=self.make('a',shared=self.shared.root);error=urllib.error.HTTPError(self.archive+'/info/network',429,'limited',{'Retry-After':'60'},io.BytesIO())
        def fail(*args,**kwargs):
            self.shared.db.execute('UPDATE config SET value=? WHERE id=1',(json.dumps({'archive':self.archive,'network':'mainnet'}),));self.shared.db.commit()
            raise error
        with patch.object(collect.urllib.request,'urlopen',side_effect=fail) as opening:
            with self.assertRaisesRegex(RuntimeError,'archive/network mismatch'):a.request(self.archive+'/info/network')
        self.assertEqual(opening.call_count,1);self.assertTrue(error.fp.closed)
        self.assertLess(float((self.shared.root/'archive-rate.txt').read_text()),time.time()+3)

    def test_a_sleeping_waiter_does_not_block_a_bounded_429_cooldown_update(self):
        root=self.shared.root;path=root/'archive-rate.txt';path.write_text(str(time.time()+0.15))
        sleeping=threading.Event();release=threading.Event();actual_sleep=collect.wait_rate_delay
        def wait(delay,deadline):
            if delay>0:
                sleeping.set();release.wait(timeout=2)
                raise collect.RatePending('test waiter finished')
            return actual_sleep(delay,deadline)
        def background():
            collect.rate_slot(self.archive+'/blocks/x',rate_root=root,rate_bucket='archive',rate_interval=0.6)
        with patch.object(collect,'wait_rate_delay',side_effect=wait),concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(background)
            try:
                self.assertTrue(sleeping.wait(timeout=2))
                start=time.monotonic()
                collect.rate_slot(self.archive+'/info/network',cooldown=60,max_wait=0.05,rate_root=root,rate_bucket='archive')
                self.assertLess(time.monotonic()-start,0.1)
                self.assertGreater(float(path.read_text()),time.time()+59)
            finally:release.set()
            with self.assertRaises(collect.RatePending):future.result(timeout=2)

    def test_429_error_is_closed_even_when_cooldown_storage_fails(self):
        a=self.make('a',shared=self.shared.root);error=urllib.error.HTTPError(self.archive+'/info/network',429,'limited',{'Retry-After':'60'},io.BytesIO())
        original=collect.rate_slot
        def slot(*args,**kwargs):
            if kwargs.get('cooldown'):raise OSError('simulated cooldown storage failure')
            return original(*args,**kwargs)
        with patch.object(collect,'rate_slot',side_effect=slot),patch.object(collect.urllib.request,'urlopen',side_effect=error):
            with self.assertRaises(collect.PacingError):a.request(self.archive+'/info/network')
        self.assertTrue(error.fp.closed)

    def test_shared_archive_cadence_preserves_legacy_address_floor(self):
        a=self.make('a',shared=self.shared.root)
        (self.shared.root/'archive-interval.txt').write_text('0.4')
        with patch.object(history.time,'time',return_value=100),patch.object(collect.urllib.request,'urlopen',return_value=Response()):
            a.request(self.archive+'/addresses/example/full-transactions-page')
        self.assertEqual(float((self.shared.root/'archive-rate.txt').read_text()),102.1)

    def test_operational_pacing_failures_never_quarantine_a_discovered_transaction(self):
        spec=importlib.util.spec_from_file_location('archive_pacing_disposition',Path(__file__).parents[1]/'archive_backfill.py')
        archive=importlib.util.module_from_spec(spec);spec.loader.exec_module(archive)
        for name in ['profile','interval','counter','permission']:
            with self.subTest(name=name):
                shared=self.make('scope-'+name)
                h=archive.ArchiveHistory(self.root/('candidate-'+name),'https://social.example',self.archive,self.network,shared_rate_root=shared.root)
                self.addCleanup(h.db.close);h.request_options={'attempts':1,'max_wait':0.05}
                txid='ab'*32;h.db.execute('INSERT INTO records(txid) VALUES (?)',(txid,));h.db.commit()
                if name=='profile':
                    shared.db.execute('UPDATE config SET value=? WHERE id=1',(json.dumps({'archive':self.archive,'network':'mainnet'}),));shared.db.commit()
                elif name=='interval':(shared.root/'archive-interval.txt').write_text('not-a-number')
                elif name=='counter':(shared.root/'archive-rate.txt').write_text('not-a-number')
                original=Path.open
                def opening(path,*args,**kwargs):
                    if name=='permission' and path==shared.root/'archive-rate.txt':raise PermissionError('readonly pacing file')
                    return original(path,*args,**kwargs)
                with patch.object(Path,'open',opening),patch.object(collect.urllib.request,'urlopen',side_effect=AssertionError('no HTTP')) as http:
                    with self.assertRaises(RuntimeError) as failure:h.finish_verification(txid,lambda:h.request(self.archive+'/transactions/'+txid))
                    self.assertNotIsInstance(failure.exception,ValueError)
                    http.assert_not_called()
                self.assertEqual(h.db.execute('SELECT status,error FROM records WHERE txid=?',(txid,)).fetchone(),('discovered',None))
                self.assertFalse(list((h.root/'quarantine').iterdir()))

    def test_profile_rebind_while_sleeping_does_not_write_a_new_slot_or_send_http(self):
        a=self.make('a',shared=self.shared.root);a.request_max_wait=0.5;a.request_options['max_wait']=0.5
        path=self.shared.root/'archive-rate.txt';next_at=time.time()+0.05;path.write_text(str(next_at))
        original=collect.wait_rate_delay
        def wait(delay,deadline):
            if delay>0:
                self.shared.db.execute('UPDATE config SET value=? WHERE id=1',(json.dumps({'archive':self.archive,'network':'mainnet'}),));self.shared.db.commit()
            return original(delay,deadline)
        with patch.object(collect,'wait_rate_delay',side_effect=wait),patch.object(collect.urllib.request,'urlopen',side_effect=AssertionError('no HTTP')) as opening:
            with self.assertRaisesRegex(RuntimeError,'archive/network mismatch'):a.request(self.archive+'/info/network')
        self.assertEqual(float(path.read_text()),next_at);opening.assert_not_called()


if __name__=='__main__':unittest.main()
