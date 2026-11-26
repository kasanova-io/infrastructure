"""Network outages cannot admit work with stale network evidence or crash-loop."""
import contextlib
import io
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import chats_worker
import evidence_worker as worker
import import_archive
from protocol_indexer.chats import EvidencePending


class EvidenceWorkerTests(unittest.TestCase):
    def exercise(self, *, once=False, periodic=False, fatal=None, stop_pending=False):
        with tempfile.TemporaryDirectory() as root:
            events=[]; handlers={}; now=[0]; checks=[0]
            connection=Mock()
            connection.execute.return_value.fetchone.return_value={'network':'testnet-10','id':'source'}
            resolver=Mock()
            def refresh():
                checks[0]+=1; events.append('network')
                if fatal:raise fatal
                if checks[0]==(2 if periodic else 1):raise EvidencePending('archive_request_exhausted')
            resolver.refresh_network.side_effect=refresh
            consumer=Mock()
            def consume(*_):
                events.append('consume')
                if periodic and consumer.run_one.call_count==1:now[0]=65
                else:handlers[signal.SIGTERM](None,None)
            consumer.run_one.side_effect=consume
            stopped=Mock()
            def wait(seconds):
                events.append(('wait',seconds))
                if stop_pending:handlers[signal.SIGTERM](None,None)
            stopped.wait.side_effect=wait
            args=['evidence_worker.py','--network','testnet-10','--archive','https://archive','--cache',root]+(['--once'] if once else [])
            with patch.object(sys,'argv',args), patch.dict(os.environ,{'PROTOCOL_DATABASE_URL':'unused'}), \
                 patch.object(worker,'connect',side_effect=lambda *_:contextlib.nullcontext(connection)), \
                 patch.object(worker,'check_source',side_effect=lambda *_:events.append('source')), \
                 patch.object(worker,'ImportResolver',return_value=resolver), \
                 patch.object(worker,'EvidenceConsumer',return_value=consumer), \
                 patch.object(worker.signal,'signal',side_effect=lambda s,h:handlers.__setitem__(s,h)), \
                 patch.object(worker.threading,'Event',return_value=stopped), \
                 patch.object(worker.time,'monotonic',side_effect=lambda:now[0]), contextlib.redirect_stdout(io.StringIO()):
                if once:
                    with self.assertRaises(SystemExit) as result:worker.main()
                    self.assertEqual(result.exception.code,2)
                elif fatal:
                    with self.assertRaises(type(fatal)):worker.main()
                else:worker.main()
            resolver.db.close.assert_called_once()
            return events,consumer,stopped

    def test_startup_outage_rechecks_source_and_network_before_first_admission(self):
        events,consumer,_=self.exercise()
        self.assertEqual(events,['source','source','network',('wait',5),'source','network','consume'])
        self.assertEqual(consumer.run_one.call_count,1)

    def test_expired_network_check_does_not_consume_during_outage(self):
        events,consumer,_=self.exercise(periodic=True)
        self.assertEqual(events.count('consume'),2)
        first=events.index(('wait',5))
        self.assertEqual(events[first:first+4],[('wait',5),'source','network','consume'])

    def test_once_reports_pending_without_admitting_or_acknowledging(self):
        _,consumer,_=self.exercise(once=True)
        consumer.run_one.assert_not_called()

    def test_shutdown_interrupts_pending_wait_without_admission(self):
        _,consumer,stopped=self.exercise(stop_pending=True)
        consumer.run_one.assert_not_called();stopped.set.assert_called_once()

    def test_network_mismatch_and_operational_failure_remain_fatal(self):
        for error in [ValueError('wrong network'),PermissionError('storage inaccessible')]:
            with self.subTest(error=type(error).__name__):
                _,consumer,_=self.exercise(fatal=error)
                consumer.run_one.assert_not_called()


class BoundedRefreshTests(unittest.TestCase):
    def test_busy_history_cache_at_startup_and_during_lookup(self):
        with tempfile.TemporaryDirectory() as root:
            shared = Path(root) / 'history'
            shared.mkdir()
            with contextlib.closing(sqlite3.connect(shared / 'inventory.sqlite')) as db:
                db.execute('CREATE TABLE config (id INTEGER PRIMARY KEY, value TEXT)')
                db.executemany('INSERT INTO config VALUES (?, ?)', [
                    (1, '{"archive":"https://archive","network":"testnet-10"}'),
                    (3, 'six-protocol-archive-evidence-v1')])
                db.commit()
                db.execute('BEGIN EXCLUSIVE')
                args = SimpleNamespace(cache=Path(root) / 'own', archive='https://archive', network='testnet-10',
                                       history_cache=shared, shared_rate_root=shared, once=False)
                stopped = Mock()
                stopped.wait.side_effect = lambda seconds: db.rollback()
                connect = sqlite3.connect
                def immediate(*args, **kwargs):
                    kwargs['timeout'] = 0
                    return connect(*args, **kwargs)
                with patch.object(sqlite3, 'connect', side_effect=immediate):
                    resolver = chats_worker.open_worker_resolver(chats_worker.Resolver, args, stopped)
                    with contextlib.closing(resolver.db):
                        stopped.wait.assert_called_once_with(5)
                        db.execute('BEGIN EXCLUSIVE')
                        with self.assertRaisesRegex(EvidencePending, 'archive_inventory_busy'):
                            resolver.history_value('blocks', 'ab' * 32)
                        db.rollback()
                        self.assertIsNone(resolver.history_value('blocks', 'ab' * 32))

    def test_evidence_startup_retries_wrapped_rate_inventory_lock(self):
        with tempfile.TemporaryDirectory() as root:
            shared = Path(root) / 'rate'
            shared.mkdir()
            with contextlib.closing(sqlite3.connect(shared / 'inventory.sqlite')) as db:
                db.execute('CREATE TABLE config (id INTEGER PRIMARY KEY, value TEXT)')
                db.execute('INSERT INTO config VALUES (1, ?)', ('{"archive":"https://archive","network":"testnet-10"}',))
                db.commit()
                db.execute('BEGIN EXCLUSIVE')
                args = SimpleNamespace(cache=Path(root) / 'own', archive='https://archive', network='testnet-10',
                                       history_cache=None, shared_rate_root=shared, once=False)
                stopped = Mock()
                stopped.wait.side_effect = lambda seconds: db.rollback()
                connect = sqlite3.connect
                def immediate(*args, **kwargs):
                    kwargs['timeout'] = 0
                    return connect(*args, **kwargs)
                with patch.object(sqlite3, 'connect', side_effect=immediate):
                    resolver = worker.chats_worker.open_worker_resolver(worker.ImportResolver, args, stopped)
                    with contextlib.closing(resolver.db):
                        stopped.wait.assert_called_once_with(5)

    def test_chats_rechecks_network_after_contention_and_honors_shutdown(self):
        with tempfile.TemporaryDirectory() as root:
            args = ['chats_worker.py', '--network', 'testnet-10', '--archive', 'https://archive',
                    '--endpoint', 'http://chats/history-import', '--cache', root]
            handlers = {}
            resolver = Mock()
            consumer = Mock()
            def stop():
                handlers[signal.SIGTERM](None, None)
            def refresh():
                if resolver.refresh_network.call_count == 1:
                    raise EvidencePending('busy')
                if resolver.refresh_network.call_count == 3:
                    stop()
            resolver.refresh_network.side_effect = refresh
            ticks = iter([0, 61, 62])
            with patch.object(sys, 'argv', args), patch.dict(os.environ, {'PROTOCOL_DATABASE_URL':'unused'}), \
                 patch.object(chats_worker, 'open_worker_resolver', return_value=resolver), \
                 patch.object(chats_worker, 'NativeTransport'), patch.object(chats_worker, 'check_source') as source, \
                 patch.object(chats_worker, 'connect', return_value=contextlib.nullcontext(Mock())), \
                 patch.object(chats_worker, 'ChatsConsumer', return_value=consumer), \
                 patch.object(signal, 'signal', side_effect=lambda sig, fn: handlers.update({sig:fn})), \
                 patch.object(chats_worker.threading.Event, 'wait', return_value=False), \
                 patch.object(chats_worker.time, 'monotonic', side_effect=lambda:next(ticks)):
                consumer.run_one.return_value = None
                chats_worker.main()
            self.assertEqual(source.call_count, 3)
            self.assertEqual(consumer.run_one.call_count, 1)

    def test_locked_shared_inventory_defers_then_revalidates_before_http(self):
        for resolver_type in (import_archive.Resolver, import_archive.ImportResolver):
            with self.subTest(worker=resolver_type.__name__), tempfile.TemporaryDirectory() as root:
                shared = Path(root) / 'shared'
                shared.mkdir()
                with contextlib.closing(sqlite3.connect(shared / 'inventory.sqlite')) as db:
                    db.execute('CREATE TABLE config (id INTEGER PRIMARY KEY, value TEXT)')
                    db.execute('INSERT INTO config VALUES (1, ?)', ('{"archive":"https://archive","network":"testnet-10"}',))
                    db.commit()
                    resolver = resolver_type(Path(root) / 'own', 'https://archive', 'https://archive',
                                             'testnet-10', shared_rate_root=shared)
                    with contextlib.closing(resolver.db), patch.object(import_archive.worker.history._chats.urllib.request, 'urlopen') as http:
                        db.execute('BEGIN EXCLUSIVE')
                        with self.assertRaisesRegex(EvidencePending, 'archive_inventory_busy'):
                            resolver.request('https://archive/info/network')
                        http.assert_not_called()
                        db.execute('UPDATE config SET value=? WHERE id=1', ('{"archive":"https://archive","network":"mainnet"}',))
                        db.commit()
                        with self.assertRaisesRegex(RuntimeError, 'archive/network mismatch'):
                            resolver.request('https://archive/info/network')
                        http.assert_not_called()
                        db.execute('UPDATE config SET value=? WHERE id=1', ('{"archive":"https://archive","network":"testnet-10"}',))
                        db.commit()
                        http.return_value.__enter__.return_value.read.return_value = b'{}'
                        self.assertEqual(resolver.request('https://archive/info/network'), {})
                        http.assert_called_once()

    def test_shared_cooldown_defers_without_http_or_network_cache_fallback(self):
        import time
        with tempfile.TemporaryDirectory() as root:
            resolver=import_archive.ImportResolver(root,'https://archive','https://archive','testnet-10')
            try:
                (Path(root)/'archive-rate.txt').write_text(str(time.time()+60))
                resolver.network_info={'networkName':'kaspa-testnet-10'}
                with patch.object(import_archive.worker.history._chats.urllib.request,'urlopen',side_effect=AssertionError('cooldown must defer')):
                    with self.assertRaises(EvidencePending):resolver.refresh_network()
            finally:resolver.db.close()
