"""Network outages cannot admit work with stale network evidence or crash-loop."""
import contextlib
import io
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

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
