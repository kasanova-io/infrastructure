"""Real contention must consume, rather than bypass, a bounded pacing budget."""
import contextlib
import fcntl
import importlib.util
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    'rate_lock_history', Path(__file__).parents[2] / 'social-indexer-history/history.py')
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)
collect = history._chats


class RequestRetryTests(unittest.TestCase):
    def check_retry(self, body):
        class Response:
            headers = {}
            def __init__(self, fail_close): self.fail_close = fail_close
            def __enter__(self): return self
            def read(self, size): return b'[]'
            def __exit__(self, *_):
                if self.fail_close: raise OSError('retryable close failure')
        with patch.object(collect, 'rate_slot'), patch.object(collect.time, 'sleep'), \
             patch.object(collect.urllib.request, 'urlopen', side_effect=[Response(True), Response(False)]) as opening:
            self.assertEqual(collect.request('https://archive/transactions/search', attempts=2,
                                            max_bytes=1024, data=body), ([], {}))
        requests = [call.args[0] for call in opening.call_args_list]
        self.assertEqual([request.get_method() for request in requests],
                         ['GET' if body is None else 'POST'] * 2)
        self.assertEqual([request.data for request in requests], [body, body])

    def test_get_retry_preserves_method_and_absent_body(self):
        self.check_retry(None)

    def test_post_retry_preserves_original_search_body(self):
        self.check_retry(b'{"transactionIds":["' + b'ab' * 32 + b'"]}')


class RateLockTests(unittest.TestCase):
    def setUp(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(root)
        self.enterContext(patch.dict(os.environ, {'CHATS_RATE_DIR': root}))

    @contextlib.contextmanager
    def held(self, name, release_after=0.08, next_at=0):
        with (self.root / name).open('w+') as holder:
            holder.write(str(next_at)); holder.flush()
            fcntl.flock(holder, fcntl.LOCK_EX)
            def release():
                time.sleep(release_after)
                fcntl.flock(holder, fcntl.LOCK_UN)
            thread = threading.Thread(target=release)
            thread.start()
            try:
                yield
            finally:
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())

    def test_archive_bucket_waits_for_brief_contention(self):
        with self.held('rate-details.txt'):
            collect.rate_slot('https://api.kaspa.org/transactions/test', max_wait=0.5)
        self.assertGreater(float((self.root / 'rate-details.txt').read_text()), time.time())

    def test_busy_bucket_defers_within_budget_without_overwriting_holder(self):
        with self.held('rate-details.txt', release_after=0.2, next_at=123):
            start = time.monotonic()
            with self.assertRaisesRegex(collect.RatePending, 'bucket busy'):
                collect.rate_slot('https://api.kaspa.org/transactions/test', max_wait=0.04)
            elapsed = time.monotonic() - start
            self.assertGreaterEqual(elapsed, 0.03)
            self.assertLess(elapsed, 0.18)
        self.assertEqual(float((self.root / 'rate-details.txt').read_text()), 123)

    def test_lock_wait_and_cooldown_share_one_budget(self):
        next_at = time.time() + 0.4
        with self.held('rate-details.txt', release_after=0.2, next_at=next_at):
            with self.assertRaisesRegex(collect.RatePending, 'cooldown'):
                collect.rate_slot('https://api.kaspa.org/transactions/test', max_wait=0.3)
        self.assertEqual(float((self.root / 'rate-details.txt').read_text()), next_at)

    def test_expired_retry_cannot_take_a_newly_freed_lock(self):
        with (self.root / 'rate-details.txt').open('w+') as holder:
            holder.write('0'); holder.flush(); fcntl.flock(holder, fcntl.LOCK_EX)
            with patch.object(collect.time, 'monotonic', side_effect=[0, 0, 0.02]), \
                 patch.object(collect.time, 'sleep', side_effect=lambda _: fcntl.flock(holder, fcntl.LOCK_UN)):
                with self.assertRaises(collect.RatePending):
                    collect.rate_slot('https://api.kaspa.org/transactions/test', max_wait=0.01)
        self.assertEqual(float((self.root / 'rate-details.txt').read_text()), 0)

    def test_cooldown_update_keeps_retry_after_under_contention(self):
        with self.held('rate-details.txt'):
            collect.rate_slot('https://api.kaspa.org/transactions/test', cooldown=60, max_wait=0.5)
        self.assertGreater(float((self.root / 'rate-details.txt').read_text()), time.time() + 59)

    def test_unbounded_existing_caller_still_waits_for_holder(self):
        with self.held('rate-details.txt'):
            collect.rate_slot('https://api.kaspa.org/transactions/test')
        self.assertGreater(float((self.root / 'rate-details.txt').read_text()), time.time())

    def test_history_wrapper_waits_for_same_bounded_bucket(self):
        h = history.History(self.root, 'https://source', 'https://archive', 'testnet-10')
        self.addCleanup(h.db.close)
        h.request_max_wait = 0.5
        class Response:
            headers = {}
            def __enter__(self):return self
            def __exit__(self,*_):pass
            def read(self,*_):return b'{"ok":true}'
        with self.held('source-rate.txt'), patch.object(collect.urllib.request,'urlopen',return_value=Response()) as opening:
            started=time.monotonic()
            self.assertEqual(h.request('https://source/transactions/test'), {'ok': True})
            self.assertGreaterEqual(time.monotonic()-started,0.05)
            self.assertEqual(opening.call_count,1)

    def test_zero_budget_and_future_cooldown_never_make_an_http_request(self):
        (self.root / 'rate-details.txt').write_text(str(time.time() + 60))
        with self.assertRaises(collect.RatePending):
            collect.rate_slot('https://api.kaspa.org/transactions/test', max_wait=0)

    def test_invalid_budget_cannot_create_an_unbounded_wait(self):
        for invalid in (-1, float('inf'), float('nan')):
            with self.subTest(invalid=invalid), self.assertRaises(collect.PacingError):
                collect.rate_slot('https://api.kaspa.org/transactions/test', max_wait=invalid)

    def test_pacing_oversleep_never_updates_bucket_or_makes_http_request(self):
        path = self.root / 'rate-details.txt'; path.write_text('0')
        clock = [0]
        with patch.object(collect.time, 'monotonic', side_effect=lambda: clock[0]), \
             patch.object(collect.time, 'sleep', side_effect=lambda _: clock.__setitem__(0, 1)), \
             patch.object(collect.urllib.request, 'urlopen') as request:
            with self.assertRaisesRegex(RuntimeError, 'retries exhausted'):
                collect.request('https://api.kaspa.org/transactions/test', attempts=1, max_wait=0.5)
        request.assert_not_called()
        self.assertEqual(path.read_text(), '0')

    def test_history_pacing_oversleep_never_updates_bucket_or_calls_downstream(self):
        h = history.History(self.root, 'https://source', 'https://archive', 'testnet-10')
        self.addCleanup(h.db.close); h.request_max_wait = 0.5; h.request_options={'attempts':1}
        path = self.root / 'source-rate.txt'; path.write_text('0')
        clock = [0]
        with patch.object(collect.time, 'monotonic', side_effect=lambda: clock[0]), \
             patch.object(collect.time, 'sleep', side_effect=lambda _: clock.__setitem__(0, 1)), \
             patch.object(collect.urllib.request, 'urlopen') as request:
            with self.assertRaisesRegex(RuntimeError, 'retries exhausted'):
                h.request('https://source/transactions/test')
        request.assert_not_called()
        self.assertEqual(path.read_text(), '0')
