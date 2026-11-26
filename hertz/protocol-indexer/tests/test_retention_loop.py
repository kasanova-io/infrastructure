"""Real child-process shutdown, failure privacy and deadline regressions."""
import importlib.util
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('retention_loop', Path(__file__).parents[1] / 'retention_loop.py')
loop = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loop)


class RetentionProcessTests(unittest.TestCase):
    def test_private_driver_diagnostics_never_escape_failed_child(self):
        with self.assertRaises(loop.Deferred) as failed:
            loop.run_command([sys.executable, '-c',
                              "import sys;sys.stderr.write('private-driver-marker');sys.exit(1)"], threading.Event())
        self.assertNotIn('private-driver-marker', str(failed.exception))

    def test_child_success_preserves_the_real_acknowledgement(self):
        raw = loop.run_command([sys.executable, '-c',
                                'print(\'{"raw_transactions_pruned": 17}\')'], threading.Event())
        self.assertEqual(raw.strip(), b'{"raw_transactions_pruned": 17}')

    def test_shutdown_and_deadline_do_not_leave_owned_child_running(self):
        real_popen = loop.subprocess.Popen
        for stop_requested in [True, False]:
            with self.subTest(stop_requested=stop_requested):
                stop = threading.Event()
                timer = threading.Timer(.05, stop.set) if stop_requested else None
                if timer:
                    timer.start()
                started = time.monotonic()
                children = []
                def create(*args, **kwargs):
                    child = real_popen(*args, **kwargs)
                    children.append(child)
                    return child
                try:
                    with patch.object(loop.subprocess, 'Popen', side_effect=create):
                        with self.assertRaises(loop.Stopped if stop_requested else loop.Deferred):
                            loop.run_command([sys.executable, '-c', 'import time;time.sleep(30)'],
                                             stop, timeout=2 if stop_requested else .05)
                    self.assertEqual(len(children), 1)
                    self.assertIsNotNone(children[0].poll(), 'owned child still running')
                finally:
                    if timer:
                        timer.join()
                    for child in children:
                        if child.poll() is None:
                            child.kill()
                            child.wait()
                self.assertLess(time.monotonic() - started, 5)
