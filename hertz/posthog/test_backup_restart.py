import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from backup import restart_owned_services


class RestartOrderTest(unittest.TestCase):
    def setUp(self):
        self.running = [
            {'Name': '/owned-' + service, 'Config': {'Labels': {
                'com.docker.compose.project': 'kasanova_posthog',
                'com.docker.compose.service': service}}}
            for service in ['db', 'web', 'proxy', 'analytics-ingress', 'capture']
        ]
        self.services = {c['Config']['Labels']['com.docker.compose.service']: c for c in self.running}

    def test_gateway_dependency_starts_before_health_wait(self):
        started = set()
        saved = []

        def start(argv):
            self.assertEqual(argv[:2], ['docker', 'start'])
            started.update(argv[2:])
            return b''

        def health(*args, **kwargs):
            self.assertIn('/owned-capture', started)
            self.assertIn('/owned-analytics-ingress', started)
            return SimpleNamespace(__enter__=None)

        class Healthy:
            status = 200
            def __enter__(self):
                health()
                return self
            def __exit__(self, *args):
                return False

        with patch('backup.run', side_effect=start), patch('backup.urllib.request.urlopen', return_value=Healthy()), \
                patch('backup.owned_stack_ready', return_value=True), patch('backup.time.sleep') as sleep:
            restart_owned_services(self.running, self.services, True, Path('/checkpoint'), lambda *x: saved.append(x))
        self.assertEqual(started, {c['Name'] for c in self.running})
        sleep.assert_not_called()
        self.assertEqual(len(saved), 1)
        self.assertTrue(saved[0][1]['owned_gateway_and_ingress_ready'])

    def test_remaining_original_services_start_even_if_core_start_fails(self):
        calls = []
        def start(argv):
            calls.append(argv)
            if '/owned-web' in argv:
                raise subprocess.CalledProcessError(1, argv)
            return b''
        with patch('backup.run', side_effect=start), patch('backup.urllib.request.urlopen') as health:
            with self.assertRaises(subprocess.CalledProcessError):
                restart_owned_services(self.running, self.services, True, Path('/checkpoint'), self.fail)
        self.assertIn('/owned-capture', calls[-1])
        self.assertIn('/owned-analytics-ingress', calls[-1])
        health.assert_not_called()

    def test_other_project_is_rejected_before_any_start(self):
        self.running[0]['Config']['Labels']['com.docker.compose.project'] = 'another-project'
        with patch('backup.run') as start:
            with self.assertRaisesRegex(RuntimeError, 'another project'):
                restart_owned_services(self.running, self.services, True, Path('/checkpoint'), self.fail)
        start.assert_not_called()

    def test_realistic_cold_start_after_four_minutes_is_verified(self):
        now = [0.0]
        saved = []
        def sleep(seconds):
            now[0] += seconds
        with patch('backup.run', return_value=b''), patch('backup.time.monotonic', side_effect=lambda: now[0]), \
                patch('backup.time.sleep', side_effect=sleep), patch('backup.owned_stack_ready', side_effect=lambda *args: now[0] >= 300):
            restart_owned_services(self.running, self.services, True, Path('/checkpoint'), lambda *x: saved.append(x))
        self.assertEqual(now[0], 300)
        self.assertTrue(saved[0][1]['owned_gateway_and_ingress_ready'])

    def test_never_ready_cold_start_remains_bounded_and_has_no_success_receipt(self):
        now = [0.0]
        saved = []
        def sleep(seconds):
            now[0] += seconds
        with patch('backup.run', return_value=b''), patch('backup.time.monotonic', side_effect=lambda: now[0]), \
                patch('backup.time.sleep', side_effect=sleep), patch('backup.owned_stack_ready', return_value=False):
            with self.assertRaisesRegex(RuntimeError, 'did not recover'):
                restart_owned_services(self.running, self.services, True, Path('/checkpoint'), lambda *x: saved.append(x))
        self.assertEqual(now[0], 900)
        self.assertEqual(saved, [])


if __name__ == '__main__':
    unittest.main()
