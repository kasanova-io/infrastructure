"""A real standby process accepts into the incumbent's authoritative database."""
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from analytics_ingress import Journal


class StandbyTest(unittest.TestCase):
    def test_real_standby_and_incumbent_share_atomic_retries_and_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            routes = {'fixture-dev': {'project_id': 2, 'project_token': 'native-fixture',
                      'cloud': {'host': 'https://us.i.posthog.com', 'project_id': 102,
                                'project_token': 'cloud-fixture'}}}
            config = root / 'config.json'
            config.write_text(json.dumps({'routes': routes}))
            path = root / 'journal.sqlite'
            incumbent = Journal(path, routes)
            original = {'event_type': 'standby-fixture', 'device_id': 'device',
                        'user_id': 'wallet', 'insert_id': 'retry', 'time': 1791662400123}
            payload = {'api_key': 'fixture-dev', 'events': [original]}
            incumbent.accept(payload)
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]
            process = subprocess.Popen([
                sys.executable, '-B', str(Path(__file__).with_name('analytics_ingress.py')),
                '--config', str(config), '--journal', str(path), '--cloud-only',
                '--delivery-enable-file', str(root / 'absent-marker'),
                '--listen', '127.0.0.1', '--port', str(port),
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            def request(method, endpoint, body=None):
                connection = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
                try:
                    connection.request(method, endpoint, body,
                                       {'Content-Type': 'application/json'})
                    response = connection.getresponse()
                    data = response.read()
                    self.assertEqual(response.status, 200, data)
                finally:
                    connection.close()

            try:
                deadline = time.monotonic() + 10
                while True:
                    try:
                        request('GET', '/health')
                        break
                    except OSError:
                        if process.poll() is not None or time.monotonic() >= deadline:
                            self.fail('Standby did not start')
                        time.sleep(0.02)
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(request, 'POST', '/kasanova-ingest/amplitude',
                                           json.dumps(payload)) if index % 2 else
                               pool.submit(incumbent.accept, payload) for index in range(20)]
                    for future in futures:
                        future.result()
                request('POST', '/kasanova-ingest/amplitude', json.dumps({
                    'api_key': 'fixture-dev', 'events': [{
                        'event_type': 'standby-fixture', 'device_id': 'device',
                        'insert_id': 'next', 'time': 1791662401123,
                    }]}))
                time.sleep(2.2)  # Cross the real worker's next gate check.
                with incumbent.connect() as con:
                    rows = con.execute('SELECT raw,native,sent,cloud_destination FROM records ORDER BY received').fetchall()
                    self.assertEqual(len(rows), 2)
                    self.assertTrue(all(row[2:] == (0, None) for row in rows))
                    self.assertTrue(all(json.loads(row[1])['distinct_id'] == 'wallet' for row in rows))
                    self.assertEqual(json.loads(rows[0][0]), original)
                    self.assertEqual(con.execute('SELECT * FROM identities').fetchall(), [('2', 'device', 'wallet')])
                process.send_signal(signal.SIGTERM)
                _, stderr = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 0, stderr.decode())
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=10)


if __name__ == '__main__':
    unittest.main()
