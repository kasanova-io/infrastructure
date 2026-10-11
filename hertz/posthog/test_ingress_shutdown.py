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


class ShutdownTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.routes = {'dev': {'project_id': 2, 'project_token': 'local-test'}}
        config = root / 'config.json'
        config.write_text(json.dumps({'routes': self.routes}))
        self.journal = root / 'ingress.sqlite'
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            self.port = probe.getsockname()[1]
        self.process = subprocess.Popen([
            sys.executable, '-B', str(Path(__file__).with_name('analytics_ingress.py')),
            '--config', str(config), '--journal', str(self.journal),
            '--listen', '127.0.0.1', '--port', str(self.port),
            '--posthog-host', 'http://127.0.0.1:1',
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(self.cleanup_process)
        deadline = time.monotonic() + 10
        while True:
            try:
                self.health()
                break
            except OSError:
                if self.process.poll() is not None or time.monotonic() >= deadline:
                    self.fail('Real ingress did not start')
                time.sleep(0.02)
        self.payload = json.dumps({'api_key': 'dev', 'events': [{
            'event_type': 'shutdown-verification', 'device_id': 'test-device',
            'insert_id': 'test-shutdown', 'time': 1791662400123,
        }]}).encode()

    def cleanup_process(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)

    def health(self):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=1)
        try:
            connection.request('GET', '/health')
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
        finally:
            connection.close()

    def assert_clean_stop_and_pending_record(self):
        stdout, stderr = self.process.communicate(timeout=10)
        self.assertEqual(self.process.returncode, 0, stderr.decode())
        journal = Journal(self.journal, self.routes)
        self.assertEqual(journal.counts(), {0: 1})
        with journal.connect() as con:
            self.assertEqual(json.loads(con.execute('SELECT raw FROM records').fetchone()[0]),
                             json.loads(self.payload)['events'][0])

    def test_sigterm_stops_real_ingress_with_committed_pending_record(self):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        try:
            connection.request('POST', '/kasanova-ingest/amplitude', self.payload,
                               {'Content-Type': 'application/json'})
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
        finally:
            connection.close()
        self.process.send_signal(signal.SIGTERM)
        self.assert_clean_stop_and_pending_record()

    def test_sigterm_waits_for_accepted_request_to_commit(self):
        connection = socket.create_connection(('127.0.0.1', self.port), timeout=5)
        self.addCleanup(connection.close)
        headers = ('POST /kasanova-ingest/amplitude HTTP/1.1\r\n'
                   'Host: localhost\r\nContent-Length: %d\r\n'
                   'Content-Type: application/json\r\nConnection: close\r\n\r\n') % len(self.payload)
        connection.sendall(headers.encode() + self.payload[:1])
        # A subsequent served request establishes that the earlier connection
        # was accepted before SIGTERM interrupts the real serving loop.
        self.health()
        self.process.send_signal(signal.SIGTERM)
        time.sleep(0.1)
        self.assertIsNone(self.process.poll())
        connection.sendall(self.payload[1:])
        response = b''
        while chunk := connection.recv(4096):
            response += chunk
        self.assertIn(b'200 OK', response)
        self.assert_clean_stop_and_pending_record()


if __name__ == '__main__':
    unittest.main()
