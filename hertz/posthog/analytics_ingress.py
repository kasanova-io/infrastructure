#!/usr/bin/env python3
"""Owned HTTP V2 transport for the wallet's existing durable analytics SDK.

An acknowledgement means the complete original payload has been committed to
the private SQLite journal. Delivery failures retain that journal for retry.
No Amplitude service is contacted. Accepted records remain as a live archive.
"""
import argparse
from contextlib import contextmanager
import datetime as dt
import hashlib
import gzip
import json
from decimal import Decimal
import math
import os
from pathlib import Path
import signal
import sqlite3
import threading
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NAMESPACE = uuid.UUID('dc627dfb-a129-4f8c-8043-5849ab31ff1a')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def precise_json(value):
    """Keep float values exact across Cloud's decimal-to-binary JSON boundary.

    The shortest Python spelling can be parsed one ULP differently by Cloud.
    Full decimal expansions retain the same JSON numeric type and binary value.
    Original journal/source serialization remains unchanged.
    """
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError('Nonfinite JSON number')
        number = str(Decimal.from_float(value))
        return number if '.' in number or 'e' in number.lower() else number + '.0'
    if isinstance(value, dict):
        if any(not isinstance(k,str) for k in value):
            raise ValueError('JSON object keys must be strings')
        return '{' + ','.join(json.dumps(k,ensure_ascii=False)+':'+precise_json(value[k]) for k in sorted(value)) + '}'
    if isinstance(value, list):
        return '[' + ','.join(precise_json(item) for item in value) + ']'
    return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)


NUMERIC_REPAIRS = '__kasanova_numeric_repairs'


def cloud_event(event):
    """Carry exact finite floats through the native Cloud transformation.

    Repair metadata is a JSON string, so the capture parser cannot round its
    decimal values. The transformation restores numeric types at their paths.
    Source event and journal bytes remain unchanged.
    """
    properties = event.get('properties') or {}
    if NUMERIC_REPAIRS in properties:
        raise ValueError('Reserved Cloud numeric repair property collision')
    repairs = []

    def visit(value, path):
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError('Nonfinite JSON number')
            repairs.append({'path': path, 'value': repr(value)})
        elif isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValueError('JSON object keys must be strings')
                visit(item, path + [key])
        elif isinstance(value, list):
            for index, item in enumerate(value, 1):
                visit(item, path + [index])  # Hog arrays are one-based.

    visit(properties, [])
    if not repairs:
        return event
    return {**event, 'properties': {**properties, NUMERIC_REPAIRS: canonical(repairs)}}


class Journal:
    def __init__(self, path, routes):
        self.path, self.routes = str(path), routes
        with self.connect() as con:
            con.executescript('''
                CREATE TABLE IF NOT EXISTS records (
                    project TEXT NOT NULL, id TEXT NOT NULL, raw TEXT NOT NULL,
                    native TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0,
                    received REAL NOT NULL, PRIMARY KEY(project,id));
                CREATE INDEX IF NOT EXISTS delivery ON records(sent,received);
                CREATE TABLE IF NOT EXISTS identities (
                    project TEXT NOT NULL, device TEXT NOT NULL, distinct_id TEXT NOT NULL,
                    PRIMARY KEY(project,device));
                CREATE TABLE IF NOT EXISTS history (
                    project TEXT NOT NULL, source_id TEXT NOT NULL,
                    PRIMARY KEY(project,source_id));
            ''')
            # Keep the original acknowledgement and local delivery state intact.
            # Cloud acknowledgements are independent and survive worker restarts.
            columns = {row[1] for row in con.execute('PRAGMA table_info(records)')}
            if 'cloud_destination' not in columns:
                con.execute('ALTER TABLE records ADD COLUMN cloud_destination TEXT')

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.path, timeout=30)
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA synchronous=FULL')
        try:
            with con:
                yield con
        finally:
            con.close()

    def accept(self, payload):
        route = self.routes.get(payload.get('api_key'))
        if route is None:
            raise ValueError('Invalid API key')
        events = payload.get('events')
        if not isinstance(events, list) or not events or len(events) > 2000:
            raise ValueError('Expected 1 to 2000 events')
        project = str(route['project_id'])
        with self.connect() as con:
            # Serialize identity and record commits: no partial batch acknowledgement.
            con.execute('BEGIN IMMEDIATE')
            for event in events:
                if not isinstance(event, dict) or not isinstance(event.get('event_type'), str):
                    raise ValueError('Invalid event')
                raw = canonical(event)
                source_id = event.get('insert_id')
                if not source_id:
                    # Attempts change on SDK retries; exclude only transport counters.
                    stable = {k: v for k, v in event.items() if k not in ('attempts',)}
                    source_id = hashlib.sha256(canonical(stable).encode()).hexdigest()
                if con.execute('SELECT 1 FROM history WHERE project=? AND source_id=?',
                               (project, str(source_id))).fetchone():
                    continue
                record_id = str(uuid.uuid5(NAMESPACE, project + ':' + str(source_id)))
                existing = con.execute('SELECT 1 FROM records WHERE project=? AND id=?',
                                       (project, record_id)).fetchone()
                if existing:
                    continue
                device = event.get('device_id')
                user = event.get('user_id')
                known = con.execute('SELECT distinct_id FROM identities WHERE project=? AND device=?',
                                    (project, device)).fetchone() if device else None
                distinct = user or (known[0] if known else device)
                if not isinstance(distinct, str) or not distinct:
                    raise ValueError('Missing identity')
                if user and device:
                    con.execute('INSERT OR REPLACE INTO identities VALUES (?,?,?)', (project, device, user))
                native = self.convert(event, raw, record_id, distinct, route)
                con.execute('INSERT INTO records(project,id,raw,native,received) VALUES (?,?,?,?,?)',
                            (project, record_id, raw, canonical(native), time.time()))
        return len(events)

    @staticmethod
    def convert(event, raw, record_id, distinct, route):
        props = dict(event.get('event_properties') or {})
        if '__kasanova_transport_record' in props or NUMERIC_REPAIRS in props:
            raise ValueError('Reserved transport property collision')
        props['__kasanova_transport_record'] = raw
        props.setdefault('$device_id', event.get('device_id'))
        props.setdefault('$insert_id', record_id)
        props.setdefault('$geoip_disable', True)
        user_props = event.get('user_properties') or {}
        # The app uses Identify.set exclusively; preserve any other operation in raw.
        if '$set' in user_props:
            props['$set'] = user_props['$set']
        elif user_props:
            props['$set'] = user_props
        extra = event.get('extra') or {}
        replay_session = extra.get('kasanova_posthog_session_id')
        if replay_session:
            props['$session_id'] = str(uuid.UUID(replay_session))
        elif event.get('session_id', -1) >= 0:
            # Match the deterministic UUIDv7 mapping used for the historical import.
            key = json.dumps([str(route['legacy_app']), event.get('device_id'),
                              str(event['session_id'])], separators=(',', ':'))
            milliseconds = min(int(event['session_id']), int(event['time']))
            entropy = int.from_bytes(hashlib.sha256(key.encode()).digest()[:10], 'big')
            value = ((milliseconds << 80) | (7 << 76) |
                     (((entropy >> 64) & 0xfff) << 64) | (2 << 62) |
                     (entropy & ((1 << 62) - 1)))
            props['$session_id'] = str(uuid.UUID(int=value))
        occurrence = extra.get('kasanova_event_timestamp_ms')
        timestamp = None
        if isinstance(occurrence, int) and not isinstance(occurrence, bool):
            try:
                timestamp = (dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) +
                             dt.timedelta(milliseconds=occurrence)).isoformat()
            except (OverflowError, OSError, ValueError):
                pass  # Old or malformed envelopes retain the established SDK time behavior.
        if timestamp is None:
            timestamp = dt.datetime.fromtimestamp(event['time'] / 1000, dt.timezone.utc).isoformat()
        return {'event': event['event_type'], 'distinct_id': distinct,
                'uuid': record_id, 'timestamp': timestamp, 'properties': props}

    def deliver(self, host, stopping=None):
        with self.connect() as con:
            rows = con.execute('SELECT project,id,native FROM records WHERE sent=0 ORDER BY received LIMIT 100').fetchall()
        if not rows:
            return 0
        delivered = 0
        for project in dict.fromkeys(row[0] for row in rows):
            if stopping is not None and stopping.is_set():
                break
            selected = [row for row in rows if row[0] == project]
            route = next(r for r in self.routes.values() if str(r['project_id']) == project)
            body = precise_json({'api_key': route['project_token'],
                              'batch': [json.loads(row[2]) for row in selected]}).encode()
            request = urllib.request.Request(host.rstrip('/') + '/batch/', data=body,
                                             headers={'Content-Type': 'application/json'})
            if stopping is not None and stopping.is_set():
                break
            with urllib.request.urlopen(request, timeout=30) as response:
                result = json.load(response)
                if result.get('status') not in (1, 'Ok'):
                    raise RuntimeError('Capture did not acknowledge batch')
            with self.connect() as con:
                con.executemany('UPDATE records SET sent=1 WHERE project=? AND id=?',
                                [(row[0], row[1]) for row in selected])
            delivered += len(selected)
        return delivered

    def counts(self):
        with self.connect() as con:
            return dict(con.execute('SELECT sent,count(*) FROM records GROUP BY sent'))

    def deliver_cloud(self, stopping=None):
        """Mirror durable events without changing Hertz delivery or client keys."""
        delivered = 0
        failures = []
        for project in dict.fromkeys(str(route['project_id']) for route in self.routes.values()):
            if stopping is not None and stopping.is_set():
                break
            route = next(r for r in self.routes.values() if str(r['project_id']) == project)
            cloud = route.get('cloud')
            if not cloud:
                continue
            try:
                delivered += self._deliver_cloud_project(project, cloud, stopping)
            except Exception as error:
                failures.append(error)  # An unavailable PROD must not starve DEV.
        if failures and not delivered:
            raise failures[0]
        return delivered

    def _deliver_cloud_project(self, project, cloud, stopping=None):
        host = cloud['host'].rstrip('/')
        if host not in ('https://us.i.posthog.com', 'https://eu.i.posthog.com'):
            raise ValueError('Unexpected PostHog Cloud ingestion host')
        destination = host + ':' + str(int(cloud['project_id']))
        with self.connect() as con:
            rows = con.execute(
                'SELECT id,native FROM records WHERE project=? '
                'AND (cloud_destination IS NULL OR cloud_destination != ?) '
                'ORDER BY received,id LIMIT 100', (project, destination)).fetchall()
        if not rows:
            return 0
        boundary = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=48)
        groups = {True: [], False: []}
        for row in rows:
            event = json.loads(row[1])
            historical = dt.datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')) <= boundary
            groups[historical].append((row[0], event))
        delivered = 0
        for historical, selected in groups.items():
            if not selected:
                continue
            payload = {'api_key': cloud['project_token'],
                       'batch': [cloud_event(event) for _, event in selected]}
            if historical:
                payload['historical_migration'] = True
            request = urllib.request.Request(host + '/batch/', data=precise_json(payload).encode(),
                                             headers={'Content-Type': 'application/json'})
            if stopping is not None and stopping.is_set():
                break
            with urllib.request.urlopen(request, timeout=30) as response:
                result = json.load(response)
                if result.get('status') not in (1, 'Ok'):
                    raise RuntimeError('Cloud capture did not acknowledge batch')
            with self.connect() as con:
                con.executemany('UPDATE records SET cloud_destination=? WHERE project=? AND id=?',
                                [(destination, project, identifier) for identifier, _ in selected])
            delivered += len(selected)
        return delivered

    def enqueue_history(self, events, project):
        """Own source events durably before suppressing matching SDK retries.

        The same transaction holds the original Amplitude payload, unchanged
        native UUID/envelope and retry identity. Delivery uses the existing
        independent Hertz/Cloud workers; an outage never loses a reservation.
        Historical person-property writes require a separate supported importer.
        """
        project = str(project)
        route = next((r for r in self.routes.values() if str(r['project_id']) == project), None)
        if route is None:
            raise ValueError('Unknown history project')
        queued = []
        with self.connect() as con:
            con.execute('BEGIN IMMEDIATE')
            for native in events:
                properties = native['properties']
                raw = properties['__amplitude_original_record']
                original = json.loads(raw)
                if hashlib.sha256(raw.encode()).hexdigest() != properties['__amplitude_record_sha256']:
                    raise ValueError('Original history digest differs')
                if str(original['app']) != str(route['legacy_app']):
                    raise ValueError('History source project differs')
                if native['uuid'] != original['uuid'] or str(uuid.UUID(native['uuid'])) != native['uuid']:
                    raise ValueError('History source UUID differs')
                if native['event'] != original['event_type'] or not native['event'] or native['event'].startswith('$'):
                    raise ValueError('History source event name differs')
                occurrence = dt.datetime.fromisoformat(original['event_time'].replace('Z', '+00:00'))
                if occurrence.tzinfo is None:
                    occurrence = occurrence.replace(tzinfo=dt.timezone.utc)
                if dt.datetime.fromisoformat(native['timestamp'].replace('Z', '+00:00')) != occurrence:
                    raise ValueError('History source occurrence timestamp differs')
                if original.get('user_properties'):
                    raise ValueError('Historical person snapshots need a supported import')
                for key, value in (original.get('event_properties') or {}).items():
                    if key not in properties or canonical(properties[key]) != canonical(value):
                        raise ValueError('History source property differs')
                if properties.get('$set') or properties.get('$set_once') or properties.get('$unset'):
                    raise ValueError('Historical person updates need a supported import')
                if NUMERIC_REPAIRS in properties:
                    raise ValueError('Reserved numeric repair collision')
                source_id = original.get('$insert_id')
                if not source_id:
                    raise ValueError('History needs an original retry identifier')
                sdk_id = str(uuid.uuid5(NAMESPACE, project + ':' + str(source_id)))
                existing = con.execute('SELECT id,native FROM records WHERE project=? AND id IN (?,?)',
                                       (project, sdk_id, native['uuid'])).fetchall()
                if existing:
                    # A concurrently committed SDK event already owns delivery.
                    if any(identifier == sdk_id for identifier, _ in existing):
                        queued.append(json.loads(next(payload for identifier, payload in existing if identifier == sdk_id)))
                        continue
                    if any(payload != canonical(native) for _, payload in existing):
                        raise ValueError('Previously queued history envelope differs')
                    queued.append(native)
                    continue
                if con.execute('SELECT 1 FROM history WHERE project=? AND source_id=?',
                               (project, str(source_id))).fetchone():
                    continue
                device = original.get('device_id')
                if device:
                    known = con.execute('SELECT distinct_id FROM identities WHERE project=? AND device=?',
                                        (project, device)).fetchone()
                    if known and known[0] != native['distinct_id']:
                        raise ValueError('History would change an existing device identity')
                    con.execute('INSERT OR IGNORE INTO identities VALUES (?,?,?)',
                                (project, device, native['distinct_id']))
                con.execute('INSERT INTO records(project,id,raw,native,received) VALUES (?,?,?,?,?)',
                            (project, native['uuid'], raw, canonical(native), time.time()))
                con.execute('INSERT INTO history VALUES (?,?)', (project, str(source_id)))
                queued.append(native)
        return queued

    def seed_history(self, source, project):
        """Bind archived device identities and skip SDK retries already imported."""
        count = 0
        with gzip.open(source, 'rt') as stream, self.connect() as con:
            for line in stream:
                native = json.loads(line)
                original = json.loads(native['properties']['__amplitude_original_record'])
                device = original.get('device_id')
                if device:
                    con.execute('INSERT OR REPLACE INTO identities VALUES (?,?,?)',
                                (str(project), device, native['distinct_id']))
                source_id = original.get('$insert_id')
                if source_id:
                    con.execute('INSERT OR IGNORE INTO history VALUES (?,?)',
                                (str(project), str(source_id)))
                count += 1
                if count % 5000 == 0:
                    con.commit()
        return count


def serve(journal, host, port, native_proxy=None):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # No keys, identities, URLs or payloads in access logs.

        def respond(self, code, body):
            data = canonical(body).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(data)

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()

        def do_GET(self):
            healthy = self.path in ('/health', '/_health', '/kasanova-ingest/health')
            if not healthy and native_proxy is not None:
                return self.forward_native()
            self.respond(200 if healthy else 404,
                         {'status': 'ok'} if healthy else {'error': 'Not found'})

        def do_POST(self):
            if self.path.rstrip('/') != '/kasanova-ingest/amplitude':
                if native_proxy is not None:
                    return self.forward_native()
                return self.respond(404, {'error': 'Not found'})
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 32 * 1024 * 1024:
                    return self.respond(413, {'code': 413, 'error': 'Payload too large'})
                payload = json.loads(self.rfile.read(length))
                count = journal.accept(payload)
                self.respond(200, {'code': 200, 'events_ingested': count,
                                   'payload_size_bytes': length, 'server_upload_time': int(time.time() * 1000)})
            except Exception:
                # Preserve the client queue on any ambiguous/unsupported input or disk failure.
                self.respond(503, {'code': 503, 'error': 'Journal unavailable; retry'})

        def forward_native(self):
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if length < 0 or length > 32 * 1024 * 1024:
                    return self.respond(413, {'error': 'Payload too large'})
                code, content_type, raw = native_proxy.request(
                    self.command, self.path, self.rfile.read(length),
                    self.headers.get('Content-Encoding', ''), self.headers.get('User-Agent', ''))
                self.send_response(code)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(raw)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(raw)
            except Exception:
                self.respond(503, {'error': 'Cloud unavailable or unsupported request; retry'})
    server = ThreadingHTTPServer((host, port), Handler)
    # Acknowledged records and requests already accepted must finish before a
    # live checkpoint can stop the journal writer.
    server.daemon_threads = False
    return server


def validate_cloud_routes(routes):
    """Require an isolated Cloud destination for every retained client route."""
    if not routes:
        raise ValueError('Cloud-only delivery requires configured client routes')
    destinations = {}
    projects = {}
    tokens = {}
    for route in routes.values():
        cloud = route.get('cloud') or {}
        host = cloud.get('host', '').rstrip('/')
        project = cloud.get('project_id')
        if (host not in ('https://us.i.posthog.com', 'https://eu.i.posthog.com')
                or type(project) is not int or project <= 0
                or not isinstance(cloud.get('project_token'), str)
                or not cloud['project_token'].strip()):
            raise ValueError('Every client route requires a complete Cloud destination')
        source = str(route['project_id'])
        destination = (host, project)
        binding = (destination, cloud['project_token'])
        if source in projects and projects[source] != binding:
            raise ValueError('One source project cannot have conflicting Cloud routes')
        if destination in destinations and destinations[destination] != source:
            raise ValueError('Distinct source environments cannot share a Cloud project')
        if cloud['project_token'] in tokens and tokens[cloud['project_token']] != source:
            raise ValueError('Distinct source environments cannot share a Cloud token')
        projects[source] = binding
        destinations[destination] = source
        tokens[cloud['project_token']] = source


def delivery_operations(journal, posthog_host, stopping, cloud_only=False):
    """Cloud-only operation never calls the retired native destination."""
    cloud = lambda: journal.deliver_cloud(stopping)
    if cloud_only:
        validate_cloud_routes(journal.routes)
        return (cloud,)
    return (lambda: journal.deliver(posthog_host, stopping), cloud)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--journal', type=Path, required=True)
    parser.add_argument('--posthog-host', default='http://proxy')
    parser.add_argument('--cloud-only', action='store_true',
                        help='Deliver only to configured Cloud projects; retain native journal acknowledgements unchanged')
    parser.add_argument('--listen', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8099)
    parser.add_argument('--seed-import', type=Path)
    parser.add_argument('--seed-project', type=int, default=1)
    args = parser.parse_args()
    routes = json.loads(args.config.read_text())['routes']
    if args.cloud_only:
        validate_cloud_routes(routes)
    journal = Journal(args.journal, routes)
    if args.seed_import:
        print(json.dumps({'history_seeded': journal.seed_history(args.seed_import, args.seed_project)}))
        return
    stopping = threading.Event()
    operations = delivery_operations(journal, args.posthog_host, stopping, args.cloud_only)
    def worker():
        while not stopping.is_set():
            delivered = 0
            for operation in operations:
                if stopping.is_set():
                    break
                try:
                    delivered += operation()
                except Exception:
                    pass  # One destination outage must not block the other.
            if delivered:
                continue
            stopping.wait(2)
    native_proxy = None
    if args.cloud_only:
        from native_cloud_proxy import NativeCloudProxy
        native_proxy = NativeCloudProxy(routes, precise_json)
    server = serve(journal, args.listen, args.port, native_proxy)
    def terminate(signum, frame):
        # Python runs as container PID1, where the default SIGTERM disposition
        # is ignored. Exit the serving loop and drain its existing writers.
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, terminate)
    delivery = threading.Thread(target=worker)
    delivery.start()
    try:
        server.serve_forever()
    finally:
        stopping.set()
        server.server_close()
        delivery.join()


if __name__ == '__main__':
    main()
