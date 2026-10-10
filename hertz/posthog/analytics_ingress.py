#!/usr/bin/env python3
"""Owned HTTP V2 transport for the wallet's existing durable analytics SDK.

An acknowledgement means the complete original payload has been committed to
the private SQLite journal. Delivery failures retain that journal for retry.
No Amplitude service is contacted. Accepted records remain as a live archive.
"""
import argparse
import datetime as dt
import hashlib
import gzip
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NAMESPACE = uuid.UUID('dc627dfb-a129-4f8c-8043-5849ab31ff1a')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


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

    def connect(self):
        con = sqlite3.connect(self.path, timeout=30)
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA synchronous=FULL')
        return con

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
        if '__kasanova_transport_record' in props:
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
        timestamp = dt.datetime.fromtimestamp(event['time'] / 1000, dt.timezone.utc).isoformat()
        return {'event': event['event_type'], 'distinct_id': distinct,
                'uuid': record_id, 'timestamp': timestamp, 'properties': props}

    def deliver(self, host):
        with self.connect() as con:
            rows = con.execute('SELECT project,id,native FROM records WHERE sent=0 ORDER BY received LIMIT 100').fetchall()
        if not rows:
            return 0
        for project in dict.fromkeys(row[0] for row in rows):
            selected = [row for row in rows if row[0] == project]
            route = next(r for r in self.routes.values() if str(r['project_id']) == project)
            body = canonical({'api_key': route['project_token'],
                              'batch': [json.loads(row[2]) for row in selected]}).encode()
            request = urllib.request.Request(host.rstrip('/') + '/batch/', data=body,
                                             headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(request, timeout=30) as response:
                result = json.load(response)
                if result.get('status') not in (1, 'Ok'):
                    raise RuntimeError('Capture did not acknowledge batch')
            with self.connect() as con:
                con.executemany('UPDATE records SET sent=1 WHERE project=? AND id=?',
                                [(row[0], row[1]) for row in selected])
        return len(rows)

    def counts(self):
        with self.connect() as con:
            return dict(con.execute('SELECT sent,count(*) FROM records GROUP BY sent'))

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


def serve(journal, host, port):
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
            healthy = self.path in ('/health', '/kasanova-ingest/health')
            self.respond(200 if healthy else 404,
                         {'status': 'ok'} if healthy else {'error': 'Not found'})

        def do_POST(self):
            if self.path.rstrip('/') != '/kasanova-ingest/amplitude':
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
    return ThreadingHTTPServer((host, port), Handler)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--journal', type=Path, required=True)
    parser.add_argument('--posthog-host', default='http://proxy')
    parser.add_argument('--listen', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8099)
    parser.add_argument('--seed-import', type=Path)
    parser.add_argument('--seed-project', type=int, default=1)
    args = parser.parse_args()
    journal = Journal(args.journal, json.loads(args.config.read_text())['routes'])
    if args.seed_import:
        print(json.dumps({'history_seeded': journal.seed_history(args.seed_import, args.seed_project)}))
        return
    def worker():
        while True:
            try:
                if journal.deliver(args.posthog_host):
                    continue
            except Exception:
                pass
            time.sleep(2)
    threading.Thread(target=worker, daemon=True).start()
    serve(journal, args.listen, args.port).serve_forever()


if __name__ == '__main__':
    main()
