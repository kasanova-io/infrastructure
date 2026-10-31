#!/usr/bin/env python3
"""Acquire public tip/room receipts through the existing bounded archive tools."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import time
import urllib.parse

from import_archive import ImportResolver
from protocol_indexer.cli import check_source
from protocol_indexer.evidence import CONSUMER, EvidenceConsumer
from protocol_indexer.journal import connect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--network', required=True)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--cache', type=Path, required=True)
    parser.add_argument('--metrics-url', default=os.environ.get('SIMPLY_METRICS_URL', 'http://indexer:8501/api/metrics'))
    parser.add_argument('--source-binding', default=os.environ.get('PROTOCOL_SOURCE_BINDING_FILE', '/run/protocol-indexer/source-binding.json'))
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    archive = urllib.parse.urlsplit(args.archive)
    if (archive.scheme != 'https' or not archive.hostname or archive.username or archive.password
            or archive.query or archive.fragment or not os.environ.get('PROTOCOL_DATABASE_URL')):
        parser.error('explicit HTTPS archive origin and PROTOCOL_DATABASE_URL required')
    os.umask(0o077)
    resolver = None
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        with connect(os.environ['PROTOCOL_DATABASE_URL']) as conn:
            check_source(conn, args.metrics_url, args.network, args.source_binding)
            config = conn.execute('SELECT network,journal_id::text AS id FROM protocol_indexer.config').fetchone()
            if not config or config['network'] != args.network:
                raise ValueError('output evidence journal network mismatch')
            source_id = config['id']
        resolver = ImportResolver(args.cache, args.archive.rstrip('/'), args.archive.rstrip('/'), args.network)
        # Same bound cache and rate buckets as Chats/history; distinct phase
        # lock permits both consumers while preventing duplicate proof workers.
        with (args.cache / 'output-evidence.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with connect(os.environ['PROTOCOL_DATABASE_URL']) as conn:
                check_source(conn, args.metrics_url, args.network, args.source_binding)
                resolver.refresh_network()
                consumer = EvidenceConsumer(args.network, source_id, resolver.resolve_outputs, resolver.resolve_sender)
                checked = time.monotonic()
                while running:
                    if time.monotonic() - checked >= 60:
                        check_source(conn, args.metrics_url, args.network, args.source_binding)
                        resolver.refresh_network()
                        checked = time.monotonic()
                    result = consumer.run_one(conn)
                    if result:
                        print(json.dumps({'consumer': CONSUMER, 'status': result}), flush=True)
                    if args.once:
                        break
                    if result is None:
                        time.sleep(0.25)
    finally:
        if resolver is not None:
            resolver.db.close()


if __name__ == '__main__':
    main()
