#!/usr/bin/env python3
"""Import checked external history quietly through the same canonical journal.

Requires the reviewed source's live removal and archive-sentinel override hooks.
Reads an immutable archive_backfill export; rerunning preserves canonical authority.
"""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import threading
import time
import urllib.parse

from protocol_indexer.archive import HEX64, checked_receipt
from protocol_indexer.backfill import AcceptanceObservation, stage_receipt, deliver_one
from protocol_indexer.chats import EvidencePending
from protocol_indexer.cli import check_source
from protocol_indexer.journal import connect

spec = importlib.util.spec_from_file_location('shared_archive_delivery', Path(__file__).with_name('chats_worker.py'))
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)
Resolver = worker.Resolver


class ImportResolver(Resolver):
    # A current-acceptance read occurs under the source's VCP fence. Never let
    # legacy HTTP retries or shared cooldown waits hold live source locks.
    request_max_wait = 2
    request_options = {'attempts': 1, 'timeout': 10, 'max_wait': 2, 'rate_all_hosts': True, 'max_bytes': 8 * 1024**2}
    request_deadline_seconds = 12

    def request(self, url):
        # Socket timeouts do not bound a slow stream or a redirect chain. This
        # standalone POSIX worker owns its alarm; expiry unwinds the whole read.
        if threading.current_thread() is not threading.main_thread() or signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
            raise RuntimeError('archive importer requires an exclusive request deadline')
        previous = signal.getsignal(signal.SIGALRM)
        def expired(*_): raise TimeoutError('archive request deadline')
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, self.request_deadline_seconds)
        try:
            return super().request(url)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)


def current_observation(resolver, receipt):
    try:
        detail = resolver.request(resolver.archive + '/transactions/' + receipt['transaction_id'] + '?resolve_previous_outpoints=light')
    except OSError as exc:
        raise EvidencePending('archive_current_acceptance_unavailable') from exc
    if not isinstance(detail, dict) or type(detail.get('is_accepted')) is not bool:
        raise EvidencePending('archive_current_acceptance_shape')
    block = detail.get('accepting_block_hash') if detail['is_accepted'] else None
    if detail['is_accepted'] and (not isinstance(block, str) or not HEX64.fullmatch(block)):
        raise EvidencePending('archive_current_acceptance_lag')
    observation = AcceptanceObservation(resolver.network, detail.get('transaction_id'), detail.get('payload'), block)
    observation.checked(receipt, resolver.network)
    return observation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--network', required=True)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--cache', type=Path, required=True)
    parser.add_argument('--batch', type=Path, required=True)
    parser.add_argument('--limit', type=int, default=500)
    parser.add_argument('--after-line', type=int, default=0, help='resume at next_after_line from the previous result')
    parser.add_argument('--metrics-url', default=os.environ.get('SIMPLY_METRICS_URL', 'http://indexer:8501/api/metrics'))
    parser.add_argument('--source-binding', default=os.environ.get('PROTOCOL_SOURCE_BINDING_FILE', '/run/protocol-indexer/source-binding.json'))
    args = parser.parse_args()
    parsed = urllib.parse.urlsplit(args.archive)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment
        or not 1 <= args.limit <= 10000 or args.after_line < 0 or not os.environ.get('PROTOCOL_DATABASE_URL')):
        parser.error('explicit HTTPS archive, database configuration and limit1..10000 required')
    os.umask(0o077)
    # No HTTP or historical source writes occur before the opt-in capability gate.
    with connect(os.environ['PROTOCOL_DATABASE_URL']) as conn:
        check_source(conn, args.metrics_url, args.network, args.source_binding, require_archive_import=True)
        resolver = ImportResolver(args.cache, args.archive, args.archive, args.network)
        try:
            with (resolver.root / 'archive-import.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                resolver.refresh_network()
                checked = time.monotonic()
                counts = {'imported': 0, 'existing': 0, 'live_pending': 0, 'deferred': 0}
                with args.batch.open('rb') as stream:
                    batch_sha256 = hashlib.file_digest(stream, 'sha256').hexdigest()
                cursor = args.after_line
                ended = False
                staged = 0
                with args.batch.open('rb') as stream:
                    for number, line in enumerate(stream, 1):
                        if number <= args.after_line: continue
                        if staged >= args.limit: break
                        cursor = number
                        if not line.strip(): continue
                        if time.monotonic() - checked >= 60:
                            check_source(conn, args.metrics_url, args.network, args.source_binding, require_archive_import=True)
                            resolver.refresh_network(); checked = time.monotonic()
                        receipt = json.loads(line)
                        receipt = checked_receipt(receipt, receipt['transaction_id'], args.network)
                        stage_receipt(conn, batch_sha256, number, receipt, args.network)
                        staged += 1
                    else:
                        ended = True
                for _ in range(args.limit):
                    outcome = deliver_one(conn, batch_sha256, args.network, lambda receipt: current_observation(resolver, receipt))
                    if outcome is None: break
                    counts[outcome['status']] += 1
                    print(json.dumps(outcome), flush=True)
                pending = conn.execute('SELECT count(*) AS count FROM protocol_indexer.archive_jobs WHERE batch_sha256=%s', (batch_sha256,)).fetchone()['count']
                print(json.dumps({'counts': counts, 'historical_notifications': 0,
                                  'batch_sha256': batch_sha256, 'next_after_line': cursor,
                                  'end_of_batch': ended, 'staged': staged, 'pending': pending}), flush=True)
                if pending: raise SystemExit(2)
        finally:
            resolver.db.close()


if __name__ == '__main__':
    main()
