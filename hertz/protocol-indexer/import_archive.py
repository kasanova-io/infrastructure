#!/usr/bin/env python3
"""Import checked external history quietly through the same canonical journal.

Requires the reviewed source's live removal and archive-sentinel override hooks.
Reads an immutable archive_backfill export; rerunning preserves canonical authority.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
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
from protocol_indexer.backfill_batch import BatchedObservations
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

    def refresh_network(self):
        try:
            return super().refresh_network()
        except (worker.history._chats.RatePending, BlockingIOError, TimeoutError) as exc:
            # A shared cooldown/busy rate bucket is outstanding work. Do not
            # confuse it with a network mismatch or a cache/storage failure.
            raise EvidencePending('archive_network_deferred') from exc

    def request(self, url, *, data=None):
        # Socket timeouts do not bound a slow stream or a redirect chain. This
        # standalone POSIX worker owns its alarm; expiry unwinds the whole read.
        if threading.current_thread() is not threading.main_thread() or signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
            raise RuntimeError('archive importer requires an exclusive request deadline')
        previous = signal.getsignal(signal.SIGALRM)
        def expired(*_): raise TimeoutError('archive request deadline')
        signal.signal(signal.SIGALRM, expired)
        try:
            signal.setitimer(signal.ITIMER_REAL, self.request_deadline_seconds)
            if data is None:
                return super().request(url)
            return super().request(url, data=data)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)


def current_observation(resolver, receipt):
    try:
        detail = resolver.request(resolver.archive + '/transactions/' + receipt['transaction_id'] + '?resolve_previous_outpoints=light')
    except OSError as exc:
        raise EvidencePending('archive_current_acceptance_unavailable') from exc
    return observation_from_detail(resolver.network, receipt, detail)


def observation_from_detail(network, receipt, detail):
    if not isinstance(detail, dict) or type(detail.get('is_accepted')) is not bool:
        raise EvidencePending('archive_current_acceptance_shape')
    block = detail.get('accepting_block_hash') if detail['is_accepted'] else None
    if detail['is_accepted'] and (not isinstance(block, str) or not HEX64.fullmatch(block)):
        raise EvidencePending('archive_current_acceptance_lag')
    observation = AcceptanceObservation(network, detail.get('transaction_id'), detail.get('payload'), block)
    observation.checked(receipt, network)
    return observation


def current_observations(resolver, receipts, budget):
    """Read only this queued group; never cache a response for later callbacks."""
    ids = {receipt['transaction_id'] for receipt in receipts}
    if (not 1 <= len(ids) <= 8 or any(not isinstance(txid, str) or not HEX64.fullmatch(txid) for txid in ids)
            or isinstance(budget, bool) or not isinstance(budget, (int, float)) or not 0 < budget <= 12):
        raise ValueError('invalid bounded archive read group')
    body = json.dumps({'transactionIds': sorted(ids)}, allow_nan=False).encode()
    prior_deadline = resolver.request_deadline_seconds
    resolver.request_deadline_seconds = min(prior_deadline, budget)
    try:
        try:
            rows = resolver.request(resolver.archive + '/transactions/search?fields=transaction_id,payload,is_accepted,accepting_block_hash', data=body)
        except OSError as exc:
            if hasattr(exc, 'close'):
                exc.close()
            raise EvidencePending('archive_current_acceptance_unavailable') from exc
    finally:
        resolver.request_deadline_seconds = prior_deadline
    if not isinstance(rows, list) or len(rows) > len(ids):
        raise EvidencePending('archive_batch_acceptance_shape')
    found = {}
    for row in rows:
        txid = row.get('transaction_id') if isinstance(row, dict) else None
        if not isinstance(txid, str) or txid not in ids or txid in found:
            raise EvidencePending('archive_batch_acceptance_identity')
        found[txid] = row
    # Missing IDs stay unavailable. They never become rejection observations.
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--network', required=True)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--cache', type=Path, required=True)
    parser.add_argument('--shared-rate-root', type=Path,
                        default=os.environ.get('PROTOCOL_SHARED_RATE_DIRECTORY') or None)
    parser.add_argument('--batch', type=Path, required=True)
    parser.add_argument('--limit', type=int, default=500)
    parser.add_argument('--after-line', type=int, default=0, help='resume at next_after_line from the previous result')
    parser.add_argument('--workers', type=int, default=1,
                        help='1 keeps individual reads; 2..8 coalesce current-acceptance reads through the archive search API')
    parser.add_argument('--metrics-url', default=os.environ.get('SIMPLY_METRICS_URL', 'http://indexer:8501/api/metrics'))
    parser.add_argument('--source-binding', default=os.environ.get('PROTOCOL_SOURCE_BINDING_FILE', '/run/protocol-indexer/source-binding.json'))
    args = parser.parse_args()
    parsed = urllib.parse.urlsplit(args.archive)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment
        or not 1 <= args.limit <= 10000 or args.after_line < 0 or not 1 <= args.workers <= 8
        or not os.environ.get('PROTOCOL_DATABASE_URL')):
        parser.error('explicit HTTPS archive, database configuration, limit1..10000 and workers1..8 required')
    os.umask(0o077)
    # No HTTP or historical source writes occur before the opt-in capability gate.
    with connect(os.environ['PROTOCOL_DATABASE_URL']) as conn:
        check_source(conn, args.metrics_url, args.network, args.source_binding, require_archive_import=True)
        resolver = ImportResolver(args.cache, args.archive, args.archive, args.network,
                                  shared_rate_root=args.shared_rate_root)
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
                if args.workers == 1:
                    for _ in range(args.limit):
                        outcome = deliver_one(conn, batch_sha256, args.network, lambda receipt: current_observation(resolver, receipt))
                        if outcome is None: break
                        counts[outcome['status']] += 1
                        print(json.dumps(outcome), flush=True)
                else:
                    with ExitStack() as stack:
                        connections = [conn]
                        for _ in range(args.workers - 1):
                            extra = stack.enter_context(connect(os.environ['PROTOCOL_DATABASE_URL']))
                            check_source(extra, args.metrics_url, args.network, args.source_binding, require_archive_import=True)
                            connections.append(extra)
                        observer = BatchedObservations(
                            lambda values, budget: current_observations(resolver, values, budget),
                            lambda receipt, detail: observation_from_detail(args.network, receipt, detail),
                            timeout=resolver.request_deadline_seconds)
                        with ThreadPoolExecutor(max_workers=args.workers) as pool:
                            try:
                                for start in range(0, args.limit, args.workers):
                                    # Refresh only between completed waves; no extra network
                                    # operation can delay an outstanding fenced observation.
                                    if time.monotonic() - checked >= 60:
                                        check_source(conn, args.metrics_url, args.network, args.source_binding, require_archive_import=True)
                                        try:
                                            resolver.refresh_network()
                                        except EvidencePending:
                                            break
                                        checked = time.monotonic()
                                    jobs = [pool.submit(deliver_one, connection, batch_sha256, args.network, observer.observe)
                                            for connection in connections[:min(args.workers, args.limit - start)]]
                                    observer.dispatch(jobs)
                                    outcomes = []
                                    error = None
                                    for job in jobs:
                                        try:
                                            outcomes.append(job.result())
                                        except Exception as exc:
                                            error = error or exc
                                    for outcome in outcomes:
                                        if outcome is not None:
                                            counts[outcome['status']] += 1
                                            print(json.dumps(outcome), flush=True)
                                    if error is not None:
                                        raise error
                                    if any(outcome is None for outcome in outcomes):
                                        break
                            finally:
                                observer.close()
                pending = conn.execute('SELECT count(*) AS count FROM protocol_indexer.archive_jobs WHERE batch_sha256=%s', (batch_sha256,)).fetchone()['count']
                print(json.dumps({'counts': counts, 'historical_notifications': 0,
                                  'batch_sha256': batch_sha256, 'next_after_line': cursor,
                                  'end_of_batch': ended, 'staged': staged, 'pending': pending}), flush=True)
                if pending: raise SystemExit(2)
        finally:
            resolver.db.close()


if __name__ == '__main__':
    main()
