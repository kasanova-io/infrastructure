#!/usr/bin/env python3
"""Schedule the existing source-bound CLI pruner; no scanner or acknowledgement."""
import argparse
import json
import os
import signal
import subprocess
import threading
import time


class Stopped(Exception):
    pass


class Deferred(Exception):
    pass


def run_command(argv, stopped, timeout=75):
    child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.monotonic() + timeout
    try:
        while True:
            if stopped.is_set():
                raise Stopped()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise Deferred('Source retention deadline exceeded')
            try:
                stdout, _ = child.communicate(timeout=min(1, remaining))
                break
            except subprocess.TimeoutExpired:
                continue
        if child.returncode:
            # SQL/driver diagnostics may contain credentials or source data.
            raise Deferred('Source retention command deferred')
        return stdout
    finally:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
        if child.stdout:
            child.stdout.close()
        if child.stderr:
            child.stderr.close()


def run_pass(network, hours, batches, stopped):
    total = 0
    for batch in range(batches):
        # The existing CLI checks network/physical source binding before each
        # atomic pruning batch. Its guards retain pending work and canonical
        # protocol/acceptance evidence. Never enable Simply's own pruner.
        raw = run_command(['python', '-m', 'protocol_indexer.cli', '--network', network,
                           'prune', '--retention-hours', str(hours)], stopped)
        try:
            count = json.loads(raw)['raw_transactions_pruned']
        except (ValueError, KeyError, TypeError):
            raise Deferred('Invalid source pruning acknowledgement') from None
        if type(count) is not int or not 0 <= count <= 10000:
            raise Deferred('Invalid source pruning acknowledgement')
        total += count
        if count < 10000:
            break
    return {'network': network, 'retention_hours': hours, 'batches': batch + 1,
            'raw_transactions_pruned': total}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--network', required=True)
    parser.add_argument('--retention-hours', type=int, default=24)
    parser.add_argument('--interval-seconds', type=int, default=300)
    parser.add_argument('--max-batches', type=int, default=12)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if (not 1 <= args.retention_hours <= 8760 or not 10 <= args.interval_seconds <= 86400
            or not 1 <= args.max_batches <= 100):
        parser.error('Bounded retention, interval and batch count required')
    # Per-process libpq deadlines also bound SQL after an interrupted client.
    # The environment belongs only to this maintenance process and its child.
    os.environ['PGCONNECT_TIMEOUT'] = '10'
    os.environ['PGOPTIONS'] = '-c statement_timeout=45000 -c lock_timeout=3000'
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    while not stopped.is_set():
        try:
            result = run_pass(args.network, args.retention_hours, args.max_batches, stopped)
            print(json.dumps({'status': 'pruned', **result}), flush=True)
        except Stopped:
            break
        except (Deferred, OSError):
            print(json.dumps({'status': 'deferred', 'network': args.network}), flush=True)
            if args.once:
                raise SystemExit(1) from None
        if args.once:
            break
        stopped.wait(args.interval_seconds)


if __name__ == '__main__':
    main()
