"""Coalesce fresh HTTP observations without changing per-record import locks.

Every call (including the post-lock recheck) gets a new queued request. A group
is fixed before HTTP starts; requests arriving during I/O belong to a later
lookup. SQL stays in the unchanged deliver_one/import_receipt transactions.
"""
from concurrent.futures import Future, TimeoutError as FutureTimeout
import queue
import threading
import time

from .chats import EvidencePending


class BatchedObservations:
    def __init__(self, fetch, interpret, *, timeout=12, window=0.02):
        if not 0 < timeout <= 12 or not 0 <= window <= 0.1:
            raise ValueError('invalid bounded archive observation timing')
        self.fetch, self.interpret = fetch, interpret
        self.timeout, self.window = timeout, window
        self.pending = queue.Queue()
        self.lock = threading.Lock()
        self.active = {}
        self.closed = False

    def finish(self, future, *, value=None, error=None):
        with self.lock:
            if not future.done():
                if error is None:
                    future.set_result(value)
                else:
                    future.set_exception(error)

    def observe(self, receipt):
        future = Future()
        deadline = time.monotonic() + self.timeout
        request = (receipt, future, deadline)
        with self.lock:
            if self.closed:
                raise EvidencePending('archive_observation_coordinator_closed')
            self.active[future] = request
            self.pending.put(request)
        try:
            try:
                detail = future.result(timeout=self.timeout)
            except FutureTimeout as exc:
                self.finish(future, error=EvidencePending('archive_observation_deadline'))
                raise EvidencePending('archive_observation_deadline') from exc
            if time.monotonic() >= deadline:
                raise EvidencePending('archive_observation_deadline')
            return self.interpret(receipt, detail)
        finally:
            with self.lock:
                self.active.pop(future, None)

    def dispatch(self, jobs):
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError('archive HTTP dispatcher requires the main-thread deadline')
        while not all(job.done() for job in jobs):
            try:
                first = self.pending.get(timeout=0.02)
            except queue.Empty:
                continue
            requests = [first]
            until = time.monotonic() + self.window
            while len(requests) < 8:
                try:
                    requests.append(self.pending.get(timeout=max(0, until - time.monotonic())))
                except queue.Empty:
                    break
            live = []
            for receipt, future, deadline in requests:
                if future.done():
                    continue
                if time.monotonic() >= deadline:
                    self.finish(future, error=EvidencePending('archive_observation_deadline'))
                else:
                    live.append((receipt, future, deadline))
            if not live:
                continue
            budget = min(deadline for _, _, deadline in live) - time.monotonic()
            if budget <= 0:
                for _, future, _ in live:
                    self.finish(future, error=EvidencePending('archive_observation_deadline'))
                continue
            try:
                # Fix the group before I/O; never attach a later request to
                # this response, even when it refers to the same transaction.
                rows = self.fetch([receipt for receipt, _, _ in live], budget)
                if not isinstance(rows, dict):
                    raise ValueError('invalid archive batch response')
            except (EvidencePending, OSError):
                for _, future, _ in live:
                    self.finish(future, error=EvidencePending('archive_batch_lookup_unavailable'))
            except ValueError:
                for _, future, _ in live:
                    self.finish(future, error=ValueError('invalid archive batch response'))
            else:
                for receipt, future, deadline in live:
                    if time.monotonic() >= deadline:
                        self.finish(future, error=EvidencePending('archive_observation_deadline'))
                    else:
                        self.finish(future, value=rows.get(receipt['transaction_id']))

    def close(self):
        with self.lock:
            self.closed = True
            for future in self.active:
                if not future.done():
                    future.set_exception(EvidencePending('archive_observation_coordinator_closed'))
