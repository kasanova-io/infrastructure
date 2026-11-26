#!/usr/bin/env python3
"""Resumable, read-only mainnet history collection. Data stays in the supplied directory."""
import argparse
import concurrent.futures
import gzip
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import threading
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

SOURCES = {"kasia": "https://indexer.kasia.fyi", "kachat": "https://kachat.duckdns.org"}
PREFIXES = (b"ciph_msg:", b"kchat:")
LOCK = threading.Lock()


class PacingError(RuntimeError):
    """Operational pacing storage/configuration failure, never bad transaction evidence."""


class RatePending(OSError):
    """Caller elected to defer instead of waiting through a shared cooldown."""


def lock_rate_bucket(stream, max_wait=None):
    """Acquire a rate bucket within the same budget used for its pacing delay."""
    if max_wait is None:
        fcntl.flock(stream, fcntl.LOCK_EX)
        return None
    if not math.isfinite(max_wait) or max_wait < 0:
        raise PacingError('invalid rate wait budget')
    deadline = time.monotonic() + max_wait
    while True:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return deadline
        except BlockingIOError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RatePending('shared rate bucket busy')
            time.sleep(min(0.01, remaining))
            if time.monotonic() >= deadline:
                raise RatePending('shared rate bucket busy')


def wait_rate_delay(delay, deadline):
    if deadline is not None and delay > max(0, deadline - time.monotonic()):
        raise RatePending('shared archive cooldown')
    time.sleep(delay)
    if deadline is not None and time.monotonic() >= deadline:
        raise RatePending('shared rate wait budget exhausted')


def rate_slot(url, cooldown=0, max_wait=None, all_hosts=False, rate_root=None,
              rate_bucket=None, rate_interval=None, rate_guard=None):
    # Keep legacy direct callers' endpoint policy. Shared callers choose one
    # bound source/archive bucket for both cadence and server Retry-After.
    if rate_bucket is None and not all_hosts and not url.startswith("https://api.kaspa.org/"):
        return
    if rate_bucket is not None and rate_bucket not in {"archive", "source"}:
        raise PacingError('invalid shared rate bucket')
    if rate_interval is not None and (isinstance(rate_interval, bool)
            or not isinstance(rate_interval, (int, float)) or not math.isfinite(rate_interval)
            or not 0.4 <= rate_interval <= 60):
        raise PacingError('invalid shared rate interval')
    if rate_guard is not None and not callable(rate_guard):
        raise PacingError('invalid rate profile guard')
    bucket = rate_bucket or ("addresses" if "/addresses/" in url else "details")
    floor = 2.1 if "/addresses/" in url and bucket != "source" else 0.4
    interval = max(floor, rate_interval if rate_interval is not None else floor)
    filename = Path(rate_root if rate_root is not None else os.environ.get("CHATS_RATE_DIR", ".")) / (bucket + "-rate.txt" if rate_bucket is not None else "rate-" + bucket + ".txt")
    first, deadline = True, None
    while True:
        if rate_guard is not None:
            rate_guard()
        remaining = max_wait if first or deadline is None else max(0, deadline - time.monotonic())
        if not first and deadline is not None and remaining <= 0:
            raise RatePending('shared rate wait budget exhausted')
        with filename.open("a+") as stream:
            acquired_deadline = lock_rate_bucket(stream, remaining)
            deadline = acquired_deadline if first or deadline is None else min(deadline, acquired_deadline)
            first = False
            stream.seek(0)
            try:
                next_at = float(stream.read() or 0)
                if not math.isfinite(next_at):raise ValueError('nonfinite rate counter')
            except ValueError as error:
                raise PacingError('invalid stored rate counter') from error
            now = time.time()
            delay = 0 if cooldown else max(0, next_at - now)
            if cooldown:
                # A sleeper never owns this lock: bounded 429 callers can record
                # the provider's deadline without waiting behind its cooldown.
                next_at = max(next_at, now + cooldown)
            elif delay <= 0:
                wait_rate_delay(0, deadline)
                next_at = time.time() + interval
            if cooldown or delay <= 0:
                stream.seek(0); stream.truncate(); stream.write(str(next_at)); stream.flush()
                return
        wait_rate_delay(delay, deadline)


def request(url, *, attempts=12, timeout=60, max_wait=None, rate_all_hosts=False, max_bytes=None, data=None, rate_root=None, rate_bucket=None, rate_interval=None, rate_guard=None):
    if not 1 <= attempts <= 12 or not 0 < timeout <= 60:
        raise ValueError('invalid bounded request policy')
    if data is not None and (not isinstance(data, bytes) or len(data) > 65536):
        raise ValueError('invalid bounded JSON request body')
    rate_options = {"max_wait": max_wait, "all_hosts": rate_all_hosts}
    for key,value in [("rate_root",rate_root),("rate_bucket",rate_bucket),("rate_interval",rate_interval),("rate_guard",rate_guard)]:
        if value is not None:rate_options[key]=value
    def paced_slot(**extra):
        try:
            rate_slot(url, **rate_options, **extra)
            if rate_guard is not None:rate_guard()
        except RatePending:
            raise
        except OSError as error:
            raise PacingError('rate storage unavailable') from error
    for attempt in range(attempts):
        try:
            paced_slot()
            headers = {"User-Agent": "Kasanova-History-Migration/1"}
            if data is not None:
                headers.update({"Content-Type": "application/json", "Cache-Control": "no-cache"})
            req = urllib.request.Request(url, headers=headers, data=data)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                if max_bytes is not None:
                    response_data = response.read(max_bytes + 1)
                    if len(response_data) > max_bytes:
                        raise ValueError('archive response exceeds configured bound')
                    return json.loads(response_data), dict(response.headers)
                return json.load(response), dict(response.headers)
        except urllib.error.HTTPError as error:
            try:
                if error.code not in (429, 500, 502, 503, 504):
                    raise
                try:
                    delay = float(error.headers.get("Retry-After", max(60, 2 ** attempt) if error.code == 429 else 2 ** attempt))
                    if not math.isfinite(delay):raise ValueError('nonfinite Retry-After')
                    delay = max(0, min(300, delay))
                except (ValueError, TypeError) as invalid:
                    raise PacingError('invalid server retry policy') from invalid
                if error.code == 429:
                    try:paced_slot(cooldown=delay)
                    except RatePending as pending:
                        raise PacingError('unable to persist server cooldown within budget') from pending
            finally:
                error.close()
        except (TimeoutError, OSError):
            delay = min(30, 2 ** attempt)
        if attempt == attempts - 1:
            raise RuntimeError("request retries exhausted")
        time.sleep(delay)


def atomic_json(path, value):
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    os.close(fd)
    tmp = Path(name)
    with gzip.open(tmp, "wt") as stream:
        json.dump(value, stream, separators=(",", ":"))
    os.replace(tmp, path)


def collect_explorer(address, root=None, *, archive="https://api.kaspa.org",
                     request_fn=None, accept_payload=None, checkpoint_scope=None,
                     page_budget=0, limit=500):
    """Reuse the address scanner with separately bound multi-protocol checkpoints.

    Legacy callers retain their exact filter/checkpoint/result contract. New
    callers must name their network and filter; a completed chat-only scan must
    never stand in for a six-family scan or a scan of another archive.
    """
    archive = archive.rstrip("/")
    if type(page_budget) is not int or page_budget < 0 or type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError("invalid address page bounds")
    binding = None
    if checkpoint_scope is not None:
        if (root is None or not isinstance(checkpoint_scope, dict)
                or checkpoint_scope.get("archive") != archive
                or not re.fullmatch(r"mainnet|testnet-[0-9]+", str(checkpoint_scope.get("network", "")))
                or not isinstance(checkpoint_scope.get("filter"), str)
                or not checkpoint_scope["filter"]):
            raise ValueError("address checkpoint requires archive/network/filter binding")
        binding = {**checkpoint_scope, "address": address, "limit": limit}
    elif archive != "https://api.kaspa.org" or accept_payload is not None or page_budget or limit != 500:
        raise ValueError("custom address scan requires scoped checkpoints")
    fetch = request_fn or request
    accepts = accept_payload or (lambda payload: payload.startswith(PREFIXES))
    rows = {}
    pages = scanned = 0
    before = 0
    checkpoint = None

    def result(done):
        value = {"rows": list(rows.values()), "pages": pages, "scanned": scanned}
        if binding is not None:
            value.update(done=done, before=before)
        return value

    def save(done):
        if checkpoint is not None:
            value = {**result(done), "before": before, "done": done}
            if binding is not None:
                value["binding"] = binding
            atomic_json(checkpoint, value)

    if root is not None:
        directory = Path(root) / "checkpoints"
        if binding is not None:
            directory /= hashlib.sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest()
        directory.mkdir(parents=True, exist_ok=True)
        checkpoint = directory / (hashlib.sha256(address.encode()).hexdigest() + ".json.gz")
        if checkpoint.exists():
            with gzip.open(checkpoint, "rt") as stream:
                saved = json.load(stream)
            if binding is not None and saved.get("binding") != binding:
                raise ValueError("address checkpoint binding mismatch")
            rows = {row["transaction_id"]: row for row in saved["rows"]}
            pages, scanned, before = saved["pages"], saved["scanned"], saved["before"]
            if saved["done"]:
                return result(True)
    started_pages = pages
    while True:
        query = {"limit": limit, "resolve_previous_outpoints": "light", "acceptance": "accepted"}
        if before:
            query["before"] = before
        url = archive + "/addresses/" + urllib.parse.quote(address, safe=":") + "/full-transactions-page?" + urllib.parse.urlencode(query)
        page, headers = fetch(url)
        if not isinstance(page, list):
            raise ValueError("explorer returned a non-list")
        pages += 1
        scanned += len(page)
        for row in page:
            payload = bytes.fromhex(row.get("payload") or "")
            if accepts(payload):
                txid = row["transaction_id"]
                old = rows.get(txid)
                if old and old["payload"] != row["payload"]:
                    raise ValueError("conflicting transaction payload")
                rows[txid] = row
        header = next((v for k, v in headers.items() if k.lower() == "x-next-page-before"), None)
        if header is None:
            save(True)
            return result(True)
        next_before = int(header)
        if next_before <= 0 or (before and next_before >= before):
            raise ValueError("explorer pagination did not advance")
        before = next_before
        save(False)
        if page_budget and pages - started_pages >= page_budget:
            return result(False)


def collect_indexer(source, address, endpoint, extra=None):
    rows = {}
    cursor = pages = 0
    opaque = None
    while True:
        query = {"address": address, "limit": 50, **(extra or {})}
        if opaque:
            query["cursor"] = opaque
        else:
            query["block_time"] = cursor
        page, _ = request(SOURCES[source] + endpoint + "?" + urllib.parse.urlencode(query))
        if not isinstance(page, list):
            raise ValueError("indexer returned a non-list")
        pages += 1
        for row in page:
            txid = row["tx_id"]
            old = rows.get(txid)
            if old and old.get("message_payload", old.get("message")) != row.get("message_payload", row.get("message")):
                raise ValueError("conflicting indexer payload")
            rows[txid] = row
        if len(page) < 50:
            break
        if page[-1].get("cursor"):
            next_opaque = page[-1]["cursor"]
            if next_opaque == opaque:
                raise ValueError("indexer opaque cursor did not advance")
            opaque = next_opaque
            continue
        next_cursor = max(int(row["block_time"]) for row in page)
        # Cursor is inclusive. Never skip a timestamp to conceal a full-page tie.
        if next_cursor <= cursor:
            raise ValueError("indexer timestamp pagination saturated; archival replay required")
        cursor = next_cursor
    return {"rows": list(rows.values()), "pages": pages}


def run_job(root, job):
    source, address, endpoint, extra = job
    key = hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()
    path = root / "results" / (key + ".json.gz")
    if path.exists():
        return True
    try:
        value = collect_explorer(address, root) if source == "explorer" else collect_indexer(source, address, endpoint, extra)
        atomic_json(path, {"job": job, **value})
        return True
    except Exception as error:
        # No address lists or payloads in progress logs. Job hashes resolve locally.
        with LOCK:
            with (root / "errors.jsonl").open("a") as stream:
                stream.write(json.dumps({"job_hash": key, "source": source, "endpoint": endpoint, "error": str(error)}) + "\n")
        return False


def run_jobs(root, jobs, workers):
    done = failed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(run_job, root, job) for job in jobs]
        for future in concurrent.futures.as_completed(futures):
            done += 1
            failed += not future.result()
            if done % 100 == 0 or done == len(jobs):
                print(json.dumps({"done": done, "total": len(jobs), "failed": failed}), flush=True)
    return failed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()
    os.umask(0o077)
    root = args.directory
    (root / "results").mkdir(exist_ok=True)
    addresses = sorted(set(root.joinpath("addresses.txt").read_text().splitlines()))
    assert addresses and all(a.startswith("kaspa:") for a in addresses)
    jobs = []
    for address in addresses:
        jobs.append(("explorer", address, "", None))
        for source in SOURCES:
            for endpoint in ("/handshakes/by-receiver", "/handshakes/by-sender", "/payments/by-receiver", "/payments/by-sender"):
                jobs.append((source, address, endpoint, None))
        jobs.append(("kachat", address, "/group-control/by-recipient", {"recipient": address}))
        jobs.append(("kachat", address, "/group-control/by-sender", {"sender": address}))
    atomic_json(root / "jobs.json.gz", jobs)
    # Finish cheap source reads even if the archival endpoint is throttled.
    jobs.sort(key=lambda job: job[0] == "explorer")
    failed = run_jobs(root, jobs, args.workers)
    # Conversations are self-send: incoming bodies live under the counterparty's
    # address. Include one hop of peers established by our users' handshakes.
    peers = set()
    for path in (root / "results").glob("*.json.gz"):
        with gzip.open(path, "rt") as stream:
            result = json.load(stream)
        if result["job"][2].startswith(("/handshakes/", "/group-control/")):
            for row in result["rows"]:
                peers.update(a for a in (row.get("sender"), row.get("receiver") or row.get("recipient")) if a and a.startswith("kaspa:"))
        elif result["job"][0] == "explorer" and result["job"][1] in addresses:
            for row in result["rows"]:
                raw = bytes.fromhex(row["payload"]).split(b":", 1)[1]
                if raw.startswith(b"1:") and not raw.startswith(b"1:handshake:"):
                    continue
                inputs = sorted(row.get("inputs", []), key=lambda item: item["index"])
                sender = inputs[0].get("previous_outpoint_address") if inputs else None
                outputs = sorted(row.get("outputs", []), key=lambda item: item["index"])
                recipients = [o.get("script_public_key_address") for o in outputs if o.get("script_public_key_address")]
                recipient = next((a for a in recipients if a != sender), recipients[0] if recipients else None)
                peers.update(a for a in (sender, recipient) if a and a.startswith("kaspa:"))
    peers.difference_update(addresses)
    root.joinpath("peers.txt").write_text("".join(a + "\n" for a in sorted(peers)))
    print(json.dumps({"addresses": len(addresses), "conversation_peers": len(peers)}), flush=True)
    failed += run_jobs(root, [("explorer", a, "", None) for a in sorted(peers)], args.workers)
    root.joinpath("collection-status.json").write_text(json.dumps({"addresses": len(addresses), "peers": len(peers), "failed_jobs": failed}))
    return bool(failed)


if __name__ == "__main__":
    raise SystemExit(main())
