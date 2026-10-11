#!/usr/bin/env python3
"""Resumable, read-only mainnet history collection. Data stays in the supplied directory."""
import argparse
import concurrent.futures
import gzip
import fcntl
import hashlib
import json
import os
from pathlib import Path
import threading
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

SOURCES = {"kasia": "https://indexer.kasia.fyi", "kachat": "https://kachat.duckdns.org"}
PREFIXES = (b"ciph_msg:", b"kchat:")
LOCK = threading.Lock()


def rate_slot(url, cooldown=0):
    # Separate API buckets share a cross-process lock, so collection and
    # confirmation lookups cannot each consume the full allowance.
    if not url.startswith("https://api.kaspa.org/"):
        return
    bucket = "addresses" if "/addresses/" in url else "details"
    interval = 2.1 if bucket == "addresses" else 0.4
    filename = Path(os.environ.get("CHATS_RATE_DIR", ".")) / ("rate-" + bucket + ".txt")
    with filename.open("a+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.seek(0)
        next_at = float(stream.read() or 0)
        now = time.time()
        if cooldown:
            next_at = max(next_at, now + cooldown)
        else:
            time.sleep(max(0, next_at - now))
            next_at = time.time() + interval
        stream.seek(0); stream.truncate(); stream.write(str(next_at)); stream.flush()


def request(url):
    for attempt in range(12):
        try:
            rate_slot(url)
            req = urllib.request.Request(url, headers={"User-Agent": "Kasanova-History-Migration/1"})
            with urllib.request.urlopen(req, timeout=60) as response:
                return json.load(response), dict(response.headers)
        except urllib.error.HTTPError as error:
            if error.code not in (429, 500, 502, 503, 504):
                raise
            delay = min(300, float(error.headers.get("Retry-After", max(60, 2 ** attempt) if error.code == 429 else 2 ** attempt)))
            if error.code == 429:
                rate_slot(url, cooldown=delay)
        except (TimeoutError, OSError):
            delay = min(30, 2 ** attempt)
        if attempt == 11:
            raise RuntimeError("request retries exhausted")
        time.sleep(delay)


def atomic_json(path, value):
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    os.close(fd)
    tmp = Path(name)
    with gzip.open(tmp, "wt") as stream:
        json.dump(value, stream, separators=(",", ":"))
    os.replace(tmp, path)


def collect_explorer(address, root=None):
    rows = {}
    pages = scanned = 0
    before = 0
    checkpoint = None
    if root is not None:
        directory = root / "checkpoints"
        directory.mkdir(exist_ok=True)
        checkpoint = directory / (hashlib.sha256(address.encode()).hexdigest() + ".json.gz")
        if checkpoint.exists():
            with gzip.open(checkpoint, "rt") as stream:
                saved = json.load(stream)
            rows = {row["transaction_id"]: row for row in saved["rows"]}
            pages, scanned, before = saved["pages"], saved["scanned"], saved["before"]
            if saved["done"]:
                return {"rows": list(rows.values()), "pages": pages, "scanned": scanned}
    while True:
        query = {"limit": 500, "resolve_previous_outpoints": "light", "acceptance": "accepted"}
        if before:
            query["before"] = before
        url = "https://api.kaspa.org/addresses/" + address + "/full-transactions-page?" + urllib.parse.urlencode(query)
        page, headers = request(url)
        if not isinstance(page, list):
            raise ValueError("explorer returned a non-list")
        pages += 1
        scanned += len(page)
        for row in page:
            payload = bytes.fromhex(row.get("payload") or "")
            if payload.startswith(PREFIXES):
                txid = row["transaction_id"]
                old = rows.get(txid)
                if old and old["payload"] != row["payload"]:
                    raise ValueError("conflicting transaction payload")
                rows[txid] = row
        header = next((v for k, v in headers.items() if k.lower() == "x-next-page-before"), None)
        if header is None:
            if checkpoint is not None:
                atomic_json(checkpoint, {"rows":list(rows.values()),"pages":pages,"scanned":scanned,"before":before,"done":True})
            break
        next_before = int(header)
        if next_before <= 0 or (before and next_before >= before):
            raise ValueError("explorer pagination did not advance")
        before = next_before
        if checkpoint is not None:
            atomic_json(checkpoint, {"rows":list(rows.values()),"pages":pages,"scanned":scanned,"before":before,"done":False})
    return {"rows": list(rows.values()), "pages": pages, "scanned": scanned}


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
