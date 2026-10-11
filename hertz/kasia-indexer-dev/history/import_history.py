#!/usr/bin/env python3
"""Import prepared mainnet ciphertext through the private transactional endpoint."""
import argparse
import gzip
import fcntl
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request
from collect import atomic_json
from prepare import load

TARGET = "http://127.0.0.1:18082"


def post(rows):
    data = json.dumps(rows, separators=(",", ":")).encode()
    for attempt in range(5):
        try:
            req = urllib.request.Request(TARGET + "/history-import", data=data,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=90) as response:
                result = json.load(response)
            if result["imported"] + result["duplicates"] != len(rows):
                raise ValueError("import count mismatch")
            return result
        except urllib.error.HTTPError as error:
            detail = error.read().decode()
            if error.code not in (500, 502, 503, 504) and "conflicted" not in detail:
                raise ValueError(detail)
        except (TimeoutError, OSError):
            pass
        if attempt == 4:
            raise RuntimeError("import retries exhausted")
        time.sleep(2 ** attempt)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    os.umask(0o077)
    root = args.directory
    # Serialize manual backlog imports and the recovery continuation. Native
    # transaction deduplication remains authoritative; this also protects journals.
    import_lock = (root / ".history-import.lock").open("a")
    fcntl.flock(import_lock.fileno(), fcntl.LOCK_EX)
    (root / "imported").mkdir(exist_ok=True)
    prepared = sorted((load(path) for path in (root / "prepared").glob("*.json.gz")),
                      key=lambda row: (row["transaction"]["block_time"], row["transaction"]["tx_id"]))
    pending = [row for row in prepared if not (root / "imported" / (row["transaction"]["tx_id"] + ".json.gz")).exists()]
    if args.limit:
        pending = pending[:args.limit]
    successes = imported = duplicates = 0
    errors = []
    for offset in range(0, len(pending), 100):
        batch = pending[offset:offset + 100]
        try:
            result = post([row["transaction"] for row in batch])
            imported += result["imported"]
            duplicates += result["duplicates"]
            good = batch
        except Exception:
            # Whole-batch rollback permits isolating failures without partial ambiguity.
            good = []
            for row in batch:
                try:
                    result = post([row["transaction"]])
                    imported += result["imported"]
                    duplicates += result["duplicates"]
                    good.append(row)
                except Exception as error:
                    errors.append({"tx_id": row["transaction"]["tx_id"], "error": str(error)})
        for row in good:
            atomic_json(root / "imported" / (row["transaction"]["tx_id"] + ".json.gz"), row)
        successes += len(good)
        if offset % 500 == 0:
            print(json.dumps({"processed": min(offset + 100, len(pending)), "pending_total": len(pending),
                              "successful": successes, "errors": len(errors)}), flush=True)
    atomic_json(root / "import-errors.json.gz", errors)
    status = {"prepared": len(prepared), "attempted": len(pending), "successful": successes,
              "newly_imported": imported, "duplicates": duplicates, "errors": len(errors),
              "total_journaled": len(list((root / "imported").glob("*.json.gz")))}
    root.joinpath("import-status.json").write_text(json.dumps(status))
    print(json.dumps(status), flush=True)
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
