#!/usr/bin/env python3
"""Bounded read-only live K payload capture; never imports or modifies a service."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sqlite3
import subprocess
import time
from history import HEX64, atomic_json, CapacityPause

MAX_BYTES = 200 * 1024**2
MAX_SECONDS = 6 * 3600
INTERVAL = 300
SQL = """BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='15s';
WITH bounds AS (SELECT min(block_time) AS oldest,max(block_time) AS latest,count(*) AS raw_count FROM transactions),
k AS (SELECT transaction_id,payload,block_time FROM transactions WHERE substring(payload from 1 for 4)=decode('6b3a313a','hex')),
size AS (SELECT count(*) AS k_count,coalesce(sum(octet_length(payload)),0) AS payload_bytes FROM k)
SELECT json_build_object('at',now(),'network',(SELECT value FROM k_vars WHERE key='network'),
'raw_oldest',bounds.oldest,'raw_latest',bounds.latest,'raw_count',bounds.raw_count,
'k_count',size.k_count,'payload_bytes',size.payload_bytes,
'records',CASE WHEN size.k_count<=1000 AND size.payload_bytes<=5242880 THEN
(SELECT coalesce(json_agg(json_build_object('transaction_id',encode(transaction_id,'hex'),'payload',encode(payload,'hex'),'block_time',block_time) ORDER BY block_time,transaction_id),'[]'::json) FROM k)
ELSE '[]'::json END) FROM bounds,size;
COMMIT;
"""


def validate(snapshot, network, previous_latest):
    if snapshot.get("network") != network:
        raise ValueError("Live tail network mismatch")
    rows = snapshot.get("records", [])
    if len(rows) != snapshot["k_count"] or snapshot["payload_bytes"] > 5242880:
        raise ValueError("Snapshot exceeds bounded read size; continuity not advanced")
    oldest, latest = snapshot["raw_oldest"], snapshot["raw_latest"]
    if oldest is None or latest is None or oldest > latest:
        raise ValueError("Missing/inverted retained transaction range")
    seen = set()
    for row in rows:
        txid = row["transaction_id"]
        raw = bytes.fromhex(row["payload"])
        if not HEX64.fullmatch(txid) or txid in seen or not raw.startswith(b"k:1:"):
            raise ValueError("Invalid/duplicate K tail record")
        if not oldest <= row["block_time"] <= latest:
            raise ValueError("Tail event outside consistent retained range")
        seen.add(txid)
    return {
        "previous_latest": previous_latest,
        "retention_overlap": (
            None if previous_latest is None else oldest <= previous_latest <= latest
        ),
        "pre_capture_gap_unresolved": True,
        "complete_chain_tail_proven": False,
    }


def capture(args):
    args.output.mkdir(parents=True, exist_ok=True)
    config = {
        "server": args.server,
        "container": args.container,
        "network": args.network,
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "interval_seconds": INTERVAL,
        "max_seconds": MAX_SECONDS,
        "max_bytes": MAX_BYTES,
    }
    manifest = args.output / "manifest.json"
    if manifest.exists():
        existing = json.loads(manifest.read_text())
        if existing["configuration"] != config:
            raise ValueError("Tail capture source/config changed; use a new output")
    else:
        existing = {
            "configuration": config,
            "started_at": time.time(),
            "live_imported": 0,
        }
        manifest.write_text(json.dumps(existing, indent=2) + "\n")
    start = existing["started_at"]
    db = sqlite3.connect(args.output / "inventory.sqlite")
    db.execute(
        "CREATE TABLE IF NOT EXISTS records(txid TEXT PRIMARY KEY,payload TEXT,block_time INTEGER)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS snapshots(ordinal INTEGER PRIMARY KEY,latest INTEGER)"
    )
    prior = db.execute(
        "SELECT ordinal,latest FROM snapshots ORDER BY ordinal DESC LIMIT 1"
    ).fetchone()
    ordinal, previous_latest = prior if prior else (0, None)
    command = (
        "docker exec -i "
        + shlex.quote(args.container)
        + " psql -U social -d social -Atq -v ON_ERROR_STOP=1"
    )
    while time.time() - start < MAX_SECONDS:
        if shutil.disk_usage(args.output).free < 2 * 1024**3:
            raise CapacityPause("Tail capture paused: less than 2GiB free")
        if (
            sum(p.stat().st_size for p in args.output.iterdir() if p.is_file())
            >= MAX_BYTES
        ):
            raise CapacityPause("Tail capture reached200MiB evidence budget")
        result = subprocess.run(
            ["ssh", args.server, command],
            input=SQL,
            text=True,
            capture_output=True,
            timeout=45,
            check=True,
        )
        snapshot = json.loads(result.stdout)
        continuity = validate(snapshot, args.network, previous_latest)
        # Preserve full consistent read before advancing the local cursor.
        ordinal += 1
        snapshot.update(continuity=continuity, ordinal=ordinal, live_imported=0)
        path = args.output / f"snapshot-{ordinal:04d}.json.gz"
        if path.exists():
            raise ValueError(
                "Unledgered snapshot already exists; preserve and review before resume"
            )
        used = sum(p.stat().st_size for p in args.output.iterdir() if p.is_file())
        if used + 3 * len(json.dumps(snapshot).encode()) + 1024**2 > MAX_BYTES:
            raise CapacityPause("Tail capture would exceed200MiB evidence budget")
        atomic_json(path, snapshot)
        with db:
            for row in snapshot["records"]:
                prior_row = db.execute(
                    "SELECT payload,block_time FROM records WHERE txid=?",
                    (row["transaction_id"],),
                ).fetchone()
                if prior_row and prior_row != (row["payload"], row["block_time"]):
                    raise ValueError("Same live tail ID changed payload/time")
                db.execute(
                    "INSERT OR IGNORE INTO records VALUES(?,?,?)",
                    (row["transaction_id"], row["payload"], row["block_time"]),
                )
            db.execute(
                "INSERT INTO snapshots VALUES(?,?)", (ordinal, snapshot["raw_latest"])
            )
        previous_latest = snapshot["raw_latest"]
        print(
            json.dumps(
                {
                    "at": time.time(),
                    "snapshot": ordinal,
                    "k_rows": len(snapshot["records"]),
                    "unique_events": db.execute(
                        "SELECT count(*) FROM records"
                    ).fetchone()[0],
                    "retention_overlap": continuity["retention_overlap"],
                    "live_imported": 0,
                }
            ),
            flush=True,
        )
        if continuity["retention_overlap"] is False:
            raise ValueError(
                "Retained window continuity gap; snapshot preserved, stop for review"
            )
        time.sleep(min(INTERVAL, max(0, MAX_SECONDS - (time.time() - start))))
    db.close()
    print(
        json.dumps({"finished": "six-hour capture bound reached", "live_imported": 0}),
        flush=True,
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--server", required=True)
    p.add_argument("--container", required=True)
    p.add_argument("--network", choices=["mainnet", "testnet-10"], required=True)
    args = p.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_.@-]+", args.server) or not re.fullmatch(
        r"[a-zA-Z0-9_-]+", args.container
    ):
        raise ValueError("Invalid fixed server/container")
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "capture.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (args.output / "capture.pid").write_text(str(os.getpid()))
        capture(args)
