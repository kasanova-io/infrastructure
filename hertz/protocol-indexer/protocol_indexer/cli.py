"""Private worker/maintenance commands. Does not expose a public import API."""

import argparse
import json
import os
import signal
import time
import urllib.request
from urllib.parse import unquote, urlsplit

from .journal import SIMPLY_REVISION, BackfillAdapter, ScanAdapter, capture_one, connect, discard_unrelated, install, prune_source
from .routing import Family


def validate_source(metrics, network, database_name, database_host, database_port):
    # Upstream cli/build.rs intentionally emits the seven-character git SHA.
    # The full revision and tested immutable image are recorded in SOURCE.lock.
    if metrics.get("commitId") not in {SIMPLY_REVISION, SIMPLY_REVISION[:7]}:
        raise ValueError("Simply source revision does not match reviewed pin")
    args = (metrics.get("settings") or {}).get("cli_args") or {}
    if args.get("network") != network:
        raise ValueError("Simply network does not match configured network")
    if (metrics.get("database") or {}).get("databaseName") != database_name:
        raise ValueError("Simply database does not match adapter database")
    endpoint = urlsplit(args.get("databaseUrl") or "")
    if (endpoint.scheme not in {"postgres", "postgresql"}
        or endpoint.hostname != database_host or (endpoint.port or 5432) != database_port
        or unquote(endpoint.path.removeprefix("/")) != database_name):
        raise ValueError("Simply SQL endpoint does not match adapter SQL endpoint")
    if (args.get("pruning") or {}).get("pruneDb") is not None:
        raise ValueError("Simply pruning must be disabled; use owned retention")
    if args.get("ignoreSelfSends"):
        raise ValueError("self-sends must be retained for Social and encrypted messages")
    critical_tables = {"VirtualChainProcessing", "TransactionAcceptance", "TransactionProcessing",
                       "TransactionsTable", "TransactionsInputs", "TransactionsOutputs"}
    critical_fields = {"TxPayload", "TxBlockTime", "TxBlockHash", "TxInPreviousOutpoint",
                       "TxOutAmount", "TxOutScriptPublicKey", "TxOutScriptPublicKeyAddress"}
    if critical_tables.intersection(args.get("disable") or []):
        raise ValueError("Simply has required transaction/acceptance tables disabled")
    if not {"BlocksTable", "BlockParentTable", "AddressesTransactionsTable"}.issubset(args.get("disable") or []):
        raise ValueError("unused chain-wide tables must be disabled for owned retention")
    if critical_fields.intersection(args.get("excludeFields") or []):
        raise ValueError("Simply excludes required payload, payment or sender evidence")


def check_source(conn, url, network):
    # The metrics body contains upstream database settings: never log it.
    with urllib.request.urlopen(url, timeout=10) as response:
        metrics = json.load(response)
    validate_source(metrics, network, conn.info.dbname, conn.info.host, conn.info.port)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", required=True)
    parser.add_argument("--metrics-url", default=os.environ.get("SIMPLY_METRICS_URL", "http://indexer:8501/api/metrics"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    worker = sub.add_parser("worker")
    worker.add_argument("--once", action="store_true")
    backfill = sub.add_parser("backfill")
    backfill.add_argument("--family", choices=[f.value for f in Family], required=True)
    backfill.add_argument("--after", default="")
    backfill.add_argument("--page-size", type=int, default=500)
    prune = sub.add_parser("prune")
    prune.add_argument("--retention-hours", type=int, default=24)
    args = parser.parse_args()
    dsn = os.environ.get("PROTOCOL_DATABASE_URL")
    if not dsn:
        parser.error("PROTOCOL_DATABASE_URL is required")
    with connect(dsn) as conn:
        check_source(conn, args.metrics_url, args.network)
        if args.command == "init":
            install(conn, args.network)
            return
        config = conn.execute("SELECT network, source_revision FROM protocol_indexer.config").fetchone()
        conn.commit()
        if not config or config["network"] != args.network or config["source_revision"] != SIMPLY_REVISION:
            raise ValueError("adapter database network/source binding does not match")
        if args.command == "backfill":
            adapter = BackfillAdapter(Family(args.family))
            cursor = bytes.fromhex(args.after)
            if cursor and len(cursor) != 32:
                parser.error("--after must be a full transaction ID")
            while (cursor := adapter.run_page(conn, cursor, args.page_size)) is not None:
                print(json.dumps({"family": args.family, "after": cursor.hex()}), flush=True)
            return
        if args.command == "prune":
            if not 1 <= args.retention_hours <= 8760:
                parser.error("retention must be 1..8760 hours")
            count = prune_source(conn, time.time_ns() // 1_000_000 - args.retention_hours * 3_600_000)
            print(json.dumps({"raw_transactions_pruned": count}))
            return
        running = True

        def stop(*_):
            nonlocal running
            running = False

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        adapters = [ScanAdapter(family) for family in Family]
        checked = time.monotonic()
        while running:
            if time.monotonic() - checked >= 60:
                check_source(conn, args.metrics_url, args.network)
                checked = time.monotonic()
            changed = discard_unrelated(conn) > 0
            for _ in range(100):
                if not capture_one(conn):
                    break
                changed = True
            for adapter in adapters:
                changed = adapter.run_one(conn) is not None or changed
            if args.once:
                break
            if not changed:
                time.sleep(0.25)


if __name__ == "__main__":
    main()
