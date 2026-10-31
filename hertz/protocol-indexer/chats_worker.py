#!/usr/bin/env python3
"""Run the private journal-to-Chats connector using existing history HTTP tools."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import time
import urllib.error
import urllib.parse

from psycopg.types.json import Jsonb

from protocol_indexer.archive import HEX64, bind_output_evidence, checked_receipt, input0_evidence, integer, verify_transaction_evidence
from protocol_indexer.chats import ChatsConsumer, EvidencePending, NativeTransport, native_transaction
from protocol_indexer.cli import check_source
from protocol_indexer.journal import connect

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("connector_history", HERE.parent / "social-indexer-history/history.py")
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)


class Resolver(history.History):
    scope = "shared-chat-delivery-evidence-v1"

    def request(self, url):
        try:
            return super().request(url)
        except RuntimeError as exc:
            # Existing HTTP helper's terminal transient failure is per job.
            # Keep capacity, storage and unrelated programmer failures visible.
            if str(exc) != "request retries exhausted":
                raise
            raise EvidencePending("archive_request_exhausted") from exc

    def save(self, path, value):
        self.guard_capacity()
        try:
            history.atomic_json(path, value)
        except OSError as exc:
            raise RuntimeError('archive cache storage failure') from exc

    def quarantine(self, path, txid):
        self.guard_capacity()
        try:
            path.rename(self.root / "quarantine" / (txid + "-cached-" + str(time.time_ns()) + ".json.gz"))
        except FileNotFoundError:
            # Another consumer sharing this cache already quarantined the file.
            pass
        except OSError as exc:
            raise RuntimeError('archive cache quarantine storage failure') from exc

    def refresh_network(self):
        from protocol_indexer.archive import check_network
        self.network_info = self.request(self.archive + "/info/network")
        check_network(self.network, self.network_info)

    def collect_body(self, txid):
        try:
            return self._collect_body(txid)
        except (OSError, EOFError, ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            raise EvidencePending("archive_evidence_shape_or_lag") from exc

    def _collect_body(self, txid):
        verified = self.root / "verified" / (txid + ".json.gz")
        if verified.exists():
            try:
                # This file is an immutable body receipt. Funding-input
                # enrichment is separately checked by the private consumer.
                return checked_receipt(history.load(verified), txid, self.network, with_input=False)
            except (OSError, EOFError, ValueError, TypeError, KeyError, IndexError, AttributeError):
                self.quarantine(verified, txid)
        # Do not cache a lagging detail or incomplete containing block. Only a
        # fully checked immutable transaction receipt is reusable on retries.
        self.guard_capacity()
        detail = self.request(self.archive + "/transactions/" + txid + "?resolve_previous_outpoints=light")
        accepted = detail.get("accepting_block_hash")
        hashes = detail.get("block_hash") or []
        if (not isinstance(accepted, str) or not HEX64.fullmatch(accepted)
            or not isinstance(hashes, list) or not hashes
            or any(not isinstance(h, str) or not HEX64.fullmatch(h) for h in hashes)):
            raise EvidencePending("archive_acceptance_lag")
        header = self.verified_header(accepted)
        containing = min(hashes)
        block = self.request(self.archive + "/blocks/" + containing + "?includeTransactions=true")
        receipt = verify_transaction_evidence(txid, self.network, self.network_info, detail, header, block,
                                              require_protocol=False, require_accepted=False)
        self.save(verified, receipt)
        return receipt

    def verified_header(self, block):
        if not isinstance(block, str) or not HEX64.fullmatch(block):
            raise EvidencePending("invalid_header_id")

        def checked(value):
            if value.get("verboseData", {}).get("hash") != block:
                raise ValueError("archive header mismatch")
            integer(value.get("header", {}).get("daaScore"))
            integer(value.get("header", {}).get("timestamp"))
            return value

        saved = self.root / "blocks" / (block + ".json.gz")
        if saved.exists():
            try:
                return checked(history.load(saved))
            except (OSError, EOFError, ValueError, TypeError, KeyError, AttributeError):
                self.quarantine(saved, block)
        self.guard_capacity()
        try:
            value = checked(self.request(self.archive + "/blocks/" + block + "?includeTransactions=false"))
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise EvidencePending("archive_header_lag") from exc
        self.save(saved, value)
        return value

    def resolve(self, conn, row):
        if not row["ever_accepted"]:
            raise EvidencePending("not_yet_accepted")
        saved = conn.execute("SELECT receipt FROM protocol_indexer.evidence WHERE transaction_id=%s",
                             (row["transaction_id"],)).fetchone()
        try:
            receipt = None
            if saved:
                try:
                    receipt = checked_receipt(saved["receipt"], row["transaction_id"].hex(), self.network)
                except (ValueError, TypeError, KeyError, IndexError, AttributeError):
                    self.save(self.root / "quarantine" / (row["transaction_id"].hex() + "-db-" + str(time.time_ns()) + ".json.gz"), saved["receipt"])
            if receipt is None:
                receipt = self.collect_body(row["transaction_id"].hex())
            if "input0" not in receipt:
                first = min(receipt["archive_transaction"]["inputs"], key=lambda v: v["index"])
                previous = self.collect_body(first["previous_outpoint_hash"])
                receipt["input0"] = input0_evidence(receipt, previous)
            block = row["accepting_block"]
            header = self.verified_header(block.hex()) if block is not None else None
            # The canonical journal, never an archive accepted flag, decides
            # the latest acceptance/revocation delivered to native storage.
            transaction = native_transaction(row, receipt, header)
        except (OSError, EOFError, ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            raise EvidencePending("archive_evidence_unavailable_or_conflicting") from exc
        # SQL/storage failures remain operational failures; never disguise them
        # as a bad public transaction or silently acknowledge the source job.
        self.store_receipt(conn, row, receipt)
        return transaction

    def resolve_outputs(self, conn, row):
        saved = conn.execute("SELECT receipt FROM protocol_indexer.evidence WHERE transaction_id=%s",
                             (row['transaction_id'],)).fetchone()
        try:
            receipt = None
            if saved:
                try:
                    receipt = checked_receipt(saved['receipt'], row['transaction_id'].hex(), self.network, with_input=False)
                except (ValueError, TypeError, KeyError, IndexError, AttributeError):
                    self.save(self.root / 'quarantine' / (row['transaction_id'].hex() + '-db-' + str(time.time_ns()) + '.json.gz'), saved['receipt'])
            if receipt is None:
                receipt = self.collect_body(row['transaction_id'].hex())
            bind_output_evidence(row, receipt)
        except (OSError, EOFError, ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            raise EvidencePending('archive_output_evidence_unavailable_or_conflicting') from exc
        self.store_receipt(conn, row, receipt)

    @staticmethod
    def store_receipt(conn, row, receipt):
        conn.execute("""INSERT INTO protocol_indexer.evidence (transaction_id,receipt) VALUES (%s,%s)
            ON CONFLICT (transaction_id) DO UPDATE SET receipt = CASE
              WHEN jsonb_typeof(protocol_indexer.evidence.receipt)='object'
              THEN protocol_indexer.evidence.receipt || EXCLUDED.receipt ELSE EXCLUDED.receipt END""",
            (row["transaction_id"], Jsonb(receipt)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--metrics-url", default=os.environ.get("SIMPLY_METRICS_URL", "http://indexer:8501/api/metrics"))
    parser.add_argument("--source-binding", default=os.environ.get("PROTOCOL_SOURCE_BINDING_FILE", "/run/protocol-indexer/source-binding.json"))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    parsed = urllib.parse.urlsplit(args.archive)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        parser.error("explicit HTTPS archive origin required")
    dsn, secret = os.environ.get("PROTOCOL_DATABASE_URL"), os.environ.get("KASANOVA_SHARED_INGEST_TOKEN", "")
    if not dsn:
        parser.error("PROTOCOL_DATABASE_URL required")
    os.umask(0o077)
    resolver = Resolver(args.cache, args.archive.rstrip("/"), args.archive.rstrip("/"), args.network)
    transport = NativeTransport(args.endpoint, secret)
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        # Reuse the collector's exclusive phase lock, immutable source binding,
        # cross-process rate buckets and capacity guard, rather than another scanner.
        import fcntl
        with (args.cache / "connector.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with connect(dsn) as conn:
                check_source(conn, args.metrics_url, args.network, args.source_binding)
                resolver.refresh_network()
                consumer = ChatsConsumer(args.network, resolver.resolve, transport)
                checked = time.monotonic()
                while running:
                    if time.monotonic() - checked >= 60:
                        check_source(conn, args.metrics_url, args.network, args.source_binding)
                        resolver.refresh_network()
                        checked = time.monotonic()
                    result = consumer.run_one(conn)
                    if result:
                        print(json.dumps({"consumer": "native-chats-v1", "status": result}), flush=True)
                    if args.once:
                        break
                    if result is None:
                        time.sleep(0.25)
    finally:
        resolver.db.close()


if __name__ == "__main__":
    main()
