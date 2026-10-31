#!/usr/bin/env python3
"""Collect six-family archival evidence using existing resumable Social history tools.

No canonical-journal import, service mutation, notification or transaction broadcast.
"""

import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path

from protocol_indexer.archive import HEX64, check_network, verify_archive

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("shared_social_history", HERE.parent / "social-indexer-history/history.py")
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)


class ArchiveHistory(history.History):
    # Do not reuse a K-only inventory or mislabel old receipts with the new checks.
    scope = "six-protocol-archive-evidence-v1"

    def bind_network(self):
        # Check the current endpoint every invocation, rather than trusting a
        # cached health response or an address prefix shared by TN10/TN11.
        self.network_info = self.request(self.archive + "/info/network")
        check_network(self.network, self.network_info)
        history.atomic_json(self.root / "archive-network.json.gz", self.network_info)

    def verify_one(self, txid, observations=None):
        detail = self.cache("archive", txid, self.archive + "/transactions/" + txid + "?resolve_previous_outpoints=light")
        accepting_hash = detail.get("accepting_block_hash", "")
        hashes = detail.get("block_hash")
        if not HEX64.fullmatch(accepting_hash) or not isinstance(hashes, list) or not hashes or any(not isinstance(h, str) or not HEX64.fullmatch(h) for h in hashes):
            raise ValueError("missing valid archive acceptance/containing block")
        accepting = self.cache("blocks", accepting_hash, self.archive + "/blocks/" + accepting_hash + "?includeTransactions=false")
        containing_hash = min(hashes)
        containing = self.cache("blocks", containing_hash + "-full", self.archive + "/blocks/" + containing_hash + "?includeTransactions=true")
        receipt = verify_archive(txid, self.network, self.network_info, detail, accepting, containing)
        receipt["archive"] = self.archive
        # Discovery supplies candidate IDs, never signature/payment authority.
        history.atomic_json(self.root / "verified" / (txid + ".json.gz"), receipt)
        return receipt["family"]

    def export(self, target):
        # Preserve binary message and tip evidence; the original exporter's
        # profile-state ordering rule is for its K-only replay format.
        rows = [history.load(self.root / "verified" / (txid + ".json.gz"))
                for txid, in self.db.execute("SELECT txid FROM records WHERE status='verified'")]
        rows.sort(key=lambda row: (row["block_time"], row["transaction_id"]))
        with open(target, "x") as out:
            for row in rows:
                out.write(json.dumps(row, separators=(",", ":")) + "\n")
        return len(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("discover", "verify", "status", "export"))
    parser.add_argument("directory", type=Path)
    parser.add_argument("--source", required=True, help="existing public Social discovery API")
    parser.add_argument("--archive", required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--txid", action="append", default=[], help="candidate ID from existing Chats/Social inventories; repeatable")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, choices=range(1, 5), default=1)
    parser.add_argument("--retry-quarantine", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.limit < 0 or len(args.txid) > 4096 or any(not HEX64.fullmatch(t) for t in args.txid):
        parser.error("nonnegative limit and at most4096 full transaction IDs required")
    os.umask(0o077)
    h = ArchiveHistory(args.directory, args.source, args.archive, args.network)
    try:
        with (h.root / (args.mode + ".lock")).open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if args.mode in ("discover", "verify"):
                h.bind_network()
            if args.mode == "discover":
                h.discover(args.limit)
            elif args.mode == "verify":
                with h.db:
                    h.db.executemany("INSERT OR IGNORE INTO records(txid) VALUES (?)", [(t,) for t in args.txid])
                # Resuming selected verified IDs is a no-op; explicit quarantine
                # retry remains required, preserving adverse evidence.
                if args.txid:
                    pending = [t for t in args.txid if h.db.execute("SELECT status FROM records WHERE txid=?", (t,)).fetchone()[0]
                               in (("discovered", "quarantined") if args.retry_quarantine else ("discovered",))]
                    if pending:
                        h.verify(args.limit, args.retry_quarantine, pending, args.workers)
                else:
                    h.verify(args.limit, args.retry_quarantine, workers=args.workers)
                h.status()
            elif args.mode == "export":
                if args.output is None:
                    parser.error("--output required")
                print(json.dumps({"exported": h.export(args.output), "canonical_imported": 0}))
            else:
                h.status()
    finally:
        h.db.close()


if __name__ == "__main__":
    main()
