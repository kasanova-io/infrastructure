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
import urllib.error
import urllib.parse

from protocol_indexer.archive import HEX64, check_network, verify_archive
from protocol_indexer.routing import route as classify

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("shared_social_history", HERE.parent / "social-indexer-history/history.py")
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)


class ArchiveHistory(history.History):
    # Do not reuse a K-only inventory or mislabel old receipts with the new checks.
    scope = "six-protocol-archive-evidence-v1"
    action_discovery = False
    address_route = "/archive-address-history-v1"
    engagement_routes = ("/get-post-engagement", "/get-post-tippers")

    def queue_address(self, address):
        prefix = "kaspa:" if self.network == "mainnet" else "kaspatest:"
        if not isinstance(address, str) or not address.startswith(prefix) or not address[len(prefix):].isalnum():
            raise ValueError("Invalid address discovery seed for configured network")
        self.add_job(self.address_route, {"address": address}, "transactions")

    def queue_engagement(self, txid):
        if not isinstance(txid, str) or not HEX64.fullmatch(txid):
            return
        self.add_job("/get-post-engagement", {"postId": txid, "type": "all", "requesterPubkey": history.ANON}, "engagement")
        self.add_job("/get-post-tippers", {"postId": txid}, "tippers")

    def seed_action_history(self):
        # Actual transaction addresses, not signer == funding-address guesses.
        # Only already checked six-family receipts expand the address frontier.
        # Discovered rows remain candidates until the normal verifier runs.
        for txid, in self.db.execute("SELECT txid FROM records WHERE status='verified'").fetchall():
            receipt = history.load(self.root / "verified" / (txid + ".json.gz"))
            if receipt.get("transaction_id") != txid or receipt.get("network") != self.network:
                raise ValueError("Verified address seed identity/network mismatch")
            classified = classify(bytes.fromhex(receipt["payload"]))
            if classified is None:
                raise ValueError("Verified address seed protocol mismatch")
            detail = receipt["archive_transaction"]
            for row in detail["outputs"]:
                if row.get("script_public_key_address"):
                    self.queue_address(row["script_public_key_address"])
            for row in detail["inputs"]:
                if row.get("previous_outpoint_address"):
                    self.queue_address(row["previous_outpoint_address"])
            if classified.action in ("post", "reply", "quote", "poll"):
                self.queue_engagement(txid)

    def job_order(self):
        # One page per address, with fair progress across long address histories.
        return "pages,id" if self.action_discovery else super().job_order()

    def empty_page_advances(self, route):
        return route == self.address_route

    def validate_cursor(self, route, cursor, next_cursor):
        if route == self.address_route:
            if not next_cursor.isascii() or not next_cursor.isdigit() or int(next_cursor) <= 0 or (cursor and int(next_cursor) >= int(cursor)):
                raise ValueError("Archive address cursor failed to advance")
        elif route not in self.engagement_routes:
            super().validate_cursor(route, cursor, next_cursor)
        # Engagement cursors are opaque. History's durable cycle check still
        # rejects repeated cursors (including A -> B -> A across restarts).

    def discovery_page(self, jid, route, params, cursor, pages):
        if route == self.address_route:
            self.guard_capacity()
            result = history._chats.collect_explorer(
                params["address"], self.root, archive=self.archive,
                request_fn=self.request_with_headers,
                accept_payload=lambda raw: classify(raw) is not None,
                checkpoint_scope={"archive": self.archive, "network": self.network,
                                  "filter": "six-family-routing-v1"}, page_budget=1,
            )
            # Accumulated candidates make an interrupted checkpoint/DB commit
            # safe: the next page replays every retained ID, without skipping.
            page = {"transactions": [{"id": row["transaction_id"]} for row in result["rows"]],
                    "pagination": {"hasMore": not result["done"],
                                   "nextCursor": str(result["before"]) if not result["done"] else None}}
            return page, self.archive + "/addresses/[seed]/full-transactions-page"
        if route not in self.engagement_routes:
            return super().discovery_page(jid, route, params, cursor, pages)
        query = {**params, "limit": 50 if route == "/get-post-tippers" else 100}
        if cursor:
            query["before"] = cursor
        url = self.source + route + "?" + urllib.parse.urlencode(query)
        page = self.cache("pages", jid + "-" + str(pages), url)
        if route == "/get-post-engagement":
            page = {**page, "engagement": [
                {**row, "id": row.get("actionTxId"), "userPublicKey": row.get("actorPubkey")}
                for row in page.get("engagement", [])]}
        return page, url

    def seed(self):
        super().seed()
        # Existing completed inventories predate action discovery. Add jobs for
        # their observed authors without resetting any content page/cursor.
        for (author,) in self.db.execute(
            "SELECT DISTINCT json_extract(data,'$.userPublicKey') FROM observations"
        ).fetchall():
            self.queue_actions(author)
        if self.action_discovery:
            self.seed_action_history()
        self.db.commit()

    def queue_actions(self, author):
        if isinstance(author, str) and history.KEY.fullmatch(author):
            self.add_job(
                "/get-notifications", {"requesterPubkey": author}, "notifications"
            )

    def is_content(self, row, route):
        # A vote's transaction ID is not a post. Avoid thousands of meaningless
        # reply scans while retaining the normal content traversal.
        return super().is_content(row, route) and (
            route != "/get-notifications"
            or row.get("contentType") in ("post", "reply", "quote", "poll")
        )

    def queue_related(self, row, route):
        if route == self.address_route:
            return
        if route in self.engagement_routes:
            self.queue_actions(row.get("userPublicKey"))
            return
        super().queue_related(row, route)
        self.queue_actions(row.get("userPublicKey"))
        if route == "/get-notifications":
            for field in ("contentId", "postId"):
                target = row.get(field)
                if isinstance(target, str) and HEX64.fullmatch(target):
                    # Public API rows supply candidates, never authenticity or
                    # a native notification. Every ID still needs full proofs.
                    self.db.execute(
                        "INSERT OR IGNORE INTO records(txid) VALUES (?)", (target,)
                    )
                    self.add_job(
                        "/get-replies", {"post": target, "requesterPubkey": history.ANON}, "replies"
                    )

    def bind_network(self):
        # Check the current endpoint every invocation, rather than trusting a
        # cached health response or an address prefix shared by TN10/TN11.
        self.network_info = self.request(self.archive + "/info/network")
        check_network(self.network, self.network_info)
        history.atomic_json(self.root / "archive-network.json.gz", self.network_info)

    def required_evidence(self, category, key, url, resource):
        try:
            return self.cache(category, key, url)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                # Missing per-record history is a durable coverage gap, not
                # verified evidence and not a reason to abandon every other ID.
                # Preserve operational errors (429, auth, server outage) so
                # they pause the worker without quarantining a whole inventory.
                raise ValueError(f"Archive {resource} evidence unavailable (404)") from exc
            raise

    def verify_one(self, txid, observations=None):
        detail = self.required_evidence("archive", txid, self.archive + "/transactions/" + txid + "?resolve_previous_outpoints=light", "transaction")
        accepting_hash = detail.get("accepting_block_hash", "")
        hashes = detail.get("block_hash")
        if not HEX64.fullmatch(accepting_hash) or not isinstance(hashes, list) or not hashes or any(not isinstance(h, str) or not HEX64.fullmatch(h) for h in hashes):
            raise ValueError("missing valid archive acceptance/containing block")
        accepting = self.required_evidence("blocks", accepting_hash, self.archive + "/blocks/" + accepting_hash + "?includeTransactions=false", "accepting block")
        containing_hash = min(hashes)
        containing = self.required_evidence("blocks", containing_hash + "-full", self.archive + "/blocks/" + containing_hash + "?includeTransactions=true", "containing block")
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
    parser.add_argument("mode", choices=("discover", "discover-actions", "verify", "status", "export"))
    parser.add_argument("directory", type=Path)
    parser.add_argument("--source", required=True, help="existing public Social discovery API")
    parser.add_argument("--archive", required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--txid", action="append", default=[], help="candidate ID from existing Chats/Social inventories; repeatable")
    parser.add_argument("--address", action="append", default=[], help="public participant address for action-history discovery; repeatable")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, choices=range(1, 5), default=1)
    parser.add_argument("--retry-quarantine", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.address and args.mode != "discover-actions":
        parser.error("--address requires discover-actions")
    if args.limit < 0 or len(args.txid) > 4096 or any(not HEX64.fullmatch(t) for t in args.txid):
        parser.error("nonnegative limit and at most4096 full transaction IDs required")
    os.umask(0o077)
    h = ArchiveHistory(args.directory, args.source, args.archive, args.network)
    try:
        lock_mode = "discover" if args.mode == "discover-actions" else args.mode
        with (h.root / (lock_mode + ".lock")).open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if args.mode in ("discover", "discover-actions", "verify"):
                h.bind_network()
            if args.mode == "discover-actions":
                h.action_discovery = True
                with h.db:
                    for address in args.address:
                        h.queue_address(address)
                h.discover(args.limit)
            elif args.mode == "discover":
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
