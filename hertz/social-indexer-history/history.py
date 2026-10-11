#!/usr/bin/env python3
"""Resumable public Social discovery and archive verification; never writes a service."""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time
import urllib.parse
import urllib.error

# Reuse the deployed Chats migration's pacing, retries and atomic gzip evidence.
_spec = importlib.util.spec_from_file_location(
    "chats_collect", Path(__file__).parents[1] / "kasia-indexer-dev/history/collect.py"
)
_chats = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_chats)
atomic_json = _chats.atomic_json
ANON = "02" + "0" * 64
HEX64 = re.compile(r"^[0-9a-f]{64}$")
KEY = re.compile(r"^0[23][0-9a-f]{64}$")
SIGNER_KEY = re.compile(r"^(?:0[23])?[0-9a-f]{64}$", re.IGNORECASE)
KINDS = {"post", "quote", "reply", "broadcast"}


class CapacityPause(RuntimeError):
    """Pause without labelling valid history as invalid."""


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def load(path):
    import gzip

    with gzip.open(path, "rt") as stream:
        return json.load(stream)


class History:
    scope = "retained-content-and-current-profiles"
    kinds = KINDS

    def __init__(self, root, source, archive, network):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        os.environ["CHATS_RATE_DIR"] = str(self.root)
        self.source, self.archive, self.network = (
            source.rstrip("/"),
            archive.rstrip("/"),
            network,
        )
        self.db = sqlite3.connect(self.root / "inventory.sqlite", timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            """
          CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY, value TEXT);
          CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, route TEXT, params TEXT, envelope TEXT, cursor TEXT, pages INTEGER DEFAULT 0, done INTEGER DEFAULT 0);
          CREATE TABLE IF NOT EXISTS observations (txid TEXT, job TEXT, data TEXT, PRIMARY KEY(txid,job));
          CREATE TABLE IF NOT EXISTS records (txid TEXT PRIMARY KEY, status TEXT DEFAULT 'discovered', kind TEXT, error TEXT);
          CREATE INDEX IF NOT EXISTS records_status ON records(status,txid);
        """
        )
        config = json.dumps(
            {"source": self.source, "archive": self.archive, "network": network},
            sort_keys=True,
        )
        old = self.db.execute("SELECT value FROM config WHERE id=1").fetchone()
        if old and old[0] != config:
            self.db.close()
            raise ValueError(
                "History directory belongs to a different source/archive/network"
            )
        self.db.execute("INSERT OR IGNORE INTO config VALUES (1,?)", (config,))
        prior_scope = self.db.execute("SELECT value FROM config WHERE id=3").fetchone()
        if prior_scope and prior_scope[0] != self.scope:
            self.db.close()
            raise ValueError("Inventory belongs to a different recovery scope")
        self.db.execute("INSERT OR IGNORE INTO config VALUES (3,?)", (self.scope,))
        self.db.commit()
        for directory in ("pages", "archive", "blocks", "verified", "quarantine"):
            (self.root / directory).mkdir(exist_ok=True)

    def request(self, url):
        # Share pacing across content/relationship collectors. The conservative
        # archive default supplements Chats' 0.4s bucket and shared 429 cooldown.
        source_request = url.startswith(self.source + "/")
        rate_root = getattr(self, "rate_root", self.root)
        interval_file = rate_root / "archive-interval.txt"
        archive_interval = (
            float(interval_file.read_text()) if interval_file.exists() else 0.6
        )
        if not 0.4 <= archive_interval <= 60:
            raise ValueError("Archive pacing must be between 0.4 and 60 seconds")
        interval = 1.0 if source_request else archive_interval
        bucket = "source-rate.txt" if source_request else "archive-rate.txt"
        with (rate_root / bucket).open("a+") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            stream.seek(0)
            next_at = float(stream.read() or 0)
            time.sleep(max(0, next_at - time.time()))
            stream.seek(0)
            stream.truncate()
            stream.write(str(time.time() + interval))
            stream.flush()
        return _chats.request(url)[0]

    def cache(self, directory, key, url):
        path = self.root / directory / (key + ".json.gz")
        if path.exists():
            return load(path)
        if shutil.disk_usage(self.root).free < 2 * 1024**3:
            raise CapacityPause("History collection paused: less than 2 GiB free")
        value = self.request(url)
        atomic_json(path, value)
        return value

    def add_job(self, route, params, envelope):
        key = digest([route, params])
        self.db.execute(
            "INSERT OR IGNORE INTO jobs(id,route,params,envelope) VALUES (?,?,?,?)",
            (key, route, json.dumps(params, sort_keys=True), envelope),
        )

    def seed(self):
        health = self.request(self.source + "/health")
        if health.get("network") != self.network:
            raise ValueError("Source network does not match configured network")
        atomic_json(self.root / "source-health.json.gz", health)
        if not (self.root / "starting-stats.json.gz").exists():
            atomic_json(
                self.root / "starting-stats.json.gz",
                self.request(self.source + "/stats"),
            )
        self.add_job("/get-posts-watching", {"requesterPubkey": ANON}, "posts")
        self.add_job("/get-users", {"requesterPubkey": ANON}, "posts")
        self.db.commit()

    def discover(self, budget, parents=None):
        self.seed()
        selected_jobs = []
        for parent in parents or []:
            if not HEX64.fullmatch(parent):
                raise ValueError("Invalid parent transaction ID")
            params = {"post": parent, "requesterPubkey": ANON}
            self.add_job("/get-replies", params, "replies")
            selected_jobs.append(digest(["/get-replies", params]))
        self.db.commit()
        condition = "done=0"
        if selected_jobs:
            condition += " AND id IN (" + ",".join("?" for _ in selected_jobs) + ")"

        for _ in range(budget or 10**9):
            job = self.db.execute(
                f"""SELECT id,route,params,envelope,cursor,pages FROM jobs WHERE {condition} ORDER BY CASE route WHEN "/get-posts-watching" THEN 0 WHEN "/get-users" THEN 1 ELSE CASE WHEN json_extract(params,'$.user') IS NOT NULL THEN 2 ELSE 3 END END,id LIMIT 1""",
                selected_jobs,
            ).fetchone()
            if not job:
                break
            jid, route, params, envelope, cursor, pages = job
            query = {**json.loads(params), "limit": 100}
            if cursor:
                query["before"] = cursor
            url = self.source + route + "?" + urllib.parse.urlencode(query)
            page = self.cache("pages", jid + "-" + str(pages), url)
            rows, pagination = page.get(envelope), page.get("pagination")
            if (
                not isinstance(rows, list)
                or not isinstance(pagination, dict)
                or not isinstance(pagination.get("hasMore"), bool)
            ):
                raise ValueError("Invalid page contract " + url)
            next_cursor = pagination.get("nextCursor")
            if pagination["hasMore"] and (
                not rows or not next_cursor or next_cursor == cursor
            ):
                raise ValueError("Pagination failed to advance " + url)
            if pagination["hasMore"]:
                parts = next_cursor.split("_")
                if len(parts) != 2 or not all(p.isdigit() for p in parts):
                    raise ValueError("Invalid source cursor")
                if cursor and tuple(map(int, parts)) >= tuple(
                    map(int, cursor.split("_"))
                ):
                    raise ValueError("Source cursor moved forward")
            with self.db:
                for row in rows:
                    txid = row.get("id", "")
                    if not HEX64.fullmatch(txid):
                        raise ValueError("Invalid transaction ID")
                    self.db.execute(
                        "INSERT OR IGNORE INTO records(txid) VALUES (?)", (txid,)
                    )
                    old = self.db.execute(
                        "SELECT data FROM observations WHERE txid=? AND job=?",
                        (txid, jid),
                    ).fetchone()
                    if old:
                        prior = json.loads(old[0])
                        for field in (
                            "signature",
                            "userPublicKey",
                            "postContent",
                            "nickname",
                            "message",
                            "timestamp",
                        ):
                            if prior.get(field) != row.get(field):
                                raise ValueError(
                                    "Conflicting source observation " + txid
                                )
                    self.db.execute(
                        "INSERT OR IGNORE INTO observations VALUES (?,?,?)",
                        (txid, jid, json.dumps(row)),
                    )
                    author = row.get("userPublicKey", "")
                    if KEY.fullmatch(author):
                        self.add_job(
                            "/get-replies",
                            {"user": author, "requesterPubkey": ANON},
                            "replies",
                        )
                    # Traverse every discovered parent, including orphan roots
                    # referred to by replies; no assumption repliesCount is complete.
                    if route != "/get-users":
                        self.add_job(
                            "/get-replies",
                            {"post": txid, "requesterPubkey": ANON},
                            "replies",
                        )
                    parent = row.get("parentPostId")
                    if parent and HEX64.fullmatch(parent):
                        self.add_job(
                            "/get-replies",
                            {"post": parent, "requesterPubkey": ANON},
                            "replies",
                        )
                self.db.execute(
                    "UPDATE jobs SET cursor=?,pages=pages+1,done=? WHERE id=?",
                    (next_cursor, int(not pagination["hasMore"]), jid),
                )
            self.status()

    def verify_one(self, txid, observations=None):
        url = (
            self.archive + "/transactions/" + txid + "?resolve_previous_outpoints=light"
        )
        try:
            tx = self.cache("archive", txid, url)
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
            # An unavailable transaction is excluded, not evidence of invalid
            # chain history. Retain the endpoint/status for an explicit retry.
            raise ValueError(
                f"Archive transaction unavailable (HTTP 404): {url}"
            ) from exc
        if tx.get("transaction_id") != txid or tx.get("is_accepted") is not True:
            raise ValueError("Archive ID mismatch or transaction not accepted")
        prefix = {"mainnet": "kaspa:", "testnet-10": "kaspatest:"}[self.network]
        addresses = [
            row.get("script_public_key_address") for row in tx.get("outputs", [])
        ]
        addresses += [
            row.get("previous_outpoint_address") for row in tx.get("inputs", [])
        ]
        addresses = [a for a in addresses if a]
        if not addresses or any(not a.startswith(prefix) for a in addresses):
            raise ValueError("Archive address network conflicts with inventory")
        raw = bytes.fromhex(tx.get("payload", ""))
        fields = raw.decode("utf-8").split(":")
        if len(fields) < 6 or fields[:2] != ["k", "1"] or fields[2] not in self.kinds:
            raise ValueError("Unexpected protocol/action for this inventory")
        kind, author, signature = fields[2:5]
        if not SIGNER_KEY.fullmatch(author):
            raise ValueError("Invalid signer key")
        author = bytes.fromhex(author).hex()
        signature = bytes.fromhex(signature).hex()
        if observations is None:
            observations = list(
                self.db.execute("SELECT data FROM observations WHERE txid=?", (txid,))
            )
        self.verify_observations(kind, fields, observations)
        accepting = tx.get("accepting_block_hash", "")
        if not HEX64.fullmatch(accepting):
            raise ValueError("Missing accepting block")
        accepted_block = self.cache(
            "blocks",
            accepting,
            self.archive + "/blocks/" + accepting + "?includeTransactions=false",
        )
        if accepted_block.get("verboseData", {}).get("hash") != accepting:
            raise ValueError("Accepting block hash mismatch")
        # Verify actual inclusion and payload in one deterministic containing block.
        hashes = tx.get("block_hash", [])
        if not hashes or any(not HEX64.fullmatch(h) for h in hashes):
            raise ValueError("Missing/invalid containing blocks")
        containing = min(hashes)
        block = self.cache(
            "blocks",
            containing + "-full",
            self.archive + "/blocks/" + containing + "?includeTransactions=true",
        )
        if block.get("verboseData", {}).get("hash") != containing:
            raise ValueError("Containing block hash mismatch")
        matches = [
            t
            for t in block.get("transactions", [])
            if t.get("verboseData", {}).get("transactionId") == txid
            and t.get("payload") == tx["payload"]
        ]
        if not matches:
            raise ValueError(
                "Original transaction payload missing from containing block"
            )
        prepared = {
            "transaction_id": txid,
            "recovery_scope": self.scope,
            "payload": tx["payload"],
            "block_time": int(block["header"]["timestamp"]),
            "kind": kind,
            "sender_pubkey": author,
            "accepting_block": accepting,
            "accepting_daa_score": int(accepted_block["header"]["daaScore"]),
            "accepting_time": int(accepted_block["header"]["timestamp"]),
            "containing_block": containing,
            "network": self.network,
            "payload_sha256": hashlib.sha256(raw).hexdigest(),
            "verification": "archive-acceptance-and-containing-block; native-signature-check-pending",
        }
        atomic_json(self.root / "verified" / (txid + ".json.gz"), prepared)
        return kind

    def verify_observations(self, kind, fields, observations):
        author, signature = (
            bytes.fromhex(fields[3]).hex(),
            bytes.fromhex(fields[4]).hex(),
        )
        for (encoded,) in observations:
            row = json.loads(encoded)
            if row.get("userPublicKey") != author or row.get("signature") != signature:
                raise ValueError("Archive signer/signature conflicts with discovery")
            if row.get("postContent") and row["postContent"] not in fields[5:]:
                raise ValueError("Archive content conflicts with discovery")

    def pending_ids(self, budget):
        for _ in range(budget or 10**9):
            row = self.db.execute(
                "SELECT txid FROM records WHERE status='discovered' ORDER BY txid LIMIT 1"
            ).fetchone()
            if row is None:
                return
            yield row[0]

    def verify(self, budget, retry=False, selected=None, workers=1):
        if not 1 <= workers <= 4:
            raise ValueError("Archive workers must be between 1 and 4")
        condition = "status!='verified'" if retry else "status='discovered'"
        txids = [
            r[0]
            for r in self.db.execute(
                "SELECT txid FROM records WHERE "
                + condition
                + " ORDER BY txid LIMIT ?",
                (budget or 10**9,),
            )
        ]
        if selected:
            if any(not HEX64.fullmatch(txid) for txid in selected):
                raise ValueError("Invalid selected transaction ID")
            missing = set(selected) - set(txids)
            if missing:
                raise ValueError(
                    "Selected transaction absent or already verified: "
                    + ",".join(sorted(missing))
                )
            txids = selected
        elif not retry:
            # Include records discovered while archive verification is running.
            txids = self.pending_ids(budget)
        if workers == 1:
            for txid in txids:
                self.finish_verification(txid, lambda: self.verify_one(txid))
            return
        # Only the coordinator accesses SQLite. Workers receive immutable source
        # observations and share the existing file-locked archive rate/cooldown.
        fixed = iter(txids) if selected or retry else None
        submitted = 0
        pending = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while True:
                while len(pending) < workers and (not budget or submitted < budget):
                    if fixed is not None:
                        txid = next(fixed, None)
                    else:
                        candidates = self.db.execute(
                            "SELECT txid FROM records WHERE status='discovered' ORDER BY txid LIMIT ?",
                            (workers,),
                        ).fetchall()
                        txid = next(
                            (r[0] for r in candidates if r[0] not in pending.values()),
                            None,
                        )
                    if txid is None:
                        break
                    observations = list(
                        self.db.execute(
                            "SELECT data FROM observations WHERE txid=?", (txid,)
                        )
                    )
                    pending[pool.submit(self.verify_one, txid, observations)] = txid
                    submitted += 1
                if not pending:
                    break
                ready, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in ready:
                    txid = pending.pop(future)
                    self.finish_verification(txid, future.result)

    def finish_verification(self, txid, result):
        try:
            kind = result()
        except CapacityPause:
            raise
        except ValueError as exc:
            atomic_json(
                self.root / "quarantine" / (txid + ".json.gz"),
                {"txid": txid, "error": str(exc)},
            )
            with self.db:
                self.db.execute(
                    "UPDATE records SET status='quarantined',error=? WHERE txid=?",
                    (str(exc), txid),
                )
        else:
            # Ledger/storage failures are operational pauses, never evidence that
            # an otherwise verified archived transaction is invalid.
            with self.db:
                self.db.execute(
                    "UPDATE records SET status='verified',kind=?,error=NULL WHERE txid=?",
                    (kind, txid),
                )
        self.status()

    def export(self, target):
        rows = [
            load(self.root / "verified" / (txid + ".json.gz"))
            for txid, in self.db.execute(
                "SELECT txid FROM records WHERE status='verified'"
            )
        ]
        rows.sort(key=lambda r: (r["block_time"], r["transaction_id"]))
        # Current-profile discovery gives at most one profile per signer. Refuse
        # ambiguous equal-time profile transitions, rather than invent ordering.
        seen = set()
        for r in rows:
            if r["kind"] == "broadcast":
                key = (r["sender_pubkey"], r["block_time"])
                if key in seen:
                    raise ValueError(
                        "Equal-time profile transitions need explicit ordering"
                    )
                seen.add(key)
        with open(target, "x") as out:
            for row in rows:
                out.write(json.dumps(row, separators=(",", ":")) + "\n")
        return len(rows)

    def status(self):
        states = dict(
            self.db.execute("SELECT status,count(*) FROM records GROUP BY status")
        )
        value = {
            "at": time.time(),
            "records": states,
            "verified_kinds": dict(
                self.db.execute(
                    "SELECT kind,count(*) FROM records WHERE status='verified' GROUP BY kind"
                )
            ),
            "jobs_total": self.db.execute("SELECT count(*) FROM jobs").fetchone()[0],
            "jobs_done": self.db.execute(
                "SELECT count(*) FROM jobs WHERE done=1"
            ).fetchone()[0],
            "pages": self.db.execute(
                "SELECT coalesce(sum(pages),0) FROM jobs"
            ).fetchone()[0],
            "staging_tracked_separately": True,
            "live_imported": 0,
        }
        atomic_json(self.root / "status.json.gz", value)
        print(json.dumps(value), flush=True)
        return value


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=["discover", "verify", "status", "export"])
    p.add_argument("directory", type=Path)
    p.add_argument("--source", required=True)
    p.add_argument("--archive", required=True)
    p.add_argument("--network", required=True, choices=["mainnet", "testnet-10"])
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Page/transaction budget; zero completes available work",
    )
    p.add_argument(
        "--workers",
        type=int,
        choices=range(1, 5),
        default=1,
        help="Bounded archive workers; shared request rate is unchanged",
    )
    p.add_argument("--retry-quarantine", action="store_true")
    p.add_argument(
        "--parent",
        action="append",
        help="Discover only these parent threads; repeatable",
    )
    p.add_argument(
        "--txid",
        action="append",
        help="Verify only an already-discovered transaction; repeatable",
    )
    p.add_argument("--output", type=Path)
    a = p.parse_args()
    os.umask(0o077)
    h = History(a.directory, a.source, a.archive, a.network)
    if a.mode == "status":
        h.status()
        return
    with (h.root / (a.mode + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (h.root / (a.mode + ".pid")).write_text(str(os.getpid()))
        if a.mode == "discover":
            h.discover(a.limit, a.parent)
        elif a.mode == "verify":
            h.verify(a.limit, a.retry_quarantine, a.txid, a.workers)
        elif a.mode == "export":
            if not a.output:
                p.error("--output required for export")
            print(json.dumps({"exported": h.export(a.output)}))


if __name__ == "__main__":
    main()
