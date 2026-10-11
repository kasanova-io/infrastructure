#!/usr/bin/env python3
"""Recover current follow/block projections separately from content history."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import urllib.parse
from history import History, ANON, HEX64, KEY, SIGNER_KEY, atomic_json


class Relationships(History):
    scope = "current-relationship-projection"
    kinds = {"follow", "block"}

    def __init__(self, root, source, archive, network, content):
        if Path(root).resolve() == Path(content).resolve():
            raise ValueError("Relationships require a separate inventory")
        super().__init__(root, source, archive, network)
        self.rate_root = Path(content).resolve()
        content_db = sqlite3.connect(
            f"file:{self.rate_root / 'inventory.sqlite'}?mode=ro", uri=True
        )
        try:
            config = content_db.execute(
                "SELECT value FROM config WHERE id=1"
            ).fetchone()[0]
            if (
                config
                != self.db.execute("SELECT value FROM config WHERE id=1").fetchone()[0]
            ):
                raise ValueError("Content source/archive/network mismatch")
        finally:
            content_db.close()
        marker = json.dumps(
            {
                "scope": "current-relationship-projection",
                "content": str(self.rate_root),
            },
            sort_keys=True,
        )
        prior = self.db.execute("SELECT value FROM config WHERE id=2").fetchone()
        if prior and prior[0] != marker:
            raise ValueError("Relationship inventory ownership mismatch")
        self.db.execute("INSERT OR IGNORE INTO config VALUES(2,?)", (marker,))
        self.db.commit()
        os.environ["CHATS_RATE_DIR"] = str(self.rate_root)

    def owner_jobs(self, owner):
        # The upstream HTTP routes reject x-only owners. Preserve their bytes in
        # discovered edges; never fabricate parity to bypass that restriction.
        if not KEY.fullmatch(owner):
            return
        for route in ("/get-users-following", "/get-users-followers"):
            self.add_job(route, {"userPubkey": owner, "requesterPubkey": ANON}, "posts")
        self.add_job("/get-blocked-users", {"requesterPubkey": owner}, "posts")

    def collect(self, budget=0, owners=None):
        health = self.request(self.source + "/health")
        if health.get("network") != self.network:
            raise ValueError("Relationship source network mismatch")
        atomic_json(self.root / "source-health.json.gz", health)
        if not (self.root / "starting-stats.json.gz").exists():
            atomic_json(
                self.root / "starting-stats.json.gz",
                self.request(self.source + "/stats"),
            )
        if owners:
            for owner in owners:
                if not KEY.fullmatch(owner):
                    raise ValueError("Upstream only supports compressed owner keys")
                self.owner_jobs(owner)
        else:
            content_db = sqlite3.connect(
                f"file:{self.rate_root / 'inventory.sqlite'}?mode=ro", uri=True
            )
            try:
                for (owner,) in content_db.execute(
                    "SELECT DISTINCT json_extract(data,'$.userPublicKey') FROM observations"
                ):
                    self.owner_jobs(owner or "")
            finally:
                content_db.close()
        self.db.commit()
        for _ in range(budget or 10**9):
            job = self.db.execute(
                "SELECT id,route,params,cursor,pages FROM jobs WHERE done=0 ORDER BY id LIMIT 1"
            ).fetchone()
            if not job:
                break
            jid, route, params, cursor, pages = job
            params = json.loads(params)
            query = {**params, "limit": 100}
            if cursor:
                query["before"] = cursor
            page = self.cache(
                "pages",
                jid + "-" + str(pages),
                self.source + route + "?" + urllib.parse.urlencode(query),
            )
            rows, pagination = page.get("posts"), page.get("pagination")
            if (
                not isinstance(rows, list)
                or not isinstance(pagination, dict)
                or not isinstance(pagination.get("hasMore"), bool)
            ):
                raise ValueError("Invalid relationship pagination contract")
            next_cursor = pagination.get("nextCursor")
            if pagination["hasMore"]:
                parts = (next_cursor or "").split("_")
                if not rows or len(parts) != 2 or not all(p.isdigit() for p in parts):
                    raise ValueError("Invalid relationship cursor")
                if cursor and tuple(map(int, parts)) >= tuple(
                    map(int, cursor.split("_"))
                ):
                    raise ValueError("Relationship cursor did not move backward")
            owner = params.get("userPubkey", params["requesterPubkey"])
            with self.db:
                for row in rows:
                    txid, related = row.get("id", ""), row.get("userPublicKey", "")
                    if not HEX64.fullmatch(txid) or not SIGNER_KEY.fullmatch(related):
                        raise ValueError("Invalid relationship identity")
                    sender, target = (
                        (related, owner)
                        if route == "/get-users-followers"
                        else (owner, related)
                    )
                    kind = "block" if route == "/get-blocked-users" else "follow"
                    observation = {
                        "id": txid,
                        "kind": kind,
                        "sender": sender,
                        "target": target,
                        "signature": row["signature"],
                        "timestamp": row["timestamp"],
                        "source_route": route,
                        "projection": "active-at-collection",
                    }
                    prior = self.db.execute(
                        "SELECT data FROM observations WHERE txid=? AND job=?",
                        (txid, jid),
                    ).fetchone()
                    encoded = json.dumps(observation, sort_keys=True)
                    if prior and prior[0] != encoded:
                        raise ValueError("Conflicting relationship observation")
                    self.db.execute(
                        "INSERT OR IGNORE INTO records(txid) VALUES(?)", (txid,)
                    )
                    self.db.execute(
                        "INSERT OR IGNORE INTO observations VALUES(?,?,?)",
                        (txid, jid, encoded),
                    )
                    if not owners:
                        self.owner_jobs(related)
                self.db.execute(
                    "UPDATE jobs SET cursor=?,pages=pages+1,done=? WHERE id=?",
                    (next_cursor, int(not pagination["hasMore"]), jid),
                )
            self.status()

    def verify_observations(self, kind, fields, observations):
        if len(fields) != 7 or fields[5] != kind or not SIGNER_KEY.fullmatch(fields[6]):
            raise ValueError(
                "Only active follow/block actions belong in this projection"
            )
        for (encoded,) in observations:
            row = json.loads(encoded)
            expected = (row["kind"], row["sender"], row["signature"], row["target"])
            actual = (kind, fields[3], fields[4], fields[6])
            if actual[0] != expected[0] or any(
                bytes.fromhex(a) != bytes.fromhex(b)
                for a, b in zip(actual[1:], expected[1:])
            ):
                raise ValueError("Archived relationship differs from source projection")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=["discover", "verify", "status", "export"])
    p.add_argument("directory", type=Path)
    p.add_argument("--content", type=Path, required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--archive", required=True)
    p.add_argument("--network", choices=["mainnet", "testnet-10"], required=True)
    p.add_argument("--owner", action="append")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--workers", type=int, choices=range(1, 5), default=1)
    p.add_argument("--retry-quarantine", action="store_true")
    p.add_argument("--output", type=Path)
    a = p.parse_args()
    os.umask(0o077)
    h = Relationships(a.directory, a.source, a.archive, a.network, a.content)
    with (h.root / (a.mode + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (h.root / (a.mode + ".pid")).write_text(str(os.getpid()))
        if a.mode == "discover":
            h.collect(a.limit, a.owner)
        elif a.mode == "verify":
            h.verify(a.limit, a.retry_quarantine, workers=a.workers)
        elif a.mode == "export":
            if not a.output:
                p.error("--output required")
            print(
                json.dumps(
                    {
                        "exported": h.export(a.output),
                        "scope": "current-relationship-projection",
                    }
                )
            )
        else:
            h.status()
