#!/usr/bin/env python3
"""Archive-verify a frozen live content/vote projection; never write a service."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from history import History, HEX64
from live_time import preserve_observed_time


class LiveOverlay(History):
    scope = "live-content-and-vote-projection"
    kinds = {"post", "quote", "reply", "vote"}

    def verify_one(self, txid, observations=None):
        if observations is None:
            observations = list(
                self.db.execute("SELECT data FROM observations WHERE txid=?", (txid,))
            )
        observed = {json.loads(encoded)["timestamp"] for (encoded,) in observations}
        if len(observed) != 1:
            raise ValueError("Missing/conflicting observed live timestamps")
        kind = super().verify_one(txid, observations)
        preserve_observed_time(self, txid, observed.pop())
        return kind

    def __init__(self, root, source, archive, network, shared_rate_root, snapshot):
        data = Path(snapshot).read_bytes()
        value = json.loads(data)
        if (
            value.get("network") != network
            or value.get("scope") != "live-projection-read-only"
        ):
            raise ValueError("Wrong live projection scope/network")
        rows = value.get("records")
        if not isinstance(rows, list) or not rows:
            raise ValueError("Expected a nonempty frozen live projection")
        ids = [row.get("id", "") for row in rows]
        if any(not HEX64.fullmatch(txid) for txid in ids) or len(set(ids)) != len(ids):
            raise ValueError("Invalid/duplicate live transaction IDs")
        if any(row.get("contentType") not in self.kinds for row in rows):
            raise ValueError("Unsupported live projection action")
        super().__init__(root, source, archive, network)
        self.rate_root = Path(shared_rate_root).resolve()
        with sqlite3.connect(
            f"file:{self.rate_root / 'inventory.sqlite'}?mode=ro", uri=True
        ) as db:
            shared_config = db.execute("SELECT value FROM config WHERE id=1").fetchone()
        if (
            shared_config
            != self.db.execute("SELECT value FROM config WHERE id=1").fetchone()
        ):
            self.db.close()
            raise ValueError("Shared archive source/network mismatch")
        marker = json.dumps(
            {
                "snapshot_sha256": hashlib.sha256(data).hexdigest(),
                "rate_root": str(self.rate_root),
            },
            sort_keys=True,
        )
        prior = self.db.execute("SELECT value FROM config WHERE id=2").fetchone()
        if prior and prior[0] != marker:
            self.db.close()
            raise ValueError("Live overlay already bound to another frozen snapshot")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO config VALUES(2,?)", (marker,))
            for row in rows:
                self.db.execute(
                    "INSERT OR IGNORE INTO records(txid) VALUES(?)", (row["id"],)
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO observations VALUES(?,?,?)",
                    (row["id"], "live-snapshot", json.dumps(row, sort_keys=True)),
                )
        os.environ["CHATS_RATE_DIR"] = str(self.rate_root)

    def verify_observations(self, kind, fields, observations):
        if not observations:
            raise ValueError("Live overlay needs original database observation")
        super().verify_observations(kind, fields, observations)
        for (encoded,) in observations:
            row = json.loads(encoded)
            if row["contentType"] != kind:
                raise ValueError("Live action differs from archive")
            if kind == "vote":
                if (
                    len(fields) < 8
                    or row["parentPostId"] != fields[5]
                    or row["voteType"] != fields[6]
                    or fields[6] not in {"upvote", "downvote"}
                ):
                    raise ValueError("Live vote target/value differs from archive")
            else:
                offset = 6 if kind in {"quote", "reply"} else 5
                if row["postContent"] != fields[offset]:
                    raise ValueError("Live content differs from archive")
                if kind in {"quote", "reply"} and row["parentPostId"] != fields[5]:
                    raise ValueError("Live content parent differs from archive")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("directory", type=Path)
    p.add_argument("--snapshot", type=Path, required=True)
    p.add_argument("--shared-rate-root", type=Path, required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--archive", required=True)
    p.add_argument("--network", choices=["mainnet", "testnet-10"], required=True)
    a = p.parse_args()
    a.directory.mkdir(parents=True, exist_ok=True)
    with (a.directory / "verify.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        h = LiveOverlay(
            a.directory, a.source, a.archive, a.network, a.shared_rate_root, a.snapshot
        )
        try:
            h.verify(0, workers=1)
            h.status()
        finally:
            h.db.close()
