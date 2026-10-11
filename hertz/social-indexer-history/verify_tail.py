#!/usr/bin/env python3
"""Archive-verify captured live K events separately; native replay remains pending."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
from history import History, HEX64, CapacityPause
from live_time import preserve_observed_time


class TailHistory(History):
    scope = "captured-live-event-tail"
    kinds = {"post", "quote", "reply", "broadcast", "follow", "block", "vote"}

    def verify_one(self, txid, observations=None):
        if observations is None:
            observations = list(
                self.db.execute("SELECT data FROM observations WHERE txid=?", (txid,))
            )
        observed = {json.loads(encoded)["block_time"] for (encoded,) in observations}
        if len(observed) != 1:
            raise ValueError("Missing/conflicting captured live timestamps")
        kind = super().verify_one(txid, observations)
        preserve_observed_time(self, txid, observed.pop())
        return kind

    def request(self, url):
        value = super().request(url)
        used = sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())
        # Reserve room for the response, derived proof and ledger overhead before
        # History.cache persists it. A budget pause never quarantines a valid tx.
        if used + 3 * len(json.dumps(value).encode()) + 1024**2 > 200 * 1024**2:
            raise CapacityPause("Tail archive evidence would exceed200MiB budget")
        return value

    def __init__(self, root, source, archive, network, shared_rate_root, captured):
        super().__init__(root, source, archive, network)
        self.rate_root = Path(shared_rate_root).resolve()
        self.captured = Path(captured).resolve()
        manifest = json.loads((self.captured / "manifest.json").read_text())
        if manifest["configuration"]["network"] != network:
            self.db.close()
            raise ValueError("Captured tail network mismatch")
        with sqlite3.connect(
            f"file:{self.rate_root / 'inventory.sqlite'}?mode=ro", uri=True
        ) as db:
            shared = db.execute("SELECT value FROM config WHERE id=1").fetchone()
        if shared != self.db.execute("SELECT value FROM config WHERE id=1").fetchone():
            self.db.close()
            raise ValueError("Shared archive source/network mismatch")
        marker = json.dumps(
            {
                "capture": str(self.captured),
                "manifest": manifest,
                "shared_rate_root": str(self.rate_root),
            },
            sort_keys=True,
        )
        prior = self.db.execute("SELECT value FROM config WHERE id=2").fetchone()
        if prior and prior[0] != marker:
            self.db.close()
            raise ValueError("Tail verifier bound to another capture")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO config VALUES(2,?)", (marker,))
        os.environ["CHATS_RATE_DIR"] = str(self.rate_root)
        self.deadline = (
            manifest["started_at"] + manifest["configuration"]["max_seconds"]
        )

    def seed_captured(self):
        with sqlite3.connect(
            f"file:{self.captured / 'inventory.sqlite'}?mode=ro", uri=True
        ) as db:
            rows = db.execute(
                "SELECT txid,payload,block_time FROM records ORDER BY block_time,txid"
            ).fetchall()
        with self.db:
            for txid, payload, stamp in rows:
                if not HEX64.fullmatch(txid) or not bytes.fromhex(payload).startswith(
                    b"k:1:"
                ):
                    raise ValueError("Invalid captured payload")
                observation = json.dumps(
                    {"payload": payload, "block_time": stamp}, sort_keys=True
                )
                prior = self.db.execute(
                    "SELECT data FROM observations WHERE txid=? AND job='captured-tail'",
                    (txid,),
                ).fetchone()
                if prior and prior[0] != observation:
                    raise ValueError("Captured observation changed")
                self.db.execute(
                    "INSERT OR IGNORE INTO records(txid) VALUES(?)", (txid,)
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO observations VALUES(?,?,?)",
                    (txid, "captured-tail", observation),
                )

    def verify_observations(self, kind, fields, observations):
        if not observations:
            raise ValueError("Missing original captured payload")
        payload = ":".join(fields).encode().hex()
        for (encoded,) in observations:
            if json.loads(encoded)["payload"] != payload:
                raise ValueError("Captured payload differs from accepted archive")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ["output", "captured", "shared-rate-root"]:
        p.add_argument("--" + name, type=Path, required=True)
    for name in ["source", "archive"]:
        p.add_argument("--" + name, required=True)
    p.add_argument("--network", choices=["mainnet", "testnet-10"], required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "verify.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (args.output / "verify.pid").write_text(str(os.getpid()))
        h = TailHistory(
            args.output,
            args.source,
            args.archive,
            args.network,
            args.shared_rate_root,
            args.captured,
        )
        try:
            while True:
                h.seed_captured()
                h.verify(0, workers=1)
                h.status()
                remaining = h.deadline - time.time()
                if remaining <= 0:
                    break
                time.sleep(min(300, remaining))
        finally:
            h.db.close()
