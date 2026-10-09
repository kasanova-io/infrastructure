#!/usr/bin/env python3
"""Fresh current-broadcast projection using the existing paced discovery/verifier."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import shutil
from history import History, SIGNER_KEY, load, CapacityPause


class Profiles(History):
    # Same record meaning as original content, but a separate dated inventory.
    kinds = {"broadcast"}

    def __init__(self, root, source, archive, network, content):
        if Path(root).resolve() == Path(content).resolve():
            raise ValueError("Fresh profiles require a separate inventory")
        super().__init__(root, source, archive, network)
        self.rate_root = Path(content).resolve()
        with sqlite3.connect(
            f"file:{self.rate_root / 'inventory.sqlite'}?mode=ro", uri=True
        ) as db:
            shared = db.execute("SELECT value FROM config WHERE id=1").fetchone()
        if shared != self.db.execute("SELECT value FROM config WHERE id=1").fetchone():
            self.db.close()
            raise ValueError("Profile source/archive/network mismatch")
        marker = json.dumps(
            {"content": str(self.rate_root), "projection": "fresh-current-broadcasts"},
            sort_keys=True,
        )
        prior = self.db.execute("SELECT value FROM config WHERE id=2").fetchone()
        if prior and prior[0] != marker:
            self.db.close()
            raise ValueError("Fresh profile inventory binding changed")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO config VALUES(2,?)", (marker,))
        os.environ["CHATS_RATE_DIR"] = str(self.rate_root)

    def add_job(self, route, params, envelope):
        # Reuse cursor validation/observation storage without expanding replies.
        if route == "/get-users":
            super().add_job(route, params, envelope)

    def verify_observations(self, kind, fields, observations):
        super().verify_observations(kind, fields, observations)
        if kind != "broadcast" or len(fields) < 8 or not observations:
            raise ValueError("Expected observed profile broadcast")
        for (encoded,) in observations:
            row = json.loads(encoded)
            if (
                row.get("userNickname") != fields[5]
                or (row.get("userProfileImage") or "") != fields[6]
                or row.get("postContent") != fields[7]
                or row.get("blockedUser") is True
            ):
                raise ValueError("Fresh profile fields differ from archive payload")

    def cache(self, directory, key, url):
        # Reuse raw immutable archive responses, never the prior prepared verdict.
        # History.verify_one rechecks ID/acceptance/network/block membership and
        # the fresh observation before writing this inventory's verified record.
        if shutil.disk_usage(self.root).free < 2 * 1024**3:
            raise CapacityPause("Fresh profiles paused: less than2GiB free")
        prior = self.rate_root / directory / (key + ".json.gz")
        if directory in {"archive", "blocks"} and prior.exists():
            return load(prior)
        return super().cache(directory, key, url)

    def latest_observed(self):
        if self.db.execute("SELECT count(*) FROM jobs WHERE done=0").fetchone()[0]:
            raise ValueError("Fresh profile pagination is incomplete")
        owners = {}
        for txid, encoded in self.db.execute("SELECT txid,data FROM observations"):
            row = json.loads(encoded)
            owner = row.get("userPublicKey", "")
            if not SIGNER_KEY.fullmatch(owner) or row.get("id") != txid:
                raise ValueError("Invalid original profile identity")
            if owner in owners and owners[owner] != txid:
                raise ValueError("Profile changed during paginated observation")
            owners[owner] = txid
        return owners


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ["output", "content"]:
        p.add_argument("--" + name, type=Path, required=True)
    for name in ["source", "archive"]:
        p.add_argument("--" + name, required=True)
    p.add_argument("--network", choices=["mainnet", "testnet-10"], required=True)
    p.add_argument("--mode", choices=["discover", "verify"], required=True)
    p.add_argument("--limit", type=int, default=30)
    a = p.parse_args()
    if not 1 <= a.limit <= 1000:
        p.error("Bounded request/verification limit must be1..1000")
    h = Profiles(a.output, a.source, a.archive, a.network, a.content)
    try:
        with (a.output / (a.mode + ".lock")).open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            (a.output / (a.mode + ".pid")).write_text(str(os.getpid()))
            if a.mode == "discover":
                h.discover(a.limit)
                owners = h.latest_observed()
                print(
                    json.dumps(
                        {
                            "current_profile_owners": len(owners),
                            "xonly_owners": sum(len(k) == 64 for k in owners),
                            "source_snapshot_atomic": False,
                        }
                    )
                )
            else:
                h.latest_observed()
                h.verify(a.limit, workers=1)
                h.status()
    finally:
        h.db.close()
