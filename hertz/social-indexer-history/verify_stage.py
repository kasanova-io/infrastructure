#!/usr/bin/env python3
"""Read every staged item through the real indexer HTTP endpoints."""
import argparse
import collections
import json
from pathlib import Path
import subprocess
import urllib.parse
from stage import Stage, validate_records

ANON = "02" + "0" * 64


def verify(
    stage, path, network, allow_relationship_snapshot=False, allow_live_overlay=False
):
    records = validate_records(
        path.read_bytes(), network, allow_relationship_snapshot, allow_live_overlay
    )
    counts = collections.Counter()
    profiles = {}
    relations = {}
    for row in records:
        fields = bytes.fromhex(row["payload"]).decode().split(":")
        if row["kind"] == "vote":
            url = "http://127.0.0.1:3001/get-vote-details?" + urllib.parse.urlencode(
                {"id": row["transaction_id"], "requesterPubkey": row["sender_pubkey"]}
            )
            vote = json.loads(
                subprocess.check_output(
                    stage.command + ["exec", "-T", "web", "wget", "-qO-", url],
                    text=True,
                )
            )["vote"]
            check_vote(row, vote)
            counts["vote"] += 1
            continue
        if row["kind"] in {"follow", "block"}:
            relations.setdefault((row["kind"], row["sender_pubkey"]), []).append(row)
            continue
        if row["kind"] == "broadcast":
            # Only the final broadcast per signer survives the production model.
            profiles[row["sender_pubkey"]] = row
            continue
        url = "http://127.0.0.1:3001/get-post-details?" + urllib.parse.urlencode(
            {"id": row["transaction_id"], "requesterPubkey": ANON}
        )
        data = json.loads(
            subprocess.check_output(
                stage.command + ["exec", "-T", "web", "wget", "-qO-", url], text=True
            )
        )["post"]
        check(row, data, fields[6 if row["kind"] in ("quote", "reply") else 5])
        counts[row["kind"]] += 1
    for key, row in profiles.items():
        fields = bytes.fromhex(row["payload"]).decode().split(":")
        if len(key) == 64:
            data = find_xonly_profile(stage, row["transaction_id"])
        else:
            url = "http://127.0.0.1:3001/get-user-details?" + urllib.parse.urlencode(
                {"user": key, "requesterPubkey": ANON}
            )
            data = json.loads(
                subprocess.check_output(
                    stage.command + ["exec", "-T", "web", "wget", "-qO-", url],
                    text=True,
                )
            )
        check(row, data, fields[-1])
        if data["userNickname"] != fields[5]:
            raise ValueError("Profile nickname mismatch")
        counts["broadcast"] += 1
    for (kind, owner), rows in relations.items():
        route = "/get-users-following" if kind == "follow" else "/get-blocked-users"
        query = (
            {"userPubkey": owner, "requesterPubkey": ANON}
            if kind == "follow"
            else {"requesterPubkey": owner}
        )
        found = {item["id"]: item for item in read_pages(stage, route, query)}
        for row in rows:
            fields = bytes.fromhex(row["payload"]).decode().split(":")
            item = found.get(row["transaction_id"])
            if not item or (
                bytes.fromhex(item["userPublicKey"]) != bytes.fromhex(fields[6])
                or bytes.fromhex(item["signature"]) != bytes.fromhex(fields[4])
                or item["timestamp"] != row["block_time"]
            ):
                raise ValueError("Relationship API mismatch: " + row["transaction_id"])
            counts[kind] += 1
    result = {
        "api_verified": sum(counts.values()),
        "kinds": dict(counts),
        "live_imported": 0,
    }
    print(json.dumps(result), flush=True)
    return result


def read_pages(stage, route, query):
    cursor = None
    seen = set()
    while True:
        params = {**query, "limit": 100}
        if cursor:
            params["before"] = cursor
        url = "http://127.0.0.1:3001" + route + "?" + urllib.parse.urlencode(params)
        data = json.loads(
            subprocess.check_output(
                stage.command + ["exec", "-T", "web", "wget", "-qO-", url], text=True
            )
        )
        yield from data["posts"]
        if not data["pagination"]["hasMore"]:
            return
        cursor = data["pagination"]["nextCursor"]
        if not cursor or cursor in seen:
            raise ValueError("API relationship pagination stalled")
        seen.add(cursor)


def find_xonly_profile(stage, txid):
    # The pinned user-specific endpoint incorrectly rejects 32-byte keys even
    # though its native parser stores them. Read global profiles without changing
    # identity bytes. This does not establish user-route compatibility.
    cursor = None
    seen = set()
    while True:
        query = {"requesterPubkey": ANON, "limit": 100}
        if cursor:
            query["before"] = cursor
        url = "http://127.0.0.1:3001/get-users?" + urllib.parse.urlencode(query)
        data = json.loads(
            subprocess.check_output(
                stage.command + ["exec", "-T", "web", "wget", "-qO-", url], text=True
            )
        )
        for profile in data["posts"]:
            if profile["id"] == txid:
                return profile
        if not data["pagination"]["hasMore"]:
            raise ValueError("X-only profile missing from global profile API")
        cursor = data["pagination"]["nextCursor"]
        if not cursor or cursor in seen:
            raise ValueError("Profile pagination stalled")
        seen.add(cursor)


def check(row, data, content):
    fields = bytes.fromhex(row["payload"]).decode().split(":")
    for actual, expected in (
        (data["id"], row["transaction_id"]),
        (data["userPublicKey"], row["sender_pubkey"]),
        (bytes.fromhex(data["signature"]), bytes.fromhex(fields[4])),
        (data["timestamp"], row["block_time"]),
        (data["postContent"], content),
    ):
        if actual != expected:
            raise ValueError("Native API readback mismatch: " + row["transaction_id"])


def check_vote(row, data):
    fields = bytes.fromhex(row["payload"]).decode().split(":")
    expected = {
        "id": row["transaction_id"],
        "userPublicKey": row["sender_pubkey"],
        "parentPostId": fields[5],
        "voteType": fields[6],
    }
    if any(data.get(key) != value for key, value in expected.items()):
        raise ValueError("Native vote API readback mismatch: " + row["transaction_id"])


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("batch", type=Path)
    p.add_argument(
        "--compose", type=Path, default=Path(__file__).with_name("compose.yaml")
    )
    p.add_argument("--project", required=True)
    p.add_argument("--network", required=True, choices=["mainnet", "testnet-10"])
    p.add_argument("--allow-relationship-snapshot", action="store_true")
    p.add_argument("--allow-live-overlay", action="store_true")
    a = p.parse_args()
    verify(
        Stage(a.compose, a.project),
        a.batch,
        a.network,
        a.allow_relationship_snapshot,
        a.allow_live_overlay,
    )
