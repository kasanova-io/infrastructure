#!/usr/bin/env python3
"""Feed immutable verified batches serially through the real parser, staging only."""
import argparse
import atexit
import fcntl
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid

HEX64 = re.compile(r"^[0-9a-f]{64}$")
PROJECT = re.compile(r"^social_history_[a-z0-9_]+$")
KINDS = {"post", "quote", "reply", "broadcast"}


def validate_records(data, network, allow_relationship_snapshot=False):
    records = [json.loads(line) for line in data.decode().splitlines() if line.strip()]
    last = None
    seen = set()
    for row in records:
        txid = row["transaction_id"]
        if not HEX64.fullmatch(txid) or txid in seen:
            raise ValueError("Invalid/duplicate transaction ID")
        seen.add(txid)
        allowed = KINDS | (
            {"follow", "block"} if allow_relationship_snapshot else set()
        )
        if row["network"] != network or row["kind"] not in allowed:
            raise ValueError("Wrong network/action")
        raw = bytes.fromhex(row["payload"])
        if hashlib.sha256(raw).hexdigest() != row["payload_sha256"]:
            raise ValueError("Payload digest mismatch")
        if not raw.startswith(("k:1:" + row["kind"] + ":").encode()):
            raise ValueError("Action mismatch")
        if row["kind"] in {"follow", "block"}:
            fields = raw.decode().split(":")
            if (
                row.get("recovery_scope") != "current-relationship-projection"
                or len(fields) != 7
                or fields[5] != row["kind"]
                or not re.fullmatch(r"(?:0[23])?[0-9a-fA-F]{64}", fields[6])
            ):
                raise ValueError(
                    "Only explicitly marked active relationship projections are allowed"
                )
        if not all(
            HEX64.fullmatch(row[k]) for k in ("accepting_block", "containing_block")
        ):
            raise ValueError("Missing acceptance evidence")
        key = (int(row["block_time"]), txid)
        if last and key <= last:
            raise ValueError("Batch not strictly chronological")
        last = key
    return records


class Stage:
    def __init__(self, compose, project):
        if not PROJECT.fullmatch(project):
            raise ValueError("Not a history staging project")
        self.command = [
            "docker",
            "compose",
            "--project-name",
            project,
            "-f",
            str(compose),
        ]
        # Verify the actual container labels; never target a supplied arbitrary DB.
        ids = (
            subprocess.check_output(self.command + ["ps", "-q", "database"], text=True)
            .strip()
            .splitlines()
        )
        if len(ids) != 1:
            raise ValueError("Expected one independent staging database")
        label = subprocess.check_output(
            [
                "docker",
                "inspect",
                "--format",
                '{{index .Config.Labels "com.docker.compose.project"}}',
                ids[0],
            ],
            text=True,
        ).strip()
        if label != project:
            raise ValueError("Database project ownership mismatch")
        self.process = subprocess.Popen(
            self.command
            + [
                "exec",
                "-T",
                "database",
                "psql",
                "-U",
                "social_history",
                "-d",
                "social_history_staging",
                "-Atq",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        atexit.register(self.close)
        self.sql("SET statement_timeout='15s'")
        guard = self.sql(
            "SELECT current_database() || ':' || purpose FROM history_stage_guard WHERE singleton=true"
        )
        if guard != "social_history_staging:isolated-social-history":
            raise ValueError("Not the marked isolated staging database")

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.close()
            except BrokenPipeError:
                pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait(timeout=5)

    def sql(self, query):
        # Keep one psql session rather than starting Docker clients for every
        # polling query. All SQL is internal; payloads/records are hex encoded.
        marker = "history_result_" + uuid.uuid4().hex
        self.process.stdin.write(query.rstrip(";\n") + ";\n\\echo " + marker + "\n")
        self.process.stdin.flush()
        rows = []
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError(
                    "Staging SQL session stopped: " + self.process.stderr.read()
                )
            if line.rstrip("\n") == marker:
                return "\n".join(rows).strip()
            rows.append(line.rstrip("\n"))

    def run(self, path, network, allow_relationship_snapshot=False):
        data = path.read_bytes()
        records = validate_records(data, network, allow_relationship_snapshot)
        batch = hashlib.sha256(data).hexdigest()
        source_network = self.sql("SELECT value FROM k_vars WHERE key='network'")
        if source_network != network:
            raise ValueError("Processor network mismatch")
        prior = self.sql("SELECT sha256 FROM history_batch")
        if prior and prior != batch:
            raise ValueError(
                "Staging already bound to another immutable batch; use a fresh staging project"
            )
        self.sql(
            f"INSERT INTO history_batch VALUES (true,'{batch}','{network}') ON CONFLICT DO NOTHING"
        )
        if self.sql("SELECT sha256 FROM history_batch") != batch:
            raise ValueError("Another batch already owns this staging database")
        done = 0
        for ordinal, row in enumerate(records, 1):
            txid = row["transaction_id"]
            raw = row["payload"]
            stamp = int(row["block_time"])
            encoded = (
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode().hex()
            )
            known = self.sql(
                f"SELECT record=convert_from(decode('{encoded}','hex'),'UTF8')::jsonb FROM history_replay WHERE transaction_id=decode('{txid}','hex')"
            )
            if known:
                if known != "t":
                    raise ValueError("Conflicting prior replay record")
                done += 1
                continue
            # Each previous parser effect is confirmed before the next NOTIFY.
            # Re-notify an unledgered row after interruption; dedup is native.
            conflict = self.sql(
                f"SELECT payload<>decode('{raw}','hex') OR block_time<>{stamp} FROM transactions WHERE transaction_id=decode('{txid}','hex')"
            )
            if conflict == "t":
                raise ValueError("Raw transaction conflict")
            if conflict:
                self.sql(f"SELECT pg_notify('transaction_channel','{txid}')")
            else:
                self.sql(
                    f"INSERT INTO transactions VALUES (decode('{txid}','hex'),decode('{raw}','hex'),{stamp})"
                )
            table = {
                "broadcast": "k_broadcasts",
                "follow": "k_follows",
                "block": "k_blocks",
            }.get(row["kind"], "k_contents")
            expected_key = row["sender_pubkey"]
            if not re.fullmatch(r"(?:0[23])?[0-9a-f]{64}", expected_key):
                raise ValueError("Invalid signer")
            found = False
            for attempt in range(100):
                result = self.sql(
                    f"SELECT encode(sender_pubkey,'hex') || ':' || block_time FROM {table} WHERE transaction_id=decode('{txid}','hex')"
                )
                if result == expected_key + ":" + str(stamp):
                    found = True
                    break
                time.sleep(0.1)
            if not found:
                raise ValueError(
                    "Native parser produced no matching row (invalid signature, dedup signature or parser error): "
                    + txid
                )
            # The original parser owns signature verification and all projections.
            # Persistent evidence is recorded only after its observable effect.
            self.sql(
                f"INSERT INTO history_replay VALUES(decode('{txid}','hex'),convert_from(decode('{encoded}','hex'),'UTF8')::jsonb,{ordinal})"
            )
            done += 1
            print(
                json.dumps(
                    {
                        "staged": done,
                        "total": len(records),
                        "last_transaction": txid,
                        "live_imported": 0,
                    }
                ),
                flush=True,
            )
        return done


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("batch", type=Path)
    p.add_argument(
        "--compose", type=Path, default=Path(__file__).with_name("compose.yaml")
    )
    p.add_argument("--project", required=True)
    p.add_argument("--network", required=True, choices=["mainnet", "testnet-10"])
    p.add_argument("--allow-relationship-snapshot", action="store_true")
    a = p.parse_args()
    if not PROJECT.fullmatch(a.project):
        raise ValueError("Not a history staging project")
    with a.compose.with_name(a.project + ".replay.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(
            json.dumps(
                {
                    "complete_staged": Stage(a.compose, a.project).run(
                        a.batch, a.network, a.allow_relationship_snapshot
                    ),
                    "live_imported": 0,
                }
            )
        )
