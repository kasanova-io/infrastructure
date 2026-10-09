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


def validate_records(
    data,
    network,
    allow_relationship_snapshot=False,
    allow_live_overlay=False,
    allow_live_tail=False,
):
    records = [json.loads(line) for line in data.decode().splitlines() if line.strip()]
    last = None
    seen = set()
    stateful_times = {}
    for row in records:
        txid = row["transaction_id"]
        if not HEX64.fullmatch(txid) or txid in seen:
            raise ValueError("Invalid/duplicate transaction ID")
        seen.add(txid)
        allowed = (
            KINDS
            | ({"follow", "block"} if allow_relationship_snapshot else set())
            | ({"vote"} if allow_live_overlay or allow_live_tail else set())
            | ({"follow", "block"} if allow_live_tail else set())
        )
        if row["network"] != network or row["kind"] not in allowed:
            raise ValueError("Wrong network/action")
        raw = bytes.fromhex(row["payload"])
        if hashlib.sha256(raw).hexdigest() != row["payload_sha256"]:
            raise ValueError("Payload digest mismatch")
        if not raw.startswith(("k:1:" + row["kind"] + ":").encode()):
            raise ValueError("Action mismatch")
        if row["kind"] == "vote":
            fields = raw.decode().split(":")
            if (
                not (
                    row.get("recovery_scope") == "live-content-and-vote-projection"
                    or (
                        allow_live_tail
                        and row.get("recovery_scope") == "captured-live-event-tail"
                    )
                )
                or len(fields) < 8
                or not HEX64.fullmatch(fields[5])
                or fields[6] not in {"upvote", "downvote"}
            ):
                raise ValueError(
                    "Only explicitly verified live vote overlays are allowed"
                )
        if row["kind"] in {"follow", "block"}:
            fields = raw.decode().split(":")
            is_tail = (
                allow_live_tail
                and row.get("recovery_scope") == "captured-live-event-tail"
            )
            if (
                not (
                    row.get("recovery_scope") == "current-relationship-projection"
                    or is_tail
                )
                or len(fields) != 7
                or fields[5]
                not in ({row["kind"], "un" + row["kind"]} if is_tail else {row["kind"]})
                or not re.fullmatch(r"(?:0[23])?[0-9a-fA-F]{64}", fields[6])
            ):
                raise ValueError(
                    "Only explicitly marked active relationship projections are allowed"
                )
            state_key = (
                row["kind"],
                bytes.fromhex(fields[3]),
                bytes.fromhex(fields[6]),
            )
            if stateful_times.get(state_key) == int(row["block_time"]):
                raise ValueError(
                    "Equal-time relationship transitions need explicit ordering"
                )
            stateful_times[state_key] = int(row["block_time"])
        if row.get("recovery_scope") == "captured-live-event-tail":
            proof = row.get("live_timestamp_provenance", {})
            if (
                not allow_live_tail
                or proof.get("observed_block_time") != row["block_time"]
                or proof.get("matched_containing_block") != row["containing_block"]
                or proof.get("policy")
                != "preserve-exact-live-time-after-block-membership-verification"
            ):
                raise ValueError(
                    "Live tail requires exact observed timestamp membership proof"
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

    def run(
        self,
        path,
        network,
        allow_relationship_snapshot=False,
        allow_live_overlay=False,
        allow_live_tail=False,
    ):
        data = path.read_bytes()
        records = validate_records(
            data,
            network,
            allow_relationship_snapshot,
            allow_live_overlay,
            allow_live_tail,
        )
        if not records:
            raise ValueError("An immutable staging batch cannot be empty")
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
        runtime = self.verify_tail_isolation(records)
        offset = self.prepare_lineage(records, batch, network, runtime=runtime)
        done = self.replay_records(records, offset)
        if self.verify_tail_isolation(records) != runtime:
            raise ValueError("Private runtime changed during native replay")
        self.complete_lineage(batch, records, offset)
        return done

    def run_delta(self, path, network, parent_sha256):
        """Append only a strictly later immutable tail to a completed private parent."""
        if not HEX64.fullmatch(parent_sha256):
            raise ValueError("Delta requires the exact parent batch digest")
        data = path.read_bytes()
        records = validate_records(data, network, False, False, True)
        if not records or any(
            row.get("recovery_scope") != "captured-live-event-tail" for row in records
        ):
            raise ValueError("Delta must contain only verified captured tail events")
        if self.sql("SELECT value FROM k_vars WHERE key='network'") != network:
            raise ValueError("Processor network mismatch")
        if self.sql("SELECT network FROM history_batch") != network:
            raise ValueError("Parent batch network mismatch")
        batch = hashlib.sha256(data).hexdigest()
        prior_ids = self.sql(
            "SELECT encode(transaction_id,'hex') FROM history_replay"
        ).splitlines()
        permitted = [{"transaction_id": txid} for txid in prior_ids] + records
        runtime = self.verify_tail_isolation(permitted)
        offset = self.prepare_lineage(records, batch, network, parent_sha256, runtime)
        done = self.replay_records(records, offset)
        if self.verify_tail_isolation(permitted) != runtime:
            raise ValueError("Private runtime changed during delta replay")
        self.complete_lineage(batch, records, offset)
        return done

    def prepare_lineage(self, records, batch, network, parent=None, runtime=None):
        if not records:
            raise ValueError("An immutable staging batch cannot be empty")
        latest_raw = self.sql(
            "SELECT json_build_object('sha256',sha256,'parent',parent_sha256,'manifest',manifest,'complete',complete)::text FROM history_lineage ORDER BY (manifest->>'end_ordinal')::bigint DESC LIMIT 1"
        )
        latest = json.loads(latest_raw) if latest_raw else None
        existing_raw = self.sql(
            f"SELECT json_build_object('parent',parent_sha256,'manifest',manifest,'complete',complete)::text FROM history_lineage WHERE sha256='{batch}'"
        )
        existing = json.loads(existing_raw) if existing_raw else None
        parent_manifest = None
        offset = 0
        if parent is not None:
            parent_raw = self.sql(
                f"SELECT json_build_object('manifest',manifest,'complete',complete)::text FROM history_lineage WHERE sha256='{parent}'"
            )
            parent_row = json.loads(parent_raw) if parent_raw else None
            if not parent_row or not parent_row["complete"]:
                raise ValueError("Delta parent is absent or incomplete")
            parent_manifest = parent_row["manifest"]
            if parent_manifest["network"] != network:
                raise ValueError("Delta parent network mismatch")
            if parent_manifest.get("runtime") != runtime:
                raise ValueError("Delta runtime differs from the completed parent")
            offset = parent_manifest["end_ordinal"]
            if records[0]["block_time"] <= parent_manifest["last_block_time"]:
                raise ValueError(
                    "Delta overlaps or shares the parent chronological boundary"
                )
        manifest = {
            "network": network,
            "record_count": len(records),
            "start_ordinal": offset + 1,
            "end_ordinal": offset + len(records),
            "first_block_time": records[0]["block_time"],
            "first_transaction_id": records[0]["transaction_id"],
            "last_block_time": records[-1]["block_time"],
            "last_transaction_id": records[-1]["transaction_id"],
            "runtime": runtime,
        }
        if existing:
            if (
                not latest
                or latest["sha256"] != batch
                or existing["parent"] != parent
                or existing["manifest"] != manifest
            ):
                raise ValueError("Resumed batch lineage differs or has a later child")
        elif (parent is None and latest is not None) or (
            parent is not None
            and (not latest or latest["sha256"] != parent or not latest["complete"])
        ):
            raise ValueError("Delta does not extend the exact completed current head")
        state = json.loads(
            self.sql(
                "SELECT json_build_object('count',count(*),'first',coalesce(min(ordinal),0),'last',coalesce(max(ordinal),0))::text FROM history_replay"
            )
        )
        if (
            state["first"] != (1 if state["count"] else 0)
            or state["last"] != state["count"]
            or not offset <= state["count"] <= manifest["end_ordinal"]
            or (not existing and state["count"] != offset)
            or (
                existing
                and existing["complete"]
                and state["count"] != manifest["end_ordinal"]
            )
        ):
            raise ValueError(
                "Replay ledger count/ordinal boundary differs from lineage"
            )
        if parent_manifest is not None:
            actual_parent = self.sql(
                f"SELECT encode(transaction_id,'hex') || ':' || (record->>'block_time') FROM history_replay WHERE ordinal={offset}"
            )
            if actual_parent != parent_manifest["last_transaction_id"] + ":" + str(
                parent_manifest["last_block_time"]
            ):
                raise ValueError("Parent ledger watermark differs from durable lineage")
        if not existing:
            encoded = json.dumps(manifest, sort_keys=True).encode().hex()
            parent_sql = "NULL" if parent is None else "'" + parent + "'"
            self.sql(
                f"INSERT INTO history_lineage(sha256,parent_sha256,manifest) VALUES('{batch}',{parent_sql},convert_from(decode('{encoded}','hex'),'UTF8')::jsonb)"
            )
        return offset

    def complete_lineage(self, batch, records, offset):
        end = offset + len(records)
        actual = self.sql(
            f"SELECT count(*) FROM history_replay WHERE ordinal>{offset} AND ordinal<={end}"
        )
        if actual != str(len(records)):
            raise ValueError("Cannot complete a partially replayed batch")
        self.sql(f"UPDATE history_lineage SET complete=true WHERE sha256='{batch}'")

    def replay_records(self, records, ordinal_offset=0):
        """One native execution path shared by initial batches and later deltas."""
        done = 0
        for ordinal, row in enumerate(records, ordinal_offset + 1):
            txid = row["transaction_id"]
            raw = row["payload"]
            stamp = int(row["block_time"])
            encoded = (
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode().hex()
            )
            known = self.sql(
                f"SELECT record=convert_from(decode('{encoded}','hex'),'UTF8')::jsonb AND ordinal={ordinal} FROM history_replay WHERE transaction_id=decode('{txid}','hex')"
            )
            if known:
                if known != "t":
                    raise ValueError("Conflicting prior replay record")
                done += 1
                continue
            fields = bytes.fromhex(raw).decode().split(":")
            undo = (
                row["kind"] in {"follow", "block"} and fields[5] == "un" + row["kind"]
            )
            undo_query = self.prepare_undo(row, encoded) if undo else None
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
                "vote": "k_votes",
            }.get(row["kind"], "k_contents")
            expected_key = row["sender_pubkey"]
            if not re.fullmatch(r"(?:0[23])?[0-9a-f]{64}", expected_key):
                raise ValueError("Invalid signer")
            found = False
            for attempt in range(100):
                result = self.sql(
                    undo_query
                    if undo
                    else f"SELECT encode(sender_pubkey,'hex') || ':' || block_time FROM {table} WHERE transaction_id=decode('{txid}','hex')"
                )
                if (undo and result == "") or (
                    not undo and result == expected_key + ":" + str(stamp)
                ):
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

    def verify_tail_isolation(self, records):
        """Undo effects require one private parser and no unrelated raw inputs."""
        identifiers = subprocess.check_output(
            self.command + ["ps", "-q"], text=True
        ).split()
        if len(identifiers) != 3:
            raise ValueError("Tail replay requires exactly three private services")
        containers = json.loads(
            subprocess.check_output(["docker", "inspect", *identifiers], text=True)
        )
        project = self.command[self.command.index("--project-name") + 1]
        expected_network = project + "_history"
        services = set()
        for container in containers:
            config = container["Config"]
            labels = config.get("Labels", {})
            service = labels.get("com.docker.compose.service")
            if (
                labels.get("com.docker.compose.project") != project
                or service not in {"database", "processor", "web"}
                or service in services
                or not container["State"]["Running"]
                or set(container["NetworkSettings"]["Networks"]) != {expected_network}
                or any(container["NetworkSettings"].get("Ports", {}).values())
            ):
                raise ValueError("Tail replay service isolation mismatch")
            services.add(service)
            if service == "processor":
                command = config["Cmd"]
                if command.count("--workers") != 1 or command[
                    command.index("--workers") + 1 :
                ][:1] != ["1"]:
                    raise ValueError("Tail replay requires exactly one parser worker")
        network = json.loads(
            subprocess.check_output(
                ["docker", "network", "inspect", expected_network], text=True
            )
        )[0]
        if not network.get("Internal") or set(network.get("Containers", {})) != {
            c["Id"] for c in containers
        }:
            raise ValueError("Tail replay network has external or extra writers")
        observed = set(
            self.sql(
                "SELECT encode(transaction_id,'hex') FROM transactions"
            ).splitlines()
        )
        if not observed <= {r["transaction_id"] for r in records}:
            raise ValueError("Tail replay contains unrelated raw input")
        return {
            "project": project,
            "network_id": network["Id"],
            "services": {
                c["Config"]["Labels"]["com.docker.compose.service"]: {
                    "container_id": c["Id"],
                    "image": c["Image"],
                    "started_at": c["State"]["StartedAt"],
                }
                for c in containers
            },
        }

    def prepare_undo(self, row, encoded):
        """Require a durable positive predecessor; absence alone proves nothing."""
        txid = row["transaction_id"]
        fields = bytes.fromhex(row["payload"]).decode().split(":")
        kind = row["kind"]
        owner = bytes.fromhex(fields[3]).hex()
        target = bytes.fromhex(fields[6]).hex()
        table, column = (
            ("k_follows", "followed_user_pubkey")
            if kind == "follow"
            else ("k_blocks", "blocked_user_pubkey")
        )
        query = f"SELECT json_build_object('transaction_id',encode(transaction_id,'hex'),'block_time',block_time)::text FROM {table} WHERE sender_pubkey=decode('{owner}','hex') AND {column}=decode('{target}','hex')"
        current = self.sql(query)
        pending = self.sql(
            f"SELECT json_build_object('matches',record=convert_from(decode('{encoded}','hex'),'UTF8')::jsonb,'predecessor',predecessor)::text FROM history_pending_undo WHERE transaction_id=decode('{txid}','hex')"
        )
        if pending:
            witness = json.loads(pending)
            if not witness["matches"]:
                raise ValueError("Conflicting pending undo record")
            if current and json.loads(current) != witness["predecessor"]:
                raise ValueError("Pending undo predecessor changed")
            if not current:
                exists = self.sql(
                    f"SELECT EXISTS(SELECT 1 FROM transactions WHERE transaction_id=decode('{txid}','hex') AND payload=decode('{row['payload']}','hex') AND block_time={int(row['block_time'])})"
                )
                if exists != "t":
                    raise ValueError(
                        "Absent undo predecessor without original submitted input"
                    )
        else:
            if not current:
                raise ValueError(
                    "No-op undo cannot prove native signature acceptance; positive predecessor required"
                )
            prior = json.loads(current)
            if int(prior["block_time"]) >= int(row["block_time"]):
                raise ValueError("Undo predecessor ordering is ambiguous")
            predecessor = json.dumps(prior, sort_keys=True).encode().hex()
            self.sql(
                f"INSERT INTO history_pending_undo VALUES(decode('{txid}','hex'),convert_from(decode('{encoded}','hex'),'UTF8')::jsonb,convert_from(decode('{predecessor}','hex'),'UTF8')::jsonb)"
            )
        return query


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
    p.add_argument("--allow-live-tail", action="store_true")
    p.add_argument(
        "--parent-batch-sha256",
        help="Append a strictly later immutable captured tail to this exact completed parent",
    )
    a = p.parse_args()
    if not PROJECT.fullmatch(a.project):
        raise ValueError("Not a history staging project")
    if a.parent_batch_sha256 and not a.allow_live_tail:
        p.error("--parent-batch-sha256 requires --allow-live-tail")
    with a.compose.with_name(a.project + ".replay.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        instance = Stage(a.compose, a.project)
        completed = (
            instance.run_delta(a.batch, a.network, a.parent_batch_sha256)
            if a.parent_batch_sha256
            else instance.run(
                a.batch,
                a.network,
                a.allow_relationship_snapshot,
                a.allow_live_overlay,
                a.allow_live_tail,
            )
        )
        print(
            json.dumps(
                {
                    "complete_staged": completed,
                    "live_imported": 0,
                }
            )
        )
