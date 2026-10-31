"""Durable handoff from Simply Kaspa to all six protocol adapter families."""

from importlib.resources import files
import time

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .routing import Family, route
from .source import SIMPLY_REVISION


def connect(dsn):
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def install(conn, network, activated_at_ms=None):
    """Bind this database to one configured network; seed existing rows quietly."""
    if network != "mainnet" and not (network.startswith("testnet-") and network[8:].isdigit()):
        raise ValueError("network must be an explicit Kaspa network identifier")
    with conn.transaction():
        # Serialize installation as well as live startup. Never infer a network
        # from address prefixes, a caller's backfill file, or a Docker name.
        conn.execute("SELECT pg_advisory_xact_lock(736124, 1)")
        row = conn.execute("SELECT value FROM public.vars WHERE key = 'schema_version'").fetchone()
        if not row or row["value"] != "23":
            raise ValueError("review adapter against Simply Kaspa schema; expected version 23")
        conn.execute(files(__package__).joinpath("schema.sql").read_text())
        current = conn.execute("SELECT * FROM protocol_indexer.config").fetchone()
        if current and (current["network"] != network or current["source_revision"] != SIMPLY_REVISION):
            raise ValueError("database is bound to another network/source revision")
        conn.execute(
            """INSERT INTO protocol_indexer.config (network, source_revision, activated_at_ms)
               VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
            (network, SIMPLY_REVISION, activated_at_ms if activated_at_ms is not None else time.time_ns() // 1_000_000),
        )
        # Insert triggers before inventorying so concurrent writes are queued.
        # Reinstallation only queues rows not already durably captured.
        conn.execute("""
            INSERT INTO protocol_indexer.pending (transaction_id)
            SELECT t.transaction_id FROM public.transactions t
            WHERE protocol_indexer.is_candidate(t.payload)
              AND NOT EXISTS (SELECT 1 FROM protocol_indexer.canonical c
                              WHERE c.transaction_id = t.transaction_id)
            ON CONFLICT DO NOTHING
        """)


def capture_one(conn):
    """Atomically capture the latest raw body/acceptance and acknowledge its queue.

    Simply inserts transaction bodies and acceptances concurrently. Triggers
    enqueue both orders; the worker reads after commit, rather than doing a
    snapshot-dependent join inside an upstream insertion trigger.
    """
    with conn.transaction():
        pending = conn.execute("""
            SELECT p.* FROM protocol_indexer.pending p
            WHERE EXISTS (SELECT 1 FROM public.transactions t WHERE t.transaction_id = p.transaction_id)
               OR EXISTS (SELECT 1 FROM protocol_indexer.canonical c WHERE c.transaction_id = p.transaction_id)
            ORDER BY p.queued_at, p.transaction_id LIMIT 1 FOR UPDATE OF p SKIP LOCKED
        """).fetchone()
        if pending is None:
            return False
        txid = pending["transaction_id"]
        old = conn.execute("SELECT * FROM protocol_indexer.canonical WHERE transaction_id = %s", (txid,)).fetchone()
        source = conn.execute("SELECT to_jsonb(t) AS body FROM public.transactions t WHERE transaction_id = %s", (txid,)).fetchone()
        acceptance = conn.execute("SELECT block_hash FROM public.transactions_acceptances WHERE transaction_id = %s", (txid,)).fetchone()
        if source is None and old is None:
            # The acceptance can commit before the body. Keep pending until the
            # body arrives; do not fabricate an empty transaction or lose it.
            conn.execute("UPDATE protocol_indexer.pending SET queued_at = clock_timestamp() WHERE transaction_id = %s", (txid,))
            return False
        body = source["body"] if source else old["source_transaction"]
        payload = bytes.fromhex(body["payload"][2:]) if body["payload"] else b""
        if old is None and not payload.startswith((b"kchat:", b"ciph_msg:", b"k:")):
            # An acceptance can arrive before its body and needs a durable wakeup
            # even when the body later proves unrelated to these protocols.
            conn.execute("DELETE FROM protocol_indexer.pending WHERE transaction_id = %s", (txid,))
            return True
        if old and old["source_transaction"]["payload"] != body["payload"]:
            raise ValueError("conflicting payload for an existing transaction")
        block = acceptance["block_hash"] if acceptance else (
            pending["accepting_block"] if pending["acceptance_changed"] else old["accepting_block"] if old else None
        )
        if old and old["source_transaction"] == body and old["accepting_block"] == block:
            conn.execute("DELETE FROM protocol_indexer.pending WHERE transaction_id = %s", (txid,))
            return True
        revision = old["revision"] + 1 if old else 1
        ever_accepted = bool(block is not None or (old and old["ever_accepted"]))
        if block is not None:
            kind = "updated" if old and old["accepting_block"] == block else "accepted"
        else:
            kind = "revoked" if old and old["accepting_block"] is not None else "observed"
        classified = route(payload)
        config = conn.execute("SELECT * FROM protocol_indexer.config").fetchone()
        notify = bool(kind == "accepted" and not (old and old["ever_accepted"])
                      and body["block_time"] is not None and body["block_time"] >= config["activated_at_ms"])
        conn.execute("""
            INSERT INTO protocol_indexer.canonical
                (transaction_id, revision, family, source_transaction, accepting_block, ever_accepted)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (transaction_id) DO UPDATE
                SET revision = EXCLUDED.revision, source_transaction = EXCLUDED.source_transaction,
                    accepting_block = EXCLUDED.accepting_block, ever_accepted = EXCLUDED.ever_accepted,
                    family = EXCLUDED.family
        """, (txid, revision, classified.family.value if classified else None, Jsonb(body), block, ever_accepted))
        event = conn.execute("""
            INSERT INTO protocol_indexer.events
                (transaction_id, revision, family, kind, source_transaction, accepting_block, notify_allowed)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING event_id
        """, (txid, revision, classified.family.value if classified else None, kind, Jsonb(body), block, notify)).fetchone()
        if classified:
            conn.execute("INSERT INTO protocol_indexer.delivery_pending VALUES (%s, %s, %s, %s)",
                         ("scan-v1:" + classified.family.value, event["event_id"], txid, revision))
        conn.execute("DELETE FROM protocol_indexer.pending WHERE transaction_id = %s", (txid,))
        return True


def discard_unrelated(conn, limit=5000):
    """Bulk-ack early acceptance wakeups once their bodies are known unrelated."""
    if not 1 <= limit <= 100000:
        raise ValueError("cleanup batch size must be 1..100000")
    with conn.transaction():
        result = conn.execute("""
            DELETE FROM protocol_indexer.pending p WHERE p.transaction_id IN (
                SELECT q.transaction_id FROM protocol_indexer.pending q
                JOIN public.transactions t USING (transaction_id)
                WHERE NOT protocol_indexer.is_candidate(t.payload)
                  AND NOT EXISTS (SELECT 1 FROM protocol_indexer.canonical c
                                  WHERE c.transaction_id = q.transaction_id)
                LIMIT %s FOR UPDATE OF q SKIP LOCKED)
        """, (limit,))
        return result.rowcount


def project(conn, family, row, mode):
    """Common scan/backfill decoder. Revision guards reject stale replay writes."""
    classified = route(bytes.fromhex(row["source_transaction"]["payload"][2:]))
    if classified is None or classified.family != family:
        return False
    conn.execute("""
        INSERT INTO protocol_indexer.records
            (family, transaction_id, revision, root, version, action, tail, source_transaction, accepting_block, mode)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (family, transaction_id) DO UPDATE
            SET revision = EXCLUDED.revision, root = EXCLUDED.root, version = EXCLUDED.version,
                action = EXCLUDED.action, tail = EXCLUDED.tail, source_transaction = EXCLUDED.source_transaction,
                accepting_block = EXCLUDED.accepting_block, mode = EXCLUDED.mode
        WHERE protocol_indexer.records.revision < EXCLUDED.revision
    """, (family.value, row["transaction_id"], row["revision"], classified.root, classified.version,
          classified.action, classified.tail, Jsonb(row["source_transaction"]), row["accepting_block"], mode))
    return True


class ScanAdapter:
    def __init__(self, family: Family):
        self.family = Family(family)
        self.consumer = "scan-v1:" + self.family.value

    def run_one(self, conn):
        with conn.transaction():
            event = conn.execute("""
                SELECT e.* FROM protocol_indexer.delivery_pending p
                JOIN protocol_indexer.events e USING (event_id)
                WHERE p.consumer = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM protocol_indexer.delivery_pending earlier
                      WHERE earlier.consumer = p.consumer
                        AND earlier.transaction_id = p.transaction_id AND earlier.revision < p.revision)
                ORDER BY p.event_id LIMIT 1 FOR UPDATE OF p SKIP LOCKED
            """, (self.consumer,)).fetchone()
            if event is None:
                return None
            project(conn, self.family, event, "scan")
            conn.execute("INSERT INTO protocol_indexer.deliveries VALUES (%s, %s)", (self.consumer, event["event_id"]))
            conn.execute("DELETE FROM protocol_indexer.delivery_pending WHERE consumer = %s AND event_id = %s",
                         (self.consumer, event["event_id"]))
            return event


class BackfillAdapter:
    def __init__(self, family: Family):
        self.family = Family(family)

    def run_page(self, conn, after=b"", limit=500):
        if not 1 <= limit <= 2000:
            raise ValueError("backfill page size must be 1..2000")
        with conn.transaction():
            rows = conn.execute("""
                SELECT * FROM protocol_indexer.canonical WHERE family = %s AND transaction_id > %s
                ORDER BY transaction_id LIMIT %s
            """, (self.family.value, after, limit)).fetchall()
            for row in rows:
                project(conn, self.family, row, "backfill")
            # This cursor bounds an inventory, not live delivery. New/later
            # commits below it are covered by ScanAdapter, with no MAX(id) gap.
            return rows[-1]["transaction_id"] if rows else None


def prune_source(conn, before_ms, limit=10000):
    """Retain canonical protocol bodies while pruning raw chain working tables.

    Simply's own pruner must remain disabled. Protocol acceptance rows remain
    present so a later VCP deletion still emits a revocation even after the raw
    body has expired. Never remove a body waiting for durable capture.
    """
    if not 1 <= limit <= 100000:
        raise ValueError("prune batch size must be 1..100000")
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(736124, 2)")
        ids = [r["transaction_id"] for r in conn.execute("""
            SELECT t.transaction_id FROM public.transactions t
            WHERE t.block_time < %s
              AND NOT EXISTS (SELECT 1 FROM protocol_indexer.pending p WHERE p.transaction_id = t.transaction_id)
              AND (NOT protocol_indexer.is_candidate(t.payload)
                   OR EXISTS (SELECT 1 FROM protocol_indexer.canonical c WHERE c.transaction_id = t.transaction_id))
            ORDER BY t.block_time LIMIT %s FOR UPDATE OF t SKIP LOCKED
        """, (before_ms, limit)).fetchall()]
        if ids:
            conn.execute("""DELETE FROM public.transactions_acceptances a
                WHERE transaction_id = ANY(%s)
                  AND NOT EXISTS (SELECT 1 FROM protocol_indexer.canonical c
                                  WHERE c.transaction_id = a.transaction_id)""", (ids,))
            conn.execute("DELETE FROM public.transactions WHERE transaction_id = ANY(%s)", (ids,))
        return len(ids)
