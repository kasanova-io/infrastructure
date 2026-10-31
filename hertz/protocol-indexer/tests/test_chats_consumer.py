"""Real journal/ack transactions; native RPC is isolated by an explicit test transport."""

from concurrent.futures import ThreadPoolExecutor
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from protocol_indexer.chats import ChatsConsumer, DeliveryPending, EvidencePending, NativeTransport
from protocol_indexer.journal import capture_one, connect
from test_postgres import PostgresHarness, DSN


class Transport:
    def __init__(self, callback=None):
        self.batches = []
        self.callback = callback

    def send(self, batch):
        self.batches.append(batch)
        if self.callback:
            self.callback(batch)
        return {"applied": 1, "duplicates": 0, "stale": 0}


def resolved(conn, row):
    # Explicit synthetic unit fixture: end-to-end native/API evidence is separate.
    return {"tx_id": row["transaction_id"].hex(), "payload": row["source_transaction"]["payload"][2:],
            "accepting_block": row["accepting_block"].hex() if row["accepting_block"] else None}


class ConsumerTests(PostgresHarness):
    def test_ack_binds_network_journal_id_and_latest_revision(self):
        txid = self.ingest(b"ciph_msg:1:handshake:sealed")
        transport = Transport()
        consumer = ChatsConsumer("testnet-10", resolved, transport)
        self.assertEqual(consumer.run_one(self.db), "delivered")
        batch = transport.batches[0]
        self.assertEqual(batch["source_id"], str(self.scalar("SELECT journal_id FROM protocol_indexer.config")))
        self.assertEqual(batch["network"], "testnet-10")
        self.assertEqual(batch["transactions"][0]["transaction"]["tx_id"], txid.hex())
        self.assertEqual(batch["transactions"][0]["revision"], 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.consumer_jobs"), 0)

    def test_missing_evidence_does_not_ack_or_delay_other_transactions(self):
        first = self.ingest(b"kchat:1:handshake:missing")
        second = self.ingest(b"ciph_msg:1:handshake:ready")
        def resolve(conn, row):
            if row["transaction_id"] == first:
                raise EvidencePending("sender")
            return resolved(conn, row)
        transport = Transport()
        consumer = ChatsConsumer("testnet-10", resolve, transport)
        self.assertEqual(consumer.run_one(self.db), "deferred")
        self.assertEqual(consumer.run_one(self.db), "delivered")
        self.assertEqual(transport.batches[0]["transactions"][0]["transaction"]["tx_id"], second.hex())
        self.assertEqual(self.scalar("SELECT transaction_id FROM protocol_indexer.consumer_jobs"), first)

    def test_failed_native_ack_keeps_durable_job(self):
        self.ingest(b"kchat:1:handshake:sealed")
        def fail(_):
            raise DeliveryPending("native unavailable")
        consumer = ChatsConsumer("testnet-10", resolved, Transport(fail))
        self.assertEqual(consumer.run_one(self.db), "deferred")
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.consumer_jobs"), 1)

    def test_revocation_committed_during_delivery_is_not_lost_or_blocked(self):
        txid = self.ingest(b"kchat:1:handshake:sealed")
        def revoke(_):
            def write():
                with connect(DSN) as writer:
                    writer.execute("DELETE FROM transactions_acceptances WHERE transaction_id=%s", (txid,))
                    self.assertTrue(capture_one(writer))
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(write).result(timeout=3)
        transport = Transport(revoke)
        consumer = ChatsConsumer("testnet-10", resolved, transport)
        self.assertEqual(consumer.run_one(self.db), "delivered")
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.consumer_jobs"), 1)
        transport.callback = None
        self.assertEqual(consumer.run_one(self.db), "delivered")
        last = transport.batches[-1]["transactions"][0]
        self.assertEqual(last["revision"], 2)
        self.assertIsNone(last["transaction"]["accepting_block"])
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.consumer_jobs"), 0)

    def test_wrong_network_cannot_deliver_or_ack(self):
        self.ingest(b"ciph_msg:1:handshake:sealed")
        transport = Transport()
        with self.assertRaises(ValueError):
            ChatsConsumer("testnet-11", resolved, transport).run_one(self.db)
        self.assertEqual(transport.batches, [])
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.consumer_jobs"), 1)

    def test_public_posts_rooms_and_dm_never_fall_through_to_old_native_projection(self):
        for wire in (b"kchat:1:post:key:sig:body", b"kchat:1:bcast:room:body", b"kchat:1:dm:tag:alias:sealed", b"k:1:post:key:sig:body"):
            self.ingest(wire)
        self.assertEqual(self.scalar("SELECT count(*) FROM protocol_indexer.consumer_jobs"), 0)


class TransportTests(unittest.TestCase):
    def test_http_failure_malformed_ack_and_redirect_never_count_as_delivery(self):
        responses = [(200, {}), (200, {"applied": True, "duplicates": 0, "stale": 0}),
                     (200, {"applied": 0, "duplicates": 0, "stale": 1}), (503, {}), (302, {})]
        received = []
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                received.append(self.path)
                status, body = responses.pop(0)
                self.send_response(status)
                if status == 302:
                    self.send_header("Location", "/unexpected-forward")
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())
            def do_GET(self):
                received.append("redirected")
                self.send_response(200); self.end_headers()
            def log_message(self, *_):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            transport = NativeTransport(f"http://127.0.0.1:{server.server_port}/history-import", "fixture-" * 8)
            for _ in range(len(responses)):
                with self.assertRaises(DeliveryPending):
                    transport.send({"transactions": [{}]})
            self.assertNotIn("redirected", received)
        finally:
            server.shutdown(); server.server_close(); thread.join()


if __name__ == "__main__":
    unittest.main()
