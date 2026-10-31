"""Durable shared-journal delivery to the existing native Chats consumer."""

from datetime import timedelta
import json
import urllib.error
import urllib.parse
import urllib.request

from .archive import integer, raw_hex, require
from .routing import native_chat, route

CONSUMER = "native-chats-v1"


class EvidencePending(Exception):
    pass


class DeliveryPending(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class NativeTransport:
    def __init__(self, endpoint, secret):
        parsed = urllib.parse.urlsplit(endpoint)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment or len(secret) < 32):
            raise ValueError("configured native endpoint and private bearer secret required")
        self.endpoint, self.secret = endpoint, secret
        self.opener = urllib.request.build_opener(NoRedirect())

    def send(self, batch):
        request = urllib.request.Request(self.endpoint, data=json.dumps(batch, allow_nan=False).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + self.secret}, method="POST")
        try:
            with self.opener.open(request, timeout=15) as response:
                data = response.read(65537)
                if response.status != 200 or len(data) > 65536:
                    raise DeliveryPending("native_response")
                result = json.loads(data)
        except urllib.error.HTTPError as exc:
            exc.close()
            raise DeliveryPending("native_transport") from exc
        except (OSError, ValueError) as exc:
            # Never log a request, secret or unbounded server error body.
            raise DeliveryPending("native_transport") from exc
        if not isinstance(result, dict):
            raise DeliveryPending("native_acknowledgement")
        counts = [result.get(k) for k in ("applied", "duplicates", "stale")]
        if any(type(n) is not int or n < 0 for n in counts) or sum(counts) != len(batch["transactions"]):
            raise DeliveryPending("native_acknowledgement")
        if result["stale"]:
            raise DeliveryPending("native_revision_ahead")
        return result


def native_transaction(row, evidence, accepting_header):
    """Resolve only from a full body/outpoint receipt matching the live journal."""
    body = row["source_transaction"]
    payload = bytes.fromhex(body["payload"][2:])
    require(native_chat(route(payload)), "not supported by current native Chats projection")
    require(payload.hex() == evidence["payload"] and row["transaction_id"].hex() == evidence["transaction_id"],
            "archive evidence differs from journal payload/ID")
    raw = evidence["rpc_transaction"]
    inputs = sorted(body.get("inputs") or [], key=lambda r: r["index"])
    outputs = sorted(body.get("outputs") or [], key=lambda r: r["index"])
    if not inputs or not outputs or "input0" not in evidence:
        raise EvidencePending("input_output_evidence")
    source_input = inputs[0]
    rpc_input = raw["inputs"][0]["previousOutpoint"]
    require(source_input["previous_outpoint_hash"][2:] == rpc_input["transactionId"]
            and source_input["previous_outpoint_index"] == rpc_input["index"], "input outpoint differs from journal")
    if source_input.get("previous_outpoint_script"):
        require(source_input["previous_outpoint_script"][2:] == evidence["input0"]["script"], "resolved input script conflict")
    if source_input.get("previous_outpoint_amount") is not None:
        require(source_input["previous_outpoint_amount"] == evidence["input0"]["amount"], "resolved input amount conflict")
    require(len(outputs) == len(raw["outputs"]), "output count differs from journal")
    sender = evidence["input0"]["address"]
    standard = []
    for source, output in zip(outputs, raw["outputs"]):
        require(source["amount"] == integer(output["amount"])
                and source["script_public_key"][2:] == raw_hex(output["scriptPublicKey"]["scriptPublicKey"]).hex(),
                "output payment evidence differs from journal")
        address = (output.get("verboseData") or {}).get("scriptPublicKeyAddress")
        claimed = source.get("script_public_key_address")
        if claimed and address:
            prefix = "kaspa:" if evidence["network"] == "mainnet" else "kaspatest:"
            source_address = claimed if ":" in claimed else prefix + claimed
            require(source_address == address, "output address differs from the source node")
        elif address:
            raise EvidencePending("source_output_address")
        if address and output["scriptPublicKey"]["version"] == 0:
            standard.append((integer(output["amount"]), address))
    if not standard:
        raise EvidencePending("standard_recipient")
    amount, receiver = next((out for out in standard if out[1] != sender), standard[0])
    block = row["accepting_block"]
    if block is not None:
        if accepting_header is None:
            raise EvidencePending("accepting_header")
        require(accepting_header["verboseData"]["hash"] == block.hex(), "accepting block differs from journal")
        daa = integer(accepting_header["header"]["daaScore"])
    else:
        daa = None
    return {"tx_id": row["transaction_id"].hex(), "payload": payload.hex(), "sender": sender,
            "receiver": receiver, "amount": amount, "block_hash": body["block_hash"][2:],
            "block_time": integer(body["block_time"]), "accepting_block": block.hex() if block is not None else None,
            "accepting_daa_score": daa}


class ChatsConsumer:
    def __init__(self, network, resolve, transport):
        self.network, self.resolve, self.transport = network, resolve, transport

    def run_one(self, conn):
        with conn.transaction():
            config = conn.execute("SELECT network, journal_id::text AS source_id FROM protocol_indexer.config").fetchone()
            if not config or config["network"] != self.network:
                raise ValueError("Chats consumer journal network mismatch")
            job = conn.execute("""SELECT * FROM protocol_indexer.consumer_jobs
                WHERE consumer=%s AND available_at<=clock_timestamp()
                ORDER BY available_at,transaction_id LIMIT 1 FOR UPDATE SKIP LOCKED""", (CONSUMER,)).fetchone()
            if job is None:
                return None
            row = conn.execute("SELECT * FROM protocol_indexer.canonical WHERE transaction_id=%s",
                               (job["transaction_id"],)).fetchone()
            revision = row["revision"]
            try:
                transaction = self.resolve(conn, row)
                batch = {"schema_version": 1, "source_id": config["source_id"], "network": self.network,
                         "transactions": [{"revision": revision, "transaction": transaction}]}
                self.transport.send(batch)
            except (EvidencePending, DeliveryPending) as exc:
                conn.execute("""UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp()+%s,
                    last_error=%s WHERE consumer=%s AND transaction_id=%s""",
                    (timedelta(seconds=5), type(exc).__name__, CONSUMER, job["transaction_id"]))
                return "deferred"
            # Fence only after HTTP, keeping live capture unblocked during I/O.
            # A writer after this lock cannot see the old job disappear: its
            # canonical update/INSERT wakeup happens after this transaction ends.
            latest = conn.execute("SELECT revision FROM protocol_indexer.canonical WHERE transaction_id=%s FOR UPDATE",
                                  (job["transaction_id"],)).fetchone()
            if latest["revision"] == revision:
                conn.execute("DELETE FROM protocol_indexer.consumer_jobs WHERE consumer=%s AND transaction_id=%s",
                             (CONSUMER, job["transaction_id"]))
            else:
                conn.execute("UPDATE protocol_indexer.consumer_jobs SET available_at=clock_timestamp(),last_error=NULL WHERE consumer=%s AND transaction_id=%s",
                             (CONSUMER, job["transaction_id"]))
            return "delivered"
