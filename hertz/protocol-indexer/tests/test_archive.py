"""Archive/payment-field integrity, binary routing and resumable collector checks."""

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from protocol_indexer.archive import check_network, verify_archive
from protocol_indexer.routing import Family

TX, ACCEPT, BLOCK, PREVIOUS = (c * 64 for c in "abcd")
INFO = {"networkName": "kaspa-testnet-10"}


def evidence(payload=b"kchat:1:tip:key:sig:post:author:cipher"):
    detail = {
        "transaction_id": TX, "is_accepted": True, "payload": payload.hex(),
        "accepting_block_hash": ACCEPT, "block_hash": [BLOCK],
        "inputs": [{"index": 0, "previous_outpoint_hash": PREVIOUS, "previous_outpoint_index": 2,
                    "signature_script": "aabb", "previous_outpoint_address": "kaspatest:sender"}],
        "outputs": [{"index": 0, "amount": 10000000, "script_public_key": "2021ac",
                     "script_public_key_address": "kaspatest:author"}],
    }
    rpc = {"verboseData": {"transactionId": TX}, "payload": payload.hex(),
           "inputs": [{"previousOutpoint": {"transactionId": PREVIOUS, "index": 2}, "signatureScript": "aabb"}],
           "outputs": [{"amount": 10000000, "scriptPublicKey": {"version": 0, "scriptPublicKey": "2021ac"},
                        "verboseData": {"scriptPublicKeyAddress": "kaspatest:author"}}]}
    accepting = {"verboseData": {"hash": ACCEPT}, "header": {"daaScore": "42", "timestamp": "12346"}}
    containing = {"verboseData": {"hash": BLOCK}, "header": {"timestamp": "12345"}, "transactions": [rpc]}
    return detail, accepting, containing


class ArchiveTests(unittest.TestCase):
    def verify(self, values):
        return verify_archive(TX, "testnet-10", INFO, *values)

    def test_all_six_families_preserve_binary_and_complete_versioned_payment_evidence(self):
        cases = {
            Family.KACHAT_POSTS: b"kchat:1:post:key:sig:aGk=:[]",
            Family.KACHAT_MESSAGES: b"kchat:1:handshake:\xff\x00:binary\xfe",
            Family.KACHAT_ACTIONS: b"kchat:1:tip:key:sig:post:author:cipher",
            Family.KASIA_MESSAGES: b"ciph_msg:1:comm:alias:\xff\x00:binary",
            Family.KSOCIAL_POSTS: b"k:1:reply:key:sig:post:aGk=:[]",
            Family.KSOCIAL_ACTIONS: b"k:1:vote:key:sig:up:post",
        }
        for family, payload in cases.items():
            with self.subTest(family=family):
                detail, accepting, containing = evidence(payload)
                receipt = self.verify((detail, accepting, containing))
                self.assertEqual(receipt["family"], family.value)
                self.assertEqual(bytes.fromhex(receipt["payload"]), payload)
                self.assertEqual(receipt["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"], 0)
                self.assertEqual(receipt["archive_transaction"], detail)
                self.assertFalse(receipt["notify_allowed"])
                self.assertIn("reconciliation pending", receipt["verification"])

    def test_network_is_explicit_even_when_address_prefix_is_the_same(self):
        for network, info in [("testnet-11", INFO), ("mainnet", INFO), ("testnet", INFO), ("testnet-10", {})]:
            with self.subTest(network=network), self.assertRaises(ValueError):
                check_network(network, info)
        check_network("mainnet", {"networkName": "kaspa-mainnet"})

    def test_missing_or_tampered_payment_outputs_never_become_verified_archive_evidence(self):
        for change in ("amount", "script", "version", "address", "index", "missing"):
            values = evidence()
            detail, _, full = values
            rpc = full["transactions"][0]["outputs"][0]
            if change == "amount":
                rpc["amount"] += 1
            elif change == "script":
                rpc["scriptPublicKey"]["scriptPublicKey"] = "deadbeef"
            elif change == "version":
                del rpc["scriptPublicKey"]["version"]
            elif change == "address":
                rpc["verboseData"]["scriptPublicKeyAddress"] = "kaspatest:other"
            elif change == "index":
                detail["outputs"][0]["index"] = 1
            else:
                full["transactions"][0]["outputs"] = []
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.verify(values)

    def test_wrong_payload_id_acceptance_or_block_inclusion_is_rejected(self):
        for change in ("payload", "id", "accepted", "block", "accepting", "duplicate"):
            values = evidence()
            detail, accepting, full = values
            if change == "payload":
                full["transactions"][0]["payload"] += "ff"
            elif change == "id":
                detail["transaction_id"] = "f" * 64
            elif change == "accepted":
                detail["is_accepted"] = False
            elif change == "block":
                full["verboseData"]["hash"] = "f" * 64
            elif change == "accepting":
                accepting["verboseData"]["hash"] = "f" * 64
            else:
                full["transactions"].append(copy.deepcopy(full["transactions"][0]))
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.verify(values)

    def test_input_outpoint_and_signature_script_cannot_be_swapped(self):
        for field in ("transactionId", "index", "signatureScript"):
            values = evidence()
            inp = values[2]["transactions"][0]["inputs"][0]
            if field == "signatureScript":
                inp[field] = "00"
            else:
                inp["previousOutpoint"][field] = "f" * 64 if field == "transactionId" else 1
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.verify(values)

    def test_unknown_protocol_cannot_fall_through_to_generic_chat(self):
        for payload in (b"kchat:2:post:a:b:c", b"kchat:1:names:a:b", b"k:1:tip:a:b:c"):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.verify(evidence(payload))

    def test_existing_inventory_tools_resume_and_export_six_family_receipts_without_live_import(self):
        path = Path(__file__).parents[1] / "archive_backfill.py"
        spec = importlib.util.spec_from_file_location("archive_backfill", path)
        tool = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tool)
        with tempfile.TemporaryDirectory() as tmp:
            h = tool.ArchiveHistory(tmp, "https://source", "https://archive", "testnet-10")
            h.network_info = INFO
            h.cache = lambda category, key, url: ({"archive": values[0], "blocks": values[1] if key == ACCEPT else values[2]})[category]
            values = evidence(b"ciph_msg:1:handshake:\xff\x00:body")
            h.db.execute("INSERT INTO records(txid) VALUES (?)", (TX,))
            h.db.commit()
            h.finish_verification(TX, lambda: h.verify_one(TX))
            self.assertEqual(h.db.execute("SELECT status,kind FROM records").fetchone(), ("verified", "kasia_messages"))
            out = Path(tmp) / "batch.jsonl"
            self.assertEqual(h.export(out), 1)
            row = json.loads(out.read_text())
            self.assertEqual(row["payload"], values[0]["payload"])
            self.assertFalse(row["notify_allowed"])
            with self.assertRaises(FileExistsError):
                h.export(out)
            h.db.close()
            resumed = tool.ArchiveHistory(tmp, "https://source", "https://archive", "testnet-10")
            self.assertEqual(resumed.db.execute("SELECT count(*) FROM records WHERE status='verified'").fetchone()[0], 1)
            resumed.db.close()
            with self.assertRaises(ValueError):
                tool.ArchiveHistory(tmp, "https://source", "https://archive", "testnet-11")


if __name__ == "__main__":
    unittest.main()
