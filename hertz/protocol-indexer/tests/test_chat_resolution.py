"""Data-source binding, ordinary/binary protocol bytes and revocation semantics."""
import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from protocol_indexer.archive import checked_receipt, input0_evidence, verify_archive, verify_transaction_evidence
from protocol_indexer.chats import EvidencePending, native_transaction
from test_archive import evidence, INFO, TX, ACCEPT, BLOCK, PREVIOUS


def fixture(payload=b"kchat:1:handshake:\xff\x00:binary"):
    values = evidence(payload)
    receipt = verify_archive(TX, "testnet-10", INFO, *values)
    receipt["input0"] = {"address": "kaspatest:sender", "script": "11ac", "amount": 20000000}
    row = {"transaction_id": bytes.fromhex(TX), "revision": 1, "accepting_block": bytes.fromhex(ACCEPT),
           "source_transaction": {"payload": "\\x" + payload.hex(), "block_hash": "\\x" + BLOCK, "block_time": 12345,
               "inputs": [{"index": 0, "previous_outpoint_hash": "\\x" + PREVIOUS, "previous_outpoint_index": 2,
                           "previous_outpoint_script": "\\x11ac", "previous_outpoint_amount": 20000000}],
               "outputs": [{"index": 0, "amount": 10000000, "script_public_key": "\\x2021ac",
                            "script_public_key_address": "kaspatest:author"}]}}
    return row, receipt, values[1]


class ResolutionTests(unittest.TestCase):
    def resolver(self):
        path = Path(__file__).parents[1] / "chats_worker.py"
        spec = importlib.util.spec_from_file_location("chat_worker_test", path)
        worker = importlib.util.module_from_spec(spec); spec.loader.exec_module(worker)
        root = self.enterContext(tempfile.TemporaryDirectory())
        resolver = worker.Resolver(root, "https://archive", "https://archive", "testnet-10")
        self.addCleanup(resolver.db.close)
        resolver.network_info = INFO
        return worker, resolver

    def test_lagging_archive_detail_and_partial_block_are_retried_without_manual_cache_clear(self):
        path = Path(__file__).parents[1] / "chats_worker.py"
        spec = importlib.util.spec_from_file_location("chat_worker_test", path)
        worker = importlib.util.module_from_spec(spec); spec.loader.exec_module(worker)
        with tempfile.TemporaryDirectory() as root:
            resolver = worker.Resolver(root, "https://archive", "https://archive", "testnet-10")
            self.addCleanup(resolver.db.close)
            resolver.network_info = INFO
            detail, header, block = evidence()
            pending = copy.deepcopy(detail); pending["accepting_block_hash"] = None
            with patch.object(resolver, "request", return_value=pending):
                with self.assertRaises(EvidencePending): resolver.collect_body(TX)
            partial = copy.deepcopy(block); partial["transactions"] = []
            with patch.object(resolver, "request", side_effect=[detail, header, partial]):
                with self.assertRaises(EvidencePending): resolver.collect_body(TX)
            self.assertFalse((Path(root)/"verified"/(TX+".json.gz")).exists())
            with patch.object(resolver, "request", side_effect=[detail, block]):
                result = resolver.collect_body(TX)
            self.assertEqual(result["transaction_id"], TX)
            with patch.object(resolver, "request", side_effect=AssertionError("immutable receipt already verified")):
                self.assertEqual(resolver.collect_body(TX), result)

    def test_malformed_public_detail_and_block_retry_without_poisoning_cache(self):
        worker, resolver = self.resolver()
        detail, header, block = evidence()
        for bad in ([], {"accepting_block_hash": ACCEPT, "block_hash": [None]}):
            with patch.object(resolver, "request", return_value=bad):
                with self.assertRaises(EvidencePending): resolver.collect_body(TX)
        partial = copy.deepcopy(block); partial["transactions"] = None
        with patch.object(resolver, "request", side_effect=[detail, header, partial]):
            with self.assertRaises(EvidencePending): resolver.collect_body(TX)
        with patch.object(resolver, "request", side_effect=[detail, block]):
            self.assertEqual(resolver.collect_body(TX)["transaction_id"], TX)

    def test_incomplete_and_malformed_cached_header_are_refetched(self):
        worker, resolver = self.resolver()
        _, header, _ = evidence()
        saved = resolver.root / "blocks" / (ACCEPT + ".json.gz")
        bad = copy.deepcopy(header); del bad["header"]["timestamp"]
        with patch.object(resolver, "request", return_value=bad):
            with self.assertRaises(EvidencePending): resolver.verified_header(ACCEPT)
        self.assertFalse(saved.exists())
        for corrupt in (bad, [], {"header": None}):
            worker.history.atomic_json(saved, corrupt)
            with patch.object(resolver, "request", return_value=header) as request:
                self.assertEqual(resolver.verified_header(ACCEPT), header)
                request.assert_called_once()
        self.assertEqual(len(list((resolver.root / "quarantine").glob("*.gz"))), 3)

    def test_malformed_cached_receipt_is_retained_and_recovered(self):
        worker, resolver = self.resolver()
        saved = resolver.root / "verified" / (TX + ".json.gz")
        for corrupt in ({"transaction_id": TX, "network": "testnet-10"}, []):
            worker.history.atomic_json(saved, corrupt)
            detail, header, block = evidence()
            responses = [detail, block] if (resolver.root / "blocks" / (ACCEPT + ".json.gz")).exists() else [detail, header, block]
            with patch.object(resolver, "request", side_effect=responses):
                self.assertEqual(resolver.collect_body(TX)["transaction_id"], TX)
        self.assertEqual(len(list((resolver.root / "quarantine").glob("*.gz"))), 2)

    def test_low_capacity_pauses_before_fetch_or_cache_write(self):
        worker, resolver = self.resolver()
        from collections import namedtuple
        Usage = namedtuple("Usage", "total used free")
        with patch.object(worker.history.shutil, "disk_usage", return_value=Usage(100, 99, 1)), \
             patch.object(resolver, "request", side_effect=AssertionError("must pause before fetch")):
            for operation in (lambda: resolver.collect_body(TX), lambda: resolver.verified_header(ACCEPT),
                              lambda: resolver.save(resolver.root / "quarantine" / "bad.json.gz", {})):
                with self.assertRaises(worker.history.CapacityPause): operation()
        self.assertFalse(any((resolver.root / "quarantine").iterdir()))

    def test_only_known_http_exhaustion_is_deferred(self):
        worker, resolver = self.resolver()
        with patch.object(worker.history.History, "request", side_effect=RuntimeError("request retries exhausted")):
            with self.assertRaises(EvidencePending): resolver.request("https://archive/transactions/" + TX)
        for failure in (RuntimeError("unrelated failure"), worker.history.CapacityPause("full")):
            with patch.object(worker.history.History, "request", side_effect=failure):
                with self.assertRaises(type(failure)) as raised: resolver.request("https://archive/info/network")
                self.assertIs(raised.exception, failure)

    def test_exact_binary_payload_and_native_receiver_selection_are_preserved(self):
        row, receipt, header = fixture()
        result = native_transaction(row, receipt, header)
        self.assertEqual(bytes.fromhex(result["payload"]), b"kchat:1:handshake:\xff\x00:binary")
        self.assertEqual((result["sender"], result["receiver"], result["amount"]),
                         ("kaspatest:sender", "kaspatest:author", 10000000))
        row["source_transaction"]["outputs"][0]["script_public_key_address"] = "author"
        self.assertEqual(native_transaction(row, receipt, header), result)

    def test_archive_acceptance_cannot_override_current_revocation(self):
        row, receipt, header = fixture()
        row["accepting_block"] = None
        result = native_transaction(row, receipt, None)
        self.assertIsNone(result["accepting_block"])
        self.assertIsNone(result["accepting_daa_score"])

    def test_source_and_receipt_conflicts_are_never_delivered(self):
        for field in ("payload", "input", "input_script", "amount", "script", "address", "acceptance"):
            row, receipt, header = fixture()
            body = row["source_transaction"]
            if field == "payload": body["payload"] += "ff"
            elif field == "input": body["inputs"][0]["previous_outpoint_index"] += 1
            elif field == "input_script": body["inputs"][0]["previous_outpoint_script"] = "\\xff"
            elif field == "amount": body["outputs"][0]["amount"] += 1
            elif field == "script": body["outputs"][0]["script_public_key"] = "\\xff"
            elif field == "address": body["outputs"][0]["script_public_key_address"] = "kaspatest:other"
            else: header["verboseData"]["hash"] = "ff" * 32
            with self.subTest(field=field), self.assertRaises(ValueError):
                native_transaction(row, receipt, header)

    def test_missing_evidence_remains_pending(self):
        row, receipt, header = fixture()
        del receipt["input0"]
        with self.assertRaises(EvidencePending): native_transaction(row, receipt, header)
        row, receipt, header = fixture()
        with self.assertRaises(EvidencePending): native_transaction(row, receipt, None)

    def test_parent_proof_resolves_sender_from_exact_previous_output(self):
        target_values = evidence()
        target_values[0]["inputs"][0]["previous_outpoint_index"] = 0
        target_values[2]["transactions"][0]["inputs"][0]["previousOutpoint"]["index"] = 0
        target = verify_archive(TX, "testnet-10", INFO, *target_values)
        previous_values = evidence(b"")
        detail, _, block = previous_values
        detail["transaction_id"] = PREVIOUS
        detail["inputs"] = []
        detail["outputs"][0]["script_public_key_address"] = "kaspatest:sender"
        rpc = block["transactions"][0]
        rpc["verboseData"]["transactionId"] = PREVIOUS
        rpc["inputs"] = []
        rpc["outputs"][0]["verboseData"]["scriptPublicKeyAddress"] = "kaspatest:sender"
        parent = verify_transaction_evidence(PREVIOUS, "testnet-10", INFO, *previous_values)
        self.assertEqual(input0_evidence(target, parent)["address"], "kaspatest:sender")
        for payload in (None, "missing"):
            values = copy.deepcopy(previous_values)
            if payload is None:
                values[0]["payload"] = values[2]["transactions"][0]["payload"] = None
            else:
                del values[0]["payload"]; del values[2]["transactions"][0]["payload"]
            ordinary = verify_transaction_evidence(PREVIOUS, "testnet-10", INFO, *values)
            self.assertEqual(ordinary["payload"], "")
            self.assertEqual(checked_receipt(ordinary, PREVIOUS, "testnet-10")["payload"], "")
        corrupt = copy.deepcopy(target)
        corrupt["archive_transaction"]["inputs"][0]["previous_outpoint_address"] = "kaspatest:liar"
        with self.assertRaises(ValueError): input0_evidence(corrupt, parent)
        parent["rpc_transaction"]["outputs"][0]["scriptPublicKey"]["version"] = 1
        with self.assertRaises(ValueError): input0_evidence(target, parent)

    def test_basic_sql_receipt_is_enriched_before_delivery(self):
        _, resolver = self.resolver()
        row, basic, header = fixture()
        row["ever_accepted"] = True
        del basic["input0"]
        row["source_transaction"]["inputs"][0]["previous_outpoint_index"] = 0
        basic["archive_transaction"]["inputs"][0]["previous_outpoint_index"] = 0
        basic["rpc_transaction"]["inputs"][0]["previousOutpoint"]["index"] = 0
        values = evidence(b"")
        values[0]["transaction_id"] = PREVIOUS
        values[0]["inputs"] = []
        values[0]["outputs"][0]["script_public_key_address"] = "kaspatest:sender"
        values[0]["outputs"][0]["amount"] = 20000000
        values[0]["outputs"][0]["script_public_key"] = "11ac"
        rpc = values[2]["transactions"][0]
        rpc["verboseData"]["transactionId"] = PREVIOUS
        rpc["inputs"] = []
        rpc["outputs"][0]["verboseData"]["scriptPublicKeyAddress"] = "kaspatest:sender"
        rpc["outputs"][0]["amount"] = 20000000
        rpc["outputs"][0]["scriptPublicKey"]["scriptPublicKey"] = "11ac"
        parent = verify_transaction_evidence(PREVIOUS, "testnet-10", INFO, *values)
        class Connection:
            def execute(self, sql, params):
                self.params = params
                return self
            def fetchone(self): return {"receipt": basic}
        conn = Connection()
        with patch.object(resolver, "collect_body", return_value=parent) as fetch, \
             patch.object(resolver, "verified_header", return_value=header):
            result = resolver.resolve(conn, row)
        fetch.assert_called_once_with(PREVIOUS)
        self.assertEqual(result["sender"], "kaspatest:sender")
        self.assertIn("input0", conn.params[1].obj)


if __name__ == "__main__": unittest.main()
