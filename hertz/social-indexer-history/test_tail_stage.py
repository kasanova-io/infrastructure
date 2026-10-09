import hashlib
import json
import unittest
from unittest.mock import Mock, patch
from stage import Stage, validate_records


class TailStageTest(unittest.TestCase):
    def setUp(self):
        raw = "k:1:follow:02" + "1" * 64 + ":dd:unfollow:03" + "2" * 64
        self.row = {
            "transaction_id": "a" * 64,
            "network": "testnet-10",
            "kind": "follow",
            "payload": raw.encode().hex(),
            "payload_sha256": hashlib.sha256(raw.encode()).hexdigest(),
            "block_time": 200,
            "accepting_block": "b" * 64,
            "containing_block": "c" * 64,
            "sender_pubkey": "02" + "1" * 64,
            "recovery_scope": "captured-live-event-tail",
            "live_timestamp_provenance": {
                "policy": "preserve-exact-live-time-after-block-membership-verification",
                "observed_block_time": 200,
                "matched_containing_block": "c" * 64,
            },
        }
        self.stage = Stage.__new__(Stage)
        self.encoded = json.dumps(self.row).encode().hex()

    def test_tail_scope_and_timestamp_proof_are_explicit(self):
        data = json.dumps(self.row).encode()
        with self.assertRaises(ValueError):
            validate_records(data, "testnet-10", True, True)
        self.assertEqual(
            len(validate_records(data, "testnet-10", False, False, True)), 1
        )
        self.row["live_timestamp_provenance"]["observed_block_time"] = 199
        with self.assertRaises(ValueError):
            validate_records(
                json.dumps(self.row).encode(), "testnet-10", False, False, True
            )

    def test_already_absent_undo_is_never_signature_proof(self):
        self.stage.sql = Mock(side_effect=["", ""])
        with self.assertRaisesRegex(ValueError, "No-op undo"):
            self.stage.prepare_undo(self.row, self.encoded)
        self.assertEqual(self.stage.sql.call_count, 2)

    def test_positive_predecessor_is_persisted_before_submission(self):
        predecessor = {"transaction_id": "d" * 64, "block_time": 100}
        self.stage.sql = Mock(side_effect=[json.dumps(predecessor), "", ""])
        query = self.stage.prepare_undo(self.row, self.encoded)
        self.assertIn("k_follows", query)
        self.assertIn(
            "INSERT INTO history_pending_undo",
            self.stage.sql.call_args_list[-1].args[0],
        )
        self.assertNotIn(
            "INSERT INTO transactions", self.stage.sql.call_args_list[-1].args[0]
        )

    def test_crash_resume_absence_requires_exact_durable_attempt(self):
        pending = json.dumps(
            {
                "matches": True,
                "predecessor": {"transaction_id": "d" * 64, "block_time": 100},
            }
        )
        self.stage.sql = Mock(side_effect=["", pending, "f"])
        with self.assertRaisesRegex(ValueError, "without original submitted input"):
            self.stage.prepare_undo(self.row, self.encoded)
        self.stage.sql = Mock(side_effect=["", pending, "t"])
        self.assertIn("k_follows", self.stage.prepare_undo(self.row, self.encoded))

    def test_mismatched_attempt_or_changed_predecessor_is_rejected(self):
        pending = json.dumps({"matches": False, "predecessor": {}})
        self.stage.sql = Mock(side_effect=["", pending])
        with self.assertRaisesRegex(ValueError, "Conflicting pending"):
            self.stage.prepare_undo(self.row, self.encoded)

    def test_private_single_writer_boundary_rejects_unsafe_runtime(self):
        import copy

        project = "social_history_undo_test"
        network = project + "_history"
        self.stage.command = ["docker", "compose", "--project-name", project]
        containers = [
            {
                "Id": name,
                "State": {"Running": True},
                "Config": {
                    "Labels": {
                        "com.docker.compose.project": project,
                        "com.docker.compose.service": name,
                    },
                    "Cmd": ["--workers", "1"],
                },
                "NetworkSettings": {"Networks": {network: {}}, "Ports": {}},
            }
            for name in ["database", "processor", "web"]
        ]
        network_data = {
            "Internal": True,
            "Containers": {c["Id"]: {} for c in containers},
        }
        self.stage.sql = Mock(return_value="")

        def check(items, topology):
            with patch(
                "stage.subprocess.check_output",
                side_effect=[
                    "database\nprocessor\nweb\n",
                    json.dumps(items),
                    json.dumps([topology]),
                ],
            ):
                self.stage.verify_tail_isolation([self.row])

        check(containers, network_data)
        for kind in ["workers", "port", "external", "extra", "raw"]:
            changed = copy.deepcopy(containers)
            topology = copy.deepcopy(network_data)
            self.stage.sql.return_value = ""
            if kind == "workers":
                changed[1]["Config"]["Cmd"][-1] = "2"
            if kind == "port":
                changed[0]["NetworkSettings"]["Ports"]["5432/tcp"] = [
                    {"HostPort": "5432"}
                ]
            if kind == "external":
                topology["Internal"] = False
            if kind == "extra":
                topology["Containers"]["other-writer"] = {}
            if kind == "raw":
                self.stage.sql.return_value = "f" * 64
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                check(changed, topology)
