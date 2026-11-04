"""Storage-copy and public-writer recovery regressions; no Docker required."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("cold_snapshot", Path(__file__).with_name("cold_snapshot.py"))
cold = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cold)


def helper_fixture():
    container = {"Id": "f" * 64, "Name": "/kasanova-chats-cold-test",
                 "Image": "sha256:helper", "Config": {"Labels": {"io.kasanova.run": "run"}},
                 "State": {"Running": True, "ExitCode": 0},
                 "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True},
                 "Mounts": [{"Name": "source", "Type": "volume", "Destination": "/from", "RW": False},
                            {"Name": "backup", "Type": "volume", "Destination": "/to", "RW": True}]}
    identity = {"id": container["Id"], "image": container["Image"], "run_id": "run",
                "source_volume": "source", "backup_volume": "backup",
                "configuration_sha256": cold.configuration(container)}
    return container, identity


class CopyTest(unittest.TestCase):
    def run_copy(self, source, target, before_verify=""):
        code = cold.COPY.replace("source=Path('/from');target=Path('/to')",
                                 f"source=Path({str(source)!r});target=Path({str(target)!r})")
        if before_verify:
            code = code.replace("after=inventory(source)", before_verify + "\nafter=inventory(source)")
        return subprocess.run([sys.executable, "-B", "-c", code], capture_output=True)

    def test_real_nested_files_and_ownership_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = [Path(directory) / name for name in ("source", "target")]
            (source / "partition").mkdir(parents=True)
            target.mkdir()
            (source / "partition" / "segment").write_bytes(bytes(range(256)) * 100)
            (source / "journal").write_bytes(b"durable registry and messages\x00")
            os.chmod(source, 0o750)
            os.chmod(source / "journal", 0o640)
            result = self.run_copy(source, target)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            data = json.loads(result.stdout)
            self.assertEqual(data["entries"], 4)
            self.assertEqual((target / "partition" / "segment").read_bytes(),
                             (source / "partition" / "segment").read_bytes())
            self.assertEqual((target / "journal").stat().st_mode & 0o777, 0o640)
            self.assertEqual(target.stat().st_mode & 0o777, 0o750)
            self.assertEqual(target.stat().st_uid, source.stat().st_uid)

    def test_special_source_path_refuses_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = [Path(directory) / name for name in ("source", "target")]
            source.mkdir(); target.mkdir()
            (source / "escape").symlink_to("/etc/passwd")
            result = self.run_copy(source, target)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(target.iterdir()), [])

    def test_nonempty_target_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = [Path(directory) / name for name in ("source", "target")]
            source.mkdir(); target.mkdir()
            (source / "data").write_bytes(b"new")
            (target / "data").write_bytes(b"foreign")
            self.assertNotEqual(self.run_copy(source, target).returncode, 0)
            self.assertEqual((target / "data").read_bytes(), b"foreign")

    def test_source_change_during_copy_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = [Path(directory) / name for name in ("source", "target")]
            source.mkdir(); target.mkdir()
            (source / "data").write_bytes(b"original")
            result = self.run_copy(source, target, "(source/'data').write_bytes(b'changed')")
            self.assertNotEqual(result.returncode, 0)


class RecoveryTest(unittest.TestCase):
    def test_first_start_oom_default_drift_preserves_helper_binding(self):
        reader, identity = helper_fixture()
        reader["HostConfig"]["OomKillDisable"] = False
        identity["configuration_sha256"] = cold.configuration(reader)
        reader["HostConfig"]["OomKillDisable"] = None
        cold.bind_helper(reader, identity)
        self.assertIsNone(reader["HostConfig"]["OomKillDisable"])

        reader["HostConfig"]["OomKillDisable"] = True
        with self.assertRaises(ValueError):
            cold.bind_helper(reader, identity)

    def test_equivalent_mount_order_does_not_change_configuration_binding(self):
        value = {"Id": "original", "Name": "/original", "Image": "sha256:original",
                 "Config": {}, "HostConfig": {}, "Mounts": [
                     {"Name": "data", "Destination": "/app/data", "RW": True},
                     {"Source": "/private/key", "Destination": "/key", "RW": False}]}
        reordered = copy.deepcopy(value)
        reordered["Mounts"].reverse()
        self.assertEqual(cold.configuration(value), cold.configuration(reordered))
        reordered["Mounts"][0]["RW"] = True
        self.assertNotEqual(cold.configuration(value), cold.configuration(reordered))

    def test_a_healthy_old_process_is_not_a_successful_restart(self):
        old = {"State": {"Running": True, "StartedAt": "before", "Health": {"Status": "healthy"}}}
        new = copy.deepcopy(old)
        new["State"]["StartedAt"] = "after"
        after = {key: 2 for key in (*cold.COUNTERS, "blocks_processed")}
        with patch.object(cold, "bind"), patch.object(cold, "writers", return_value=["original"]), \
                patch.object(cold, "inspect", side_effect=[old, new, new]), \
                patch.object(cold, "metrics", side_effect=[dict(after, blocks_processed=1), after]) as measured, \
                patch.object(cold.time, "sleep"):
            result = cold.recover({"id": "original", "volume": "source"},
                                  {key: 1 for key in after}, "before", 10)
        self.assertEqual(result, after)
        self.assertEqual(measured.call_count, 2)

    def test_process_counter_reset_is_accepted_only_after_same_new_process_advances(self):
        new = {"State": {"Running": True, "StartedAt": "new", "Health": {"Status": "healthy"}}}
        baseline = {key: 12 for key in cold.COUNTERS}; baseline["blocks_processed"] = 11036077
        first = dict(baseline, blocks_processed=3002200)
        second = dict(baseline, blocks_processed=3003314)
        with patch.object(cold, "bind"), patch.object(cold, "inspect", return_value=new), \
                patch.object(cold, "writers", return_value=["original"]), \
                patch.object(cold, "metrics", side_effect=[first, second]), \
                patch.object(cold.time, "sleep"), patch.object(cold, "command") as command:
            self.assertEqual(cold.recover({"id": "original", "volume": "source"}, baseline,
                                         "old", 10, start_if_stopped=False), second)
        command.assert_not_called()

    def test_stalled_new_process_never_passes_even_if_counter_exceeds_old(self):
        new = {"State": {"Running": True, "StartedAt": "new", "Health": {"Status": "healthy"}}}
        baseline = {key: 1 for key in (*cold.COUNTERS, "blocks_processed")}
        with patch.object(cold, "bind"), patch.object(cold, "inspect", return_value=new), \
                patch.object(cold, "writers", return_value=["original"]), \
                patch.object(cold, "metrics", return_value=dict(baseline, blocks_processed=99)), \
                patch.object(cold.time, "monotonic", side_effect=[0, 0, 1, 11]), patch.object(cold.time, "sleep"):
            with self.assertRaises(TimeoutError):
                cold.recover({"id": "original", "volume": "source"}, baseline, "old", 10)

    def test_approximate_partition_estimates_may_decrease_after_restart(self):
        new = {"State": {"Running": True, "StartedAt": "new", "Health": {"Status": "healthy"}}}
        baseline = {key: 126 for key in cold.COUNTERS}
        first = {key: 124 for key in cold.COUNTERS}
        first.update(contextual_messages=126, blocks_processed=1)
        second = dict(first, blocks_processed=2)
        with patch.object(cold, "bind"), patch.object(cold, "inspect", return_value=new), \
                patch.object(cold, "writers", return_value=["original"]), \
                patch.object(cold, "metrics", side_effect=[first, second]), \
                patch.object(cold.time, "sleep"), patch.object(cold, "command") as command:
            self.assertEqual(cold.recover({"id": "original", "volume": "source"}, baseline,
                                         "old", 10, start_if_stopped=False), second)
        command.assert_not_called()

    def test_exact_content_count_loss_still_rejects_recovery(self):
        new = {"State": {"Running": True, "StartedAt": "new", "Health": {"Status": "healthy"}}}
        baseline = {key: 126 for key in cold.COUNTERS}
        first = dict(baseline, contextual_messages=125, blocks_processed=1)
        second = dict(first, blocks_processed=2)
        with patch.object(cold, "bind"), patch.object(cold, "inspect", return_value=new), \
                patch.object(cold, "writers", return_value=["original"]), \
                patch.object(cold, "metrics", side_effect=[first, second]), \
                patch.object(cold.time, "monotonic", side_effect=[0, 0, 1, 11]), \
                patch.object(cold.time, "sleep"), patch.object(cold, "command") as command:
            with self.assertRaises(TimeoutError):
                cold.recover({"id": "original", "volume": "source"}, baseline,
                             "old", 10, start_if_stopped=False)
        command.assert_not_called()

    def test_a_second_restart_resets_the_progress_observation(self):
        new = {"State": {"Running": True, "StartedAt": "new", "Health": {"Status": "healthy"}}}
        again = copy.deepcopy(new); again["State"]["StartedAt"] = "again"
        baseline = {key: 1 for key in (*cold.COUNTERS, "blocks_processed")}
        with patch.object(cold, "bind"), patch.object(cold, "inspect", side_effect=[new, again, again]), \
                patch.object(cold, "writers", return_value=["original"]), \
                patch.object(cold, "metrics", side_effect=[dict(baseline, blocks_processed=i) for i in [9, 10, 11]]) as measured, \
                patch.object(cold.time, "sleep"):
            result = cold.recover({"id": "original", "volume": "source"}, baseline, "old", 10)
        self.assertEqual(measured.call_count, 3)
        self.assertEqual(result["blocks_processed"], 11)

    def test_readonly_reconciliation_never_starts_a_stopped_original(self):
        with patch.object(cold, "bind"), patch.object(cold, "inspect", return_value={"State": {"Running": False}}), \
                patch.object(cold, "writers", return_value=[]), patch.object(cold, "command") as command:
            with self.assertRaisesRegex(ValueError, "never starts"):
                cold.recover({"id": "original", "volume": "source"}, {}, "old", 10, start_if_stopped=False)
        command.assert_not_called()

    def test_foreign_writer_is_preserved_and_original_not_started(self):
        stopped = {"State": {"Running": False}}
        with patch.object(cold, "bind"), patch.object(cold, "inspect", return_value=stopped), \
                patch.object(cold, "writers", return_value=["foreign"]), \
                patch.object(cold, "command") as command:
            with self.assertRaisesRegex(ValueError, "foreign"):
                cold.recover({"id": "original", "volume": "source"}, {}, "before", 10)
        command.assert_not_called()

    def test_exact_owned_readonly_helper_does_not_block_original_restart(self):
        stopped = {"State": {"Running": False}}
        running = {"State": {"Running": True, "StartedAt": "after", "Health": {"Status": "healthy"}}}
        reader, identity = helper_fixture()
        states = iter([stopped, running, running])
        after = {key: 2 for key in (*cold.COUNTERS, "blocks_processed")}
        with patch.object(cold, "bind"), \
                patch.object(cold, "inspect", side_effect=lambda name: reader if name == identity["id"] else next(states)), \
                patch.object(cold, "writers", return_value=[identity["id"]]), \
                patch.object(cold, "metrics", side_effect=[dict(after, blocks_processed=1), after]), \
                patch.object(cold, "command") as command, patch.object(cold.time, "sleep"):
            cold.recover({"id": "original", "volume": "source"},
                         {key: 1 for key in after}, "before", 10,
                         identity)
        command.assert_called_once_with(["docker", "start", "original"])

    def test_owned_helper_with_writable_source_is_rejected(self):
        stopped = {"State": {"Running": False}}
        reader, identity = helper_fixture()
        reader["Mounts"][0]["RW"] = True
        with patch.object(cold, "bind"), \
                patch.object(cold, "inspect", side_effect=lambda name: reader if name == identity["id"] else stopped), \
                patch.object(cold, "writers", return_value=[identity["id"]]), \
                patch.object(cold, "command") as command:
            with self.assertRaisesRegex(ValueError, "mounts"):
                cold.recover({"id": "original", "volume": "source"}, {}, "before", 10,
                             identity)
        command.assert_not_called()

    def test_owned_helper_run_label_change_is_rejected(self):
        reader, identity = helper_fixture()
        reader["Config"]["Labels"]["io.kasanova.run"] = "foreign"
        with self.assertRaisesRegex(ValueError, "ownership"):
            cold.bind_helper(reader, identity)


class ReceiptTest(unittest.TestCase):
    def test_interrupted_write_does_not_prevent_terminal_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            with patch.object(cold.json, "dump", side_effect=InterruptedError("signal")):
                with self.assertRaises(InterruptedError):
                    cold.write(receipt, {"status": "RUNNING"})
            cold.write(receipt, {"status": "FAIL", "source_recovered": True})
            self.assertEqual(json.loads(receipt.read_text())["status"], "FAIL")
            self.assertEqual(list(Path(directory).glob("*.tmp")), [])

    def test_fresh_receipt_never_replaces_existing_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            cold.write(receipt, {"status": "FAIL"}, fresh=True)
            with self.assertRaises(ValueError):
                cold.write(receipt, {"status": "PASS"}, fresh=True)
            self.assertEqual(json.loads(receipt.read_text())["status"], "FAIL")


class SnapshotLifecycleTest(unittest.TestCase):
    def exercise(self, directory, fail_copy=False, fail_cleanup=False, interrupted=False):
        expected = {"schema": "chats-cold-snapshot-preflight-v1", "id": "original",
                    "image": "sha256:original", "volume": "source",
                    "utility_image": "sha256:helper", "configuration_sha256": "config",
                    "max_bytes": cold.MAX_BYTES, "max_entries": cold.MAX_FILES,
                    "source_started_at": "before"}
        helper = "kasanova-chats-cold-test"
        volume = "kasanova_chats_cold_test"
        state = {"source_running": True, "helper_running": False, "run_id": None}
        events = []

        def inspect(identity):
            if identity == "original":
                return {"State": {"Running": state["source_running"], "ExitCode": 0,
                                  "Health": {"Status": "healthy"}}}
            return {"Id": "f" * 64, "Name": "/" + helper,
                    "Config": {"Labels": {"io.kasanova.run": state["run_id"]}},
                    "Image": "sha256:helper", "State": {
                        "Running": state["helper_running"], "ExitCode": 0},
                    "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True},
                    "Mounts": [{"Type": "volume", "Name": "source", "Destination": "/from", "RW": False},
                               {"Type": "volume", "Name": volume, "Destination": "/to", "RW": True}]}

        def command(args, timeout=30):
            events.append(tuple(args[:3]))
            if args[1:3] == ["volume", "create"]:
                state["run_id"] = next(x.split("=", 1)[1] for x in args if x.startswith("io.kasanova.run="))
            elif args[1:3] == ["volume", "inspect"]:
                return json.dumps([{"Labels": {"io.kasanova.run": state["run_id"]}}]).encode()
            elif args[1:2] == ["create"]:
                return ("f" * 64).encode()
            elif args[1:3] == ["stop", "--timeout"]:
                state["source_running"] = False
            elif args[1:3] == ["start", "--attach"]:
                if fail_copy or interrupted:
                    state["helper_running"] = True
                    if interrupted:
                        raise InterruptedError("signal")
                    raise RuntimeError("copy failed")
            elif args[1:2] == ["stop"]:
                if fail_cleanup:
                    raise RuntimeError("helper stop failed")
                state["helper_running"] = False
            elif args[1:2] == ["logs"]:
                return json.dumps({"inventory_sha256": "digest", "bytes": 256, "entries": 3}).encode()
            return b"ok"

        def recover(*args):
            events.append(("RECOVER", "original"))
            self.assertFalse(state["source_running"])
            state["source_running"] = True
            return {"contextual_messages": 12, "push_registered_devices": 3}

        receipt = Path(directory) / "result.json"
        with patch.object(cold, "preflight", return_value=expected), \
                patch.object(cold, "bind"), patch.object(cold, "inspect", side_effect=inspect), \
                patch.object(cold, "writers", side_effect=lambda _: ["original"] if state["source_running"] else []), \
                patch.object(cold, "metrics", return_value={"contextual_messages": 12}), \
                patch.object(cold, "command", side_effect=command), \
                patch.object(cold, "recover", side_effect=recover), \
                patch.object(cold.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)):
            if fail_copy or interrupted:
                with self.assertRaises(RuntimeError):
                    cold.snapshot(expected, volume, helper, receipt)
            else:
                cold.snapshot(expected, volume, helper, receipt)
        self.assertTrue(state["source_running"])
        self.assertEqual(events.count(("RECOVER", "original")), 1)
        return json.loads(receipt.read_text())

    def test_success_recovers_original_and_keeps_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = self.exercise(directory)
        self.assertEqual(receipt["status"], "PASS")
        self.assertTrue(receipt["backup_complete"])
        self.assertTrue(receipt["source_recovered"])

    def test_copy_failure_recovers_original_and_retains_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = self.exercise(directory, fail_copy=True)
        self.assertEqual(receipt["status"], "FAIL")
        self.assertFalse(receipt["backup_complete"])
        self.assertTrue(receipt["source_recovered"])

    def test_failed_helper_stop_still_attempts_original_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = self.exercise(directory, fail_copy=True, fail_cleanup=True)
        self.assertEqual(receipt["status"], "FAIL")
        self.assertTrue(receipt["source_recovered"])
        self.assertEqual(receipt["helper_cleanup_error_type"], "RuntimeError")

    def test_interruption_runs_recovery_before_reraising(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = self.exercise(directory, interrupted=True)
        self.assertEqual(receipt["status"], "FAIL")
        self.assertTrue(receipt["source_recovered"])


if __name__ == "__main__":
    unittest.main()
