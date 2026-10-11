import tempfile
from pathlib import Path
import unittest
import json
from types import SimpleNamespace
from complete_stage import ready, state, run
from history import History


class CompletionTest(unittest.TestCase):
    def test_waits_for_jobs_and_archive_work_but_preserves_exclusions(self):
        pending = {"unfinished_jobs": 1, "records": {"verified": 20}}
        rejected = {"unfinished_jobs": 0, "records": {"verified": 20, "quarantined": 1}}
        self.assertFalse(ready([pending, rejected]))
        pending["unfinished_jobs"] = 0
        pending["records"]["discovered"] = 1
        self.assertFalse(ready([pending, rejected]))
        pending["records"]["discovered"] = 0
        self.assertTrue(ready([pending, rejected]))
        self.assertFalse(ready([{"unfinished_jobs": 0, "records": {}}, rejected]))

    def test_state_uses_actual_inventory_without_mutation(self):
        with tempfile.TemporaryDirectory() as root:
            h = History(root, "https://source", "https://archive", "mainnet")
            h.add_job("/get-posts-watching", {}, "posts")
            h.db.execute(
                "INSERT INTO records(txid,status) VALUES('fixture','verified')"
            )
            h.db.commit()
            self.assertEqual(
                state(Path(root)), {"unfinished_jobs": 1, "records": {"verified": 1}}
            )
            h.db.close()

    def test_old_four_file_snapshot_is_rejected_before_any_tooling_write(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            tooling = output / "tooling"
            tooling.mkdir()
            previous = {
                name: "original-digest"
                for name in ("stage.py", "verify_stage.py", "compose.yaml", "init.sql")
            }
            manifest = output / "tooling-sha256.json"
            manifest.write_text(json.dumps(previous))
            original = manifest.read_bytes()
            args = SimpleNamespace(
                project="social_history_original",
                output=output,
                processor_image="sha256:" + "1" * 64,
                web_image="sha256:" + "2" * 64,
            )
            with self.assertRaisesRegex(ValueError, "fresh output directory"):
                run(args)
            self.assertEqual(manifest.read_bytes(), original)
            self.assertEqual(list(tooling.iterdir()), [])
