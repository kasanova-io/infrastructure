"""Production preparer source and rejection checks; no Docker or fake lock."""
import importlib.util
import os
import pathlib
import tempfile
import unittest
from unittest.mock import patch

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("processor_build", HERE / "prepare-processor-build.py")
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)
SOURCE = pathlib.Path(os.environ.get("KSNV411_UPSTREAM_SOURCE", str(build.SOURCE)))


class ProcessorBuild(unittest.TestCase):
    def test_actual_pinned_tracked_source_selected(self):
        files = build.tracked_source(SOURCE)
        self.assertIn("K-transaction-processor/src/k_protocol.rs", files)
        self.assertTrue(all(name in {"Cargo.toml", "K-transaction-processor/Cargo.toml"}
                            or name.startswith("K-transaction-processor/src/") for name in files))
        self.assertFalse(any(".env" in pathlib.PurePosixPath(name).parts for name in files))

    def test_actual_pin_drift_rejected(self):
        with patch.object(build.subprocess, "check_output", return_value="0" * 40):
            with self.assertRaisesRegex(ValueError, "new upstream pin"):
                build.tracked_source(SOURCE)

    def test_actual_dirty_tracked_source_rejected(self):
        with patch.object(build.subprocess, "check_output", side_effect=[build.PIN, " M source"]):
            with self.assertRaisesRegex(ValueError, "local changes"):
                build.tracked_source(SOURCE)

    def test_occupied_context_preserved(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
            destination = pathlib.Path(directory) / "context"
            destination.mkdir()
            witness = destination / "witness"
            witness.write_bytes(b"keep")
            with self.assertRaisesRegex(ValueError, "Occupied"):
                build.prepare(SOURCE, destination, pathlib.Path(directory) / "missing-lock")
            self.assertEqual(witness.read_bytes(), b"keep")

    def test_symlink_context_rejected(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
            destination = pathlib.Path(directory) / "context"
            destination.symlink_to(pathlib.Path(directory) / "absent", target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "Occupied"):
                build.prepare(SOURCE, destination, pathlib.Path(directory) / "missing-lock")
            self.assertTrue(destination.is_symlink())

    def test_missing_real_lock_creates_no_context(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
            destination = pathlib.Path(directory) / "context"
            with self.assertRaisesRegex(ValueError, "Missing regular"):
                build.prepare(SOURCE, destination, pathlib.Path(directory) / "missing-lock")
            self.assertFalse(destination.exists())

    def test_symlink_input_rejected(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
            source = pathlib.Path(directory) / "source"
            source.write_bytes(b"public test witness")
            link = pathlib.Path(directory) / "link"
            link.symlink_to(source)
            with self.assertRaisesRegex(ValueError, "Missing regular"):
                build.read_regular(link)


if __name__ == "__main__":
    unittest.main(verbosity=2)
