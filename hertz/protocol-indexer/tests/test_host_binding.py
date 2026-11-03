import importlib.util
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("bind_source", Path(__file__).parents[1] / "bind-source.py")
host = importlib.util.module_from_spec(spec)
spec.loader.exec_module(host)


class HostBindingTests(unittest.TestCase):
    def test_non_root_worker_can_read_receipt_under_private_umask(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source-binding.json"
            previous = os.umask(0o077)
            try:
                host.write_binding(path, {"network": "testnet-10", "no_credentials": True})
            finally:
                os.umask(previous)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o444)
            self.assertEqual(json.loads(path.read_text())["network"], "testnet-10")
            self.assertFalse(path.with_suffix(".json.tmp").exists())
            host.write_binding(path, {"network": "mainnet"})
            self.assertEqual(json.loads(path.read_text())["network"], "mainnet")

    def test_collector_option_cannot_be_missing_or_ambiguous(self):
        self.assertEqual(host.option(["-n", "testnet-10"], {"-n", "--network"}), "testnet-10")
        for command in ([], ["-n"], ["-n", "mainnet", "--network", "testnet-10"]):
            with self.subTest(command=command), self.assertRaises(ValueError):
                host.option(command, {"-n", "--network"})
