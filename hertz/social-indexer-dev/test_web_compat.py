"""Apply the reviewed patch to temporary source; test production validator and coverage."""
import pathlib
import shutil
import subprocess
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
SOURCE = HERE.parent.parent / "vendor/k-indexer"


class HistoricalAuthorCompatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = pathlib.Path(cls.temp.name)
        shutil.copytree(SOURCE / "K-webserver/src", cls.root / "K-webserver/src")
        subprocess.run(["patch", "--batch", "--fuzz=0", "-p1", "-i", str(HERE / "patches/historical-author-keys.patch")], cwd=cls.root, check=True, capture_output=True)
        subprocess.run(["patch", "--batch", "--fuzz=0", "-p1", "-i", str(HERE / "patches/exact-vote-details.patch")], cwd=cls.root, check=True, capture_output=True)
        cls.handlers = (cls.root / "K-webserver/src/api_handlers.rs").read_text()

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_actual_production_validator(self):
        # Compile the unchanged function and its Rust tests from patched source.
        source = self.handlers[self.handlers.index("// Author keys are opaque protocol identities;"):]
        file = self.root / "validator.rs"
        file.write_text(source)
        binary = self.root / "validator-tests"
        subprocess.run(["rustc", "--edition=2024", "--test", str(file), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True)

    def test_all_author_targets_reuse_validator_and_requesters_stay_compressed(self):
        self.assertEqual(self.handlers.count("if !is_valid_author_key(user_public_key)"), 4)
        self.assertEqual(self.handlers.count("if !is_valid_author_key(user_pubkey)"), 2)
        self.assertEqual(self.handlers.count("if !is_valid_author_key(pubkey)"), 1)
        self.assertNotIn("if user_public_key.len() != 66", self.handlers)
        self.assertNotIn("if user_pubkey.len() != 66", self.handlers)
        original = (SOURCE / "K-webserver/src/api_handlers.rs").read_text()
        self.assertEqual(self.handlers.count("if requester_pubkey.len() != 66"), original.count("if requester_pubkey.len() != 66"))
        self.assertNotIn("pk[2..]", self.handlers)
        database = (self.root / "K-webserver/src/database_postgres_impl.rs").read_text()
        self.assertIn('" AND b.sender_pubkey = ${}"', database)
        self.assertNotIn("hex_pattern", database)


if __name__ == "__main__":
    unittest.main()
