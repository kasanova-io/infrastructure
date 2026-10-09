import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from provenance import ensure_environment, expected_config, verify_services


class ProvenanceTest(unittest.TestCase):
    def setUp(self):
        self.expected = expected_config(
            "social_history_provenance",
            "mainnet",
            "sha256:" + "1" * 64,
            "sha256:" + "2" * 64,
        )

    def test_create_and_resume_preserve_existing_credential(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            ensure_environment(path, self.expected, create=True)
            original = path.read_bytes()
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            ensure_environment(path, self.expected, create=True)
            self.assertEqual(path.read_bytes(), original)

    def test_each_resumed_pin_mismatch_rejects_without_rewriting(self):
        for key, replacement in (
            ("HISTORY_PROJECT", "social_history_other"),
            ("SOCIAL_NETWORK", "testnet-10"),
            ("SOCIAL_PROCESSOR_IMAGE", "sha256:" + "3" * 64),
            ("SOCIAL_WEB_IMAGE", "sha256:" + "4" * 64),
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / ".env"
                ensure_environment(path, self.expected, create=True)
                original = path.read_bytes()
                with self.assertRaisesRegex(ValueError, key):
                    ensure_environment(
                        path, {**self.expected, key: replacement}, create=True
                    )
                self.assertEqual(path.read_bytes(), original)

    def test_duplicate_or_extra_configuration_is_rejected(self):
        for extra in ("SOCIAL_NETWORK=mainnet\n", "COMPOSE_FILE=other.yaml\n"):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / ".env"
                ensure_environment(path, self.expected, create=True)
                with path.open("a") as handle:
                    handle.write(extra)
                with self.assertRaises(ValueError):
                    ensure_environment(path, self.expected)

    def fixtures(self):
        return {
            service: {
                "Id": service + "-id",
                "Image": self.expected[key],
                "State": {"Running": True},
                "Config": {
                    "Labels": {
                        "com.docker.compose.project": self.expected["HISTORY_PROJECT"],
                        "com.docker.compose.service": service,
                    },
                    "Cmd": ["--network", "mainnet"],
                },
                "NetworkSettings": {
                    "Networks": {"social_history_provenance_history": {}}
                },
            }
            for service, key in (
                ("processor", "SOCIAL_PROCESSOR_IMAGE"),
                ("web", "SOCIAL_WEB_IMAGE"),
            )
        }

    def check_runtime(self, fixtures):
        def docker(command, text):
            if command[1] == "compose":
                self.assertEqual(
                    command[2:4], ["--project-name", self.expected["HISTORY_PROJECT"]]
                )
                return command[-1] + "\n"
            self.assertEqual(command[:2], ["docker", "inspect"])
            return json.dumps([fixtures[command[-1]]])

        with patch("provenance.subprocess.check_output", side_effect=docker):
            return verify_services(self.expected)

    def test_running_images_are_read_from_docker_not_assumed_from_env(self):
        fixtures = self.fixtures()
        proof = self.check_runtime(fixtures)
        self.assertEqual(
            proof["processor"]["image"], self.expected["SOCIAL_PROCESSOR_IMAGE"]
        )
        for service in ("processor", "web"):
            with self.subTest(service=service):
                changed = copy.deepcopy(fixtures)
                changed[service]["Image"] = "sha256:" + "f" * 64
                with self.assertRaisesRegex(ValueError, "provenance mismatch"):
                    self.check_runtime(changed)

    def test_actual_processor_network_must_match_requested_network(self):
        fixtures = self.fixtures()
        fixtures["processor"]["Config"]["Cmd"] = ["--network", "testnet-10"]
        with self.assertRaisesRegex(ValueError, "network mismatch"):
            self.check_runtime(fixtures)

    def test_other_project_or_shared_network_is_rejected(self):
        fixtures = self.fixtures()
        fixtures["web"]["Config"]["Labels"][
            "com.docker.compose.project"
        ] = "social_indexer_prod"
        with self.assertRaisesRegex(ValueError, "provenance mismatch"):
            self.check_runtime(fixtures)
        fixtures = self.fixtures()
        fixtures["web"]["NetworkSettings"]["Networks"]["caddy_caddy_net"] = {}
        with self.assertRaisesRegex(ValueError, "isolated network"):
            self.check_runtime(fixtures)
