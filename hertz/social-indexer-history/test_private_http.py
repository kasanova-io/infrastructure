import copy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from private_http import PrivateHttpReader
from verify_stage import read_json


class PrivateHttpTest(unittest.TestCase):
    def setUp(self):
        self.project = "social_history_http_test"
        self.image = "sha256:" + "a" * 64
        self.stage = SimpleNamespace(
            command=["docker", "compose", "--project-name", self.project]
        )
        self.container = {
            "Id": "web-id",
            "Image": self.image,
            "State": {"Running": True, "StartedAt": "fixed-epoch"},
            "Config": {
                "Labels": {
                    "com.docker.compose.project": self.project,
                    "com.docker.compose.service": "web",
                }
            },
            "NetworkSettings": {
                "Networks": {self.project + "_history": {"IPAddress": "172.20.0.3"}},
                "Ports": {"3001/tcp": None},
            },
        }
        self.network = {
            "Id": "network-id",
            "Internal": True,
            "Labels": {"com.docker.compose.project": self.project},
            "Containers": {"web-id": {}},
            "IPAM": {"Config": [{"Subnet": "172.20.0.0/24"}]},
        }
        self.connection = Mock()

    def docker(self, command, text):
        if command[:2] == ["docker", "compose"]:
            return "web-id\n"
        if command[:3] == ["docker", "network", "inspect"]:
            return json.dumps([self.network])
        return json.dumps([self.container])

    def reader(self):
        with (
            patch("private_http.sys.platform", "linux"),
            patch("private_http.subprocess.check_output", side_effect=self.docker),
            patch(
                "private_http.http.client.HTTPConnection", return_value=self.connection
            ) as constructor,
        ):
            reader = PrivateHttpReader(self.stage, self.image)
            constructor.assert_called_once_with("172.20.0.3", 3001, timeout=10)
            return reader

    def test_host_and_image_pin_are_mandatory(self):
        with (
            patch("private_http.sys.platform", "darwin"),
            self.assertRaises(ValueError),
        ):
            PrivateHttpReader(self.stage, self.image)
        with patch("private_http.sys.platform", "linux"), self.assertRaises(ValueError):
            PrivateHttpReader(self.stage, "latest")

    def test_runtime_image_network_and_ports_fail_closed(self):
        original_container, original_network = copy.deepcopy(
            self.container
        ), copy.deepcopy(self.network)
        for case in ["image", "shared", "published", "outside", "member"]:
            self.container, self.network = copy.deepcopy(
                original_container
            ), copy.deepcopy(original_network)
            if case == "image":
                self.container["Image"] = "sha256:" + "f" * 64
            if case == "shared":
                self.network["Internal"] = False
            if case == "published":
                self.container["NetworkSettings"]["Ports"]["3001/tcp"] = [
                    {"HostPort": "3001"}
                ]
            if case == "outside":
                self.network["IPAM"]["Config"][0]["Subnet"] = "192.168.0.0/24"
            if case == "member":
                self.network["Containers"] = {}
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.reader()

    def test_same_verification_json_path_is_used(self):
        reader = self.reader()
        response = self.connection.getresponse.return_value
        response.status = 200
        response.read.return_value = b'{"post":{"id":"fixed"}}'
        self.stage.http_reader = reader
        self.assertEqual(
            read_json(self.stage, "http://127.0.0.1:3001/get-post-details?id=fixed"),
            {"post": {"id": "fixed"}},
        )
        self.connection.request.assert_called_once_with(
            "GET",
            "/get-post-details?id=fixed",
            headers={"User-Agent": "Kasanova-Private-Readback/1"},
        )

    def test_default_transport_remains_existing_docker_command(self):
        with patch(
            "verify_stage.subprocess.check_output", return_value='{"posts":[]}'
        ) as read:
            self.assertEqual(
                read_json(self.stage, "http://127.0.0.1:3001/get-users-following?x=y"),
                {"posts": []},
            )
            self.assertEqual(
                read.call_args.args[0],
                self.stage.command
                + [
                    "exec",
                    "-T",
                    "web",
                    "wget",
                    "-qO-",
                    "http://127.0.0.1:3001/get-users-following?x=y",
                ],
            )

    def test_unexpected_route_does_not_send_a_request(self):
        reader = self.reader()
        for url in [
            "https://example.com/get-post-details",
            "http://127.0.0.1:3001/admin",
            "http://127.0.0.1:3001/get-post-details#fragment",
        ]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                reader.read(url)
        self.connection.request.assert_not_called()

    def test_http_failure_size_limit_and_timeout_never_fallback(self):
        reader = self.reader()
        response = self.connection.getresponse.return_value
        for status in [301, 400, 404, 500]:
            response.status, response.read.return_value = status, b"{}"
            with (
                self.subTest(status=status),
                self.assertRaisesRegex(ValueError, str(status)),
            ):
                reader.read("http://127.0.0.1:3001/get-user-details")
        response.status = 200
        reader.MAX_BYTES = 4
        response.read.return_value = b"12345"
        with self.assertRaisesRegex(ValueError, "bounded"):
            reader.read("http://127.0.0.1:3001/get-user-details")
        self.connection.request.side_effect = TimeoutError("deadline")
        with self.assertRaises(TimeoutError):
            reader.read("http://127.0.0.1:3001/get-user-details")
        self.assertEqual(self.connection.close.call_count, 6)

    def test_identity_drift_prevents_success_receipt(self):
        reader = self.reader()
        with patch("private_http.subprocess.check_output", side_effect=self.docker):
            self.assertEqual(reader.verify_identity()["before"], reader.before)
            self.container["State"]["StartedAt"] = "restarted"
            with self.assertRaisesRegex(ValueError, "identity changed"):
                reader.verify_identity()
