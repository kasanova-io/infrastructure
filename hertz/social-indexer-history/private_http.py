"""Optional persistent readback on the Linux host of an isolated staging web."""

import http.client
import ipaddress
import json
import re
import subprocess
import sys
import urllib.parse


class PrivateHttpReader:
    MAX_BYTES = 8 * 1024**2
    ROUTES = {
        "/get-post-details",
        "/get-vote-details",
        "/get-user-details",
        "/get-users-following",
        "/get-blocked-users",
    }

    def __init__(self, stage, expected_image):
        if sys.platform != "linux":
            raise ValueError("Private HTTP readback must run on the Linux Docker host")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", expected_image):
            raise ValueError("Private HTTP readback requires the reviewed image pin")
        self.command = stage.command
        self.expected_image = expected_image
        self.project = self.command[self.command.index("--project-name") + 1]
        if not re.fullmatch(r"social_history_[a-z0-9_]+", self.project):
            raise ValueError("Not a private history project")
        self.before = self.inspect()
        self.connection = http.client.HTTPConnection(
            self.before["ip"], 3001, timeout=10
        )

    def inspect(self):
        identifiers = subprocess.check_output(
            self.command + ["ps", "-q", "web"], text=True
        ).split()
        if len(identifiers) != 1:
            raise ValueError("Expected exactly one private staging web container")
        container = json.loads(
            subprocess.check_output(["docker", "inspect", identifiers[0]], text=True)
        )[0]
        labels = container["Config"].get("Labels", {})
        network_name = self.project + "_history"
        settings = container["NetworkSettings"]
        if (
            labels.get("com.docker.compose.project") != self.project
            or labels.get("com.docker.compose.service") != "web"
            or container["Image"] != self.expected_image
            or not container["State"]["Running"]
            or set(settings["Networks"]) != {network_name}
            or any(settings.get("Ports", {}).values())
        ):
            raise ValueError("Private HTTP container provenance mismatch")
        network = json.loads(
            subprocess.check_output(
                ["docker", "network", "inspect", network_name], text=True
            )
        )[0]
        address = ipaddress.ip_address(settings["Networks"][network_name]["IPAddress"])
        if (
            not network.get("Internal")
            or network.get("Labels", {}).get("com.docker.compose.project")
            != self.project
            or container["Id"] not in network.get("Containers", {})
            or not address.is_private
            or not any(
                address in ipaddress.ip_network(config["Subnet"])
                for config in network["IPAM"]["Config"]
            )
        ):
            raise ValueError("Private HTTP network provenance mismatch")
        return {
            "container_id": container["Id"],
            "started_at": container["State"]["StartedAt"],
            "image": container["Image"],
            "network": network_name,
            "network_id": network["Id"],
            "ip": str(address),
            "port": 3001,
        }

    def read(self, url):
        parsed = urllib.parse.urlsplit(url)
        if (
            parsed.scheme != "http"
            or parsed.netloc != "127.0.0.1:3001"
            or parsed.fragment
            or parsed.path not in self.ROUTES
        ):
            raise ValueError(
                "Private readback only accepts existing verification routes"
            )
        target = parsed.path + ("?" + parsed.query if parsed.query else "")
        try:
            self.connection.request(
                "GET", target, headers={"User-Agent": "Kasanova-Private-Readback/1"}
            )
            response = self.connection.getresponse()
            body = response.read(self.MAX_BYTES + 1)
            if response.status != 200:
                raise ValueError("Private readback HTTP status " + str(response.status))
            if len(body) > self.MAX_BYTES:
                raise ValueError("Private readback exceeds bounded response size")
            return body.decode("utf-8")
        except Exception:
            self.close()
            raise

    def verify_identity(self):
        after = self.inspect()
        if after != self.before:
            self.close()
            raise ValueError("Private HTTP staging identity changed during readback")
        return {"before": self.before, "after": after}

    def close(self):
        self.connection.close()
