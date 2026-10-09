"""Bind resumed private staging configuration and running services to requested pins."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess

from stage import PROJECT


def expected_config(project, network, processor_image, web_image):
    if not PROJECT.fullmatch(project) or network not in ("mainnet", "testnet-10"):
        raise ValueError("Invalid private staging project/network")
    for image in (processor_image, web_image):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
            raise ValueError("Immutable local image ID required")
    return {
        "HISTORY_PROJECT": project,
        "SOCIAL_NETWORK": network,
        "SOCIAL_PROCESSOR_IMAGE": processor_image,
        "SOCIAL_WEB_IMAGE": web_image,
    }


def ensure_environment(path, expected, create=False):
    path = Path(path)
    if not path.exists() and create:
        # Exclusive creation cannot replace an existing resumed environment.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        values = {**expected, "HISTORY_DB_PASSWORD": secrets.token_hex(32)}
        with os.fdopen(fd, "w") as handle:
            handle.write("".join(k + "=" + v + "\n" for k, v in values.items()))
    values = {}
    for line in path.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or key in values:
            raise ValueError("Malformed or duplicate staging configuration")
        values[key] = value
    if set(values) != set(expected) | {"HISTORY_DB_PASSWORD"}:
        raise ValueError("Unexpected staging configuration keys")
    for key, value in expected.items():
        if values[key] != value:
            raise ValueError("Resumed staging configuration mismatch: " + key)
    if not re.fullmatch(r"[0-9a-f]{64}", values["HISTORY_DB_PASSWORD"]):
        raise ValueError("Invalid staging database credential format")


def verify_services(expected):
    project = expected["HISTORY_PROJECT"]
    results = {}
    for service, key in (
        ("processor", "SOCIAL_PROCESSOR_IMAGE"),
        ("web", "SOCIAL_WEB_IMAGE"),
    ):
        identifiers = (
            subprocess.check_output(
                ["docker", "compose", "--project-name", project, "ps", "-q", service],
                text=True,
            )
            .strip()
            .splitlines()
        )
        if len(identifiers) != 1:
            raise ValueError("Expected one staging " + service)
        data = json.loads(
            subprocess.check_output(["docker", "inspect", identifiers[0]], text=True)
        )[0]
        labels = data["Config"].get("Labels", {})
        if (
            labels.get("com.docker.compose.project") != project
            or labels.get("com.docker.compose.service") != service
            or data["Image"] != expected[key]
            or not data["State"]["Running"]
        ):
            raise ValueError("Running staging service provenance mismatch: " + service)
        if set(data["NetworkSettings"]["Networks"]) != {project + "_history"}:
            raise ValueError("Staging service is not on its isolated network")
        if service == "processor":
            command = data["Config"]["Cmd"]
            if command.count("--network") != 1:
                raise ValueError("Missing or ambiguous processor network")
            position = command.index("--network") + 1
            if (
                position == len(command)
                or command[position] != expected["SOCIAL_NETWORK"]
            ):
                raise ValueError("Running processor network mismatch")
        results[service] = {"container_id": data["Id"], "image": data["Image"]}
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "check"))
    for name in ("project", "network", "processor-image", "web-image"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    expected = expected_config(
        args.project, args.network, args.processor_image, args.web_image
    )
    ensure_environment(Path(".env"), expected, create=args.action == "prepare")
    if args.action == "check":
        print(
            json.dumps(
                {"configuration": expected, "services": verify_services(expected)},
                indent=2,
            )
        )
