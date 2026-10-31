#!/usr/bin/env python3
"""Create private read-only source binding from verified candidate containers.

Run on the Docker host after starting database/indexer, before init/adapters.
The receipt contains no credentials. The adapter receives its SQL credential
through PROTOCOL_DATABASE_URL; no Docker socket is exposed to it.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import unquote, urlsplit

from protocol_indexer.source import SIMPLY_IMAGE_REF, SIMPLY_REVISION


def docker(*args):
    # Container inspection includes secret command fields. Never print it.
    return subprocess.check_output(["docker", *args], text=True)


def inspect(identifier):
    return json.loads(docker("inspect", identifier))[0]


def option(command, names):
    positions = [i for i, value in enumerate(command) if value in names]
    if len(positions) != 1 or positions[0] + 1 >= len(command):
        raise ValueError("missing or ambiguous collector option")
    return command[positions[0] + 1]


def write_binding(output, binding):
    """Atomic, non-secret read-only receipt; independent of caller's umask."""
    temporary = output.with_name(output.name + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(binding, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fchmod(stream.fileno(), 0o444)
        os.fsync(stream.fileno())
    temporary.replace(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--output", type=Path, default=Path("source-binding.json"))
    args = parser.parse_args()
    config = json.loads(subprocess.check_output(
        ["docker", "compose", "-f", "compose.candidate.yaml", "--profile", "candidate", "config", "--format", "json"], text=True))
    if config["name"] != args.project:
        raise ValueError("compose project does not match requested source")
    lock = json.loads(Path("SOURCE.lock").read_text())["simply"]
    if lock["revision"] != SIMPLY_REVISION or lock["image"] != SIMPLY_IMAGE_REF:
        raise ValueError("source lock does not match reviewed adapter")
    containers = {}
    for service in ("indexer", "database"):
        ids = docker("ps", "-q", "--filter", "label=com.docker.compose.project=" + args.project,
                     "--filter", "label=com.docker.compose.service=" + service).split()
        if len(ids) != 1:
            raise ValueError("expected one running source " + service)
        item = inspect(ids[0])
        if not item["State"]["Running"]:
            raise ValueError("source is not running")
        if Path(item["Config"]["Labels"]["com.docker.compose.project.working_dir"]).resolve() != Path.cwd().resolve():
            raise ValueError("running source belongs to another work directory")
        containers[service] = item
    indexer, database = containers["indexer"], containers["database"]
    expected_image = json.loads(docker("image", "inspect", lock["image"]))[0]["Id"]
    if indexer["Image"] != expected_image:
        raise ValueError("running collector image does not match source lock")
    command = indexer["Config"]["Cmd"]
    if command != config["services"]["indexer"]["command"]:
        raise ValueError("running collector command does not match committed compose")
    if option(command, {"-n", "--network"}) != args.network:
        raise ValueError("running collector network mismatch")
    url = option(command, {"-d", "--database-url"})
    if url != config["services"]["init"]["environment"]["PROTOCOL_DATABASE_URL"]:
        raise ValueError("collector and adapter SQL configuration differ")
    endpoint = urlsplit(url)
    if endpoint.scheme not in {"postgres", "postgresql"} or endpoint.hostname != "database" or (endpoint.port or 5432) != 5432:
        raise ValueError("candidate source must use its private database service")
    private_network = config["networks"]["protocol"]["name"]
    for item in (indexer, database):
        if private_network not in item["NetworkSettings"]["Networks"]:
            raise ValueError("source is not on the private protocol network")
    if "database" not in database["NetworkSettings"]["Networks"][private_network]["Aliases"]:
        raise ValueError("SQL service alias is not bound to verified database container")
    db_name, db_user = unquote(endpoint.path[1:]), unquote(endpoint.username or "")
    sql = """SELECT json_build_object('system_identifier', system_identifier::text,
        'oid', (SELECT oid::bigint FROM pg_database WHERE datname=current_database()),
        'name', current_database()) FROM pg_control_system()"""
    identity = json.loads(docker("exec", database["Id"], "psql", "-U", db_user, "-d", db_name, "-Atc", sql))
    if identity["name"] != db_name:
        raise ValueError("SQL identity database mismatch")
    binding = {"schema_version": 1, "network": args.network, "source_revision": SIMPLY_REVISION,
               "source_image_ref": lock["image"], "source_image_id": indexer["Image"],
               "source_container": indexer["Id"],
               "metrics_url": config["services"]["init"]["environment"]["SIMPLY_METRICS_URL"],
               "database": {"host": endpoint.hostname, "port": endpoint.port or 5432, **identity}}
    # Atomic replacement supports legitimate source container recreation without
    # exposing a partially written binding. Recreate adapter containers afterward
    # so a read-only file mount receives the new inode.
    write_binding(args.output, binding)
    print(json.dumps({"binding_created": str(args.output), "network": args.network,
                      "source_revision": SIMPLY_REVISION, "project": args.project}))


if __name__ == "__main__":
    main()
