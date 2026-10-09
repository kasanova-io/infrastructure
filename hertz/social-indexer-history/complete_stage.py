#!/usr/bin/env python3
"""Wait for both collectors, then replay accepted data into a NEW private stage."""
import argparse
from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import sqlite3
import subprocess
import time
from history import History
from relationships import Relationships
from stage import PROJECT, validate_records

FILES = ("stage.py", "verify_stage.py", "provenance.py", "compose.yaml", "init.sql")
# Images are supplied explicitly on the CLI, never inferred from a mutable tag.


def state(directory):
    with sqlite3.connect(
        f"file:{directory / 'inventory.sqlite'}?mode=ro", uri=True
    ) as db:
        return {
            "unfinished_jobs": db.execute(
                "SELECT count(*) FROM jobs WHERE done=0"
            ).fetchone()[0],
            "records": dict(
                db.execute("SELECT status,count(*) FROM records GROUP BY status")
            ),
        }


def ready(states):
    return all(
        s["unfinished_jobs"] == 0
        and s["records"].get("discovered", 0) == 0
        and s["records"].get("verified", 0) > 0
        for s in states
    )


def run(args):
    if not PROJECT.fullmatch(args.project):
        raise ValueError("Only a private history project is allowed")
    for digest in (args.processor_image, args.web_image):
        if not digest.startswith("sha256:") or len(digest) != 71:
            raise ValueError("An immutable local image digest is required")
        bytes.fromhex(digest[7:])
    args.output.mkdir(parents=True, exist_ok=True)
    frozen = args.output / "tooling"
    frozen.mkdir(exist_ok=True)
    manifest = args.output / "tooling-sha256.json"
    if manifest.exists() and set(json.loads(manifest.read_text())) != set(FILES):
        raise ValueError(
            "Frozen staging tooling version differs; use a fresh output directory"
        )
    for name in FILES:
        dest = frozen / name
        # Resuming a watcher retains its original tested tool versions.
        if not dest.exists():
            shutil.copyfile(Path(__file__).with_name(name), dest)
    hashes = {
        name: hashlib.sha256((frozen / name).read_bytes()).hexdigest() for name in FILES
    }
    if manifest.exists() and json.loads(manifest.read_text()) != hashes:
        raise ValueError("Frozen staging tooling changed")
    manifest.write_text(json.dumps(hashes, indent=2) + "\n")
    batch = args.output / "batch.jsonl"
    if not batch.exists():
        while True:
            states = [state(args.content), state(args.relationships)]
            print(
                json.dumps(
                    {
                        "at": time.time(),
                        "waiting": not ready(states),
                        "inventories": states,
                        "live_imported": 0,
                    }
                ),
                flush=True,
            )
            if ready(states):
                with ExitStack() as stack:
                    try:
                        for root in (args.content, args.relationships):
                            for phase in ("discover", "verify"):
                                lock = stack.enter_context(
                                    (root / (phase + ".lock")).open("a")
                                )
                                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        time.sleep(2)
                        continue
                    # Recheck after owning every collector lock.
                    states = [state(args.content), state(args.relationships)]
                    if not ready(states):
                        continue
                    content = History(
                        args.content, args.source, args.archive, args.network
                    )
                    relations = Relationships(
                        args.relationships,
                        args.source,
                        args.archive,
                        args.network,
                        args.content,
                    )
                    try:
                        for name, inventory in (
                            ("content.jsonl", content),
                            ("relationships.jsonl", relations),
                        ):
                            target = args.output / name
                            if not target.exists():
                                inventory.export(target)
                    finally:
                        content.db.close()
                        relations.db.close()
                    rows = []
                    for name in ("content.jsonl", "relationships.jsonl"):
                        rows.extend(
                            json.loads(line)
                            for line in (args.output / name).read_text().splitlines()
                        )
                    rows.sort(key=lambda r: (r["block_time"], r["transaction_id"]))
                    data = "".join(
                        json.dumps(r, separators=(",", ":")) + "\n" for r in rows
                    )
                    validate_records(data.encode(), args.network, True)
                    expected = set()
                    for root in (args.content, args.relationships):
                        with sqlite3.connect(
                            f"file:{root / 'inventory.sqlite'}?mode=ro", uri=True
                        ) as db:
                            expected.update(
                                r[0]
                                for r in db.execute(
                                    "SELECT txid FROM records WHERE status='verified'"
                                )
                            )
                    if {r["transaction_id"] for r in rows} != expected:
                        raise ValueError(
                            "Prior partial export differs from completed inventories; use a fresh output directory"
                        )
                    temp = batch.with_suffix(".jsonl.tmp")
                    temp.write_text(data)
                    os.replace(temp, batch)
                    (args.output / "coverage.json").write_text(
                        json.dumps(
                            {
                                "inventories": states,
                                "staging_records": len(rows),
                                "scope": "retained content/current profile and relationship projections; not full chain history",
                                "live_imported": 0,
                            },
                            indent=2,
                        )
                        + "\n"
                    )
                break
            time.sleep(30)
    validate_records(batch.read_bytes(), args.network, True)
    remote = "/home/ren/Kasanova/deployments/" + args.project.replace("_", "-")
    # Only a fresh marked private project is targeted; Stage enforces its DB
    # marker, actual Docker labels, network and immutable batch hash again.
    subprocess.run(["ssh", args.server, "mkdir -p " + shlex.quote(remote)], check=True)
    subprocess.run(
        ["rsync", "-az", str(frozen) + "/", args.server + ":" + remote + "/"],
        check=True,
    )
    subprocess.run(
        ["rsync", "-az", str(batch), args.server + ":" + remote + "/"], check=True
    )
    provenance_flags = " ".join(
        shlex.quote(value)
        for value in (
            "--project",
            args.project,
            "--network",
            args.network,
            "--processor-image",
            args.processor_image,
            "--web-image",
            args.web_image,
        )
    )
    subprocess.run(
        [
            "ssh",
            args.server,
            "cd "
            + shlex.quote(remote)
            + " && python3 provenance.py prepare "
            + provenance_flags,
        ],
        check=True,
    )
    provenance_check = "python3 provenance.py check " + provenance_flags
    wait_for_schema = "import subprocess,time\ncmd=['docker','compose','exec','-T','database','psql','-U','social_history','-d','social_history_staging','-Atqc',\"SELECT to_regclass('k_vars')\"]\nfor attempt in range(60):\n result=subprocess.run(cmd,capture_output=True,text=True)\n if result.returncode==0 and result.stdout.strip()=='k_vars': break\n time.sleep(1)\nelse: raise SystemExit('Native schema did not become ready')\n"
    ready_command = "python3 -c " + shlex.quote(wait_for_schema)
    flags = (
        " --project "
        + shlex.quote(args.project)
        + " --network "
        + shlex.quote(args.network)
        + " --allow-relationship-snapshot"
    )
    command = (
        "cd "
        + shlex.quote(remote)
        + " && docker compose config --quiet && docker compose up -d && "
        + ready_command
        + " && "
        + provenance_check
        + " > provenance-before.json"
        + " && python3 stage.py batch.jsonl"
        + flags
        + " > replay.log 2>&1 && python3 stage.py batch.jsonl"
        + flags
        + " > replay-rerun.log 2>&1 && python3 verify_stage.py batch.jsonl"
        + flags
        + " > api-verification.json"
        + " && "
        + provenance_check
        + " > provenance-after.json"
    )
    with (args.output / "remote-execution.log").open("a") as log:
        subprocess.run(
            ["ssh", args.server, command],
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
        )
    for name in (
        "replay.log",
        "replay-rerun.log",
        "api-verification.json",
        "provenance-before.json",
        "provenance-after.json",
    ):
        subprocess.run(
            [
                "rsync",
                "-az",
                args.server + ":" + remote + "/" + name,
                str(args.output / name),
            ],
            check=True,
        )
    print((args.output / "api-verification.json").read_text(), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("content", "relationships", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    for name in (
        "source",
        "archive",
        "network",
        "server",
        "project",
        "processor-image",
        "web-image",
    ):
        p.add_argument("--" + name, required=True)
    a = p.parse_args()
    os.umask(0o077)
    a.output.mkdir(parents=True, exist_ok=True)
    with (a.output / "watcher.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (a.output / "watcher.pid").write_text(str(os.getpid()))
        run(a)
