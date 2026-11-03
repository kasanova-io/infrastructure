#!/usr/bin/env python3
"""Cold-copy a native Chats store, then recover the unchanged original writer.

Run preflight first. Snapshot execution needs a coordinated writer-stop window;
it never changes routing, deployment configuration, or the source volume. The
helper opens files only, never Fjall. Keep this command running independently of
the SSH transport and retain its receipt, helper logs, and backup on the host.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time
import uuid


COUNTERS = (
    "handshakes_by_sender", "uniq_handshakes_by_receiver",
    "payments_by_sender", "uniq_payments_by_receiver", "contextual_messages",
    "group_messages", "group_controls", "push_registered_devices",
)
MAX_BYTES = 32 * 1024**3
MAX_FILES = 32768

# The source is mounted read-only. Full inventory stays in this helper's logs.
# Three hash passes prove that the stopped source and copied files agree. Do not
# substitute this with a live copy: WAL/manifest consistency requires quiescence.
COPY = r'''
import hashlib,json,os,shutil,stat
from pathlib import Path
MAX_BYTES=32*1024**3
MAX_FILES=32768
def inventory(root):
 rows=[];size=0
 root_stat=root.lstat()
 assert stat.S_ISDIR(root_stat.st_mode)
 rows.append({'path':'.','mode':stat.S_IMODE(root_stat.st_mode),'uid':root_stat.st_uid,'gid':root_stat.st_gid,'size':0,'sha256':None})
 for p in sorted(root.rglob('*')):
  assert len(rows)<MAX_FILES,'file count bound'
  s=p.lstat()
  assert stat.S_ISREG(s.st_mode) or stat.S_ISDIR(s.st_mode),'special path'
  length=s.st_size if stat.S_ISREG(s.st_mode) else 0
  size+=length
  assert size<=MAX_BYTES,'byte bound'
  with p.open('rb') if stat.S_ISREG(s.st_mode) else __import__('contextlib').nullcontext() as f:
   digest=hashlib.file_digest(f,'sha256').hexdigest() if f else None
  rows.append({'path':str(p.relative_to(root)),'mode':stat.S_IMODE(s.st_mode),'uid':s.st_uid,'gid':s.st_gid,'size':length,'sha256':digest})
 return rows
source=Path('/from');target=Path('/to')
assert not list(target.iterdir()),'fresh empty target required'
before=inventory(source)
assert len(before)>1 and sum(r['size'] for r in before)>0,'empty source'
shutil.copytree(source,target,dirs_exist_ok=True)
for row in reversed(before):
 p=target/row['path']
 os.chown(p,row['uid'],row['gid'])
 os.chmod(p,row['mode'])
 if p.is_file():
  with p.open('rb') as f:os.fsync(f.fileno())
 else:
  fd=os.open(p,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(fd)
  finally:os.close(fd)
after=inventory(source);copied=inventory(target)
assert before==after==copied,'source changed or copy mismatch'
print(json.dumps({'inventory':before,'inventory_sha256':hashlib.sha256(json.dumps(before,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'bytes':sum(r['size'] for r in before),'entries':len(before)},sort_keys=True),flush=True)
'''


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def command(args, timeout=30):
    completed = subprocess.run(args, capture_output=True, timeout=timeout)
    if completed.returncode:
        # Docker inspection can contain private env; do not echo its stdout or
        # stderr into the public receipt or exception.
        raise RuntimeError(f"{args[0]} command failed ({completed.returncode})")
    return completed.stdout


def inspect(identity):
    return json.loads(command(["docker", "inspect", identity]))[0]


def configuration(container):
    value = {key: container[key] for key in
             ("Id", "Name", "Image", "Config", "HostConfig", "Mounts")}
    # Docker may enumerate equivalent runtime mounts in a different order after
    # a restart. Retain every field and duplicate, but normalize that inventory.
    value["Mounts"] = sorted(value["Mounts"], key=canonical)
    return digest(value)


def writers(volume):
    return command(["docker", "ps", "-q", "--no-trunc", "--filter",
                    "volume=" + volume]).decode().splitlines()


def bind(container, expected):
    if container["Id"] != expected["id"] or container["Image"] != expected["image"]:
        raise ValueError("source container/image changed")
    data = [m for m in container["Mounts"] if m["Destination"] == "/app/data"]
    if len(data) != 1 or data[0]["Type"] != "volume" or not data[0]["RW"] or \
            data[0]["Name"] != expected["volume"]:
        raise ValueError("source data volume changed")
    if expected.get("configuration_sha256") and \
            configuration(container) != expected["configuration_sha256"]:
        raise ValueError("source deployment configuration changed")
    if container["Config"].get("StopSignal") != "SIGINT":
        raise ValueError("source does not use the established graceful stop")


def healthy(container):
    return container["State"]["Running"] and \
        container["State"].get("Health", {}).get("Status") == "healthy"


def bind_helper(container, expected):
    mounts = sorted((m["Type"], m.get("Name"), m["Destination"], m["RW"])
                    for m in container["Mounts"])
    if container["Id"] != expected["id"] or container["Image"] != expected["image"] or \
            container["Config"].get("Labels", {}).get("io.kasanova.run") != expected["run_id"] or \
            container["HostConfig"]["NetworkMode"] != "none" or \
            not container["HostConfig"]["ReadonlyRootfs"] or mounts != sorted([
                ("volume", expected["source_volume"], "/from", False),
                ("volume", expected["backup_volume"], "/to", True)]):
        raise ValueError("helper ownership/isolation/mounts changed")
    if expected.get("configuration_sha256") and \
            configuration(container) != expected["configuration_sha256"]:
        raise ValueError("helper deployment configuration changed")


def metrics(identity):
    value = json.loads(command(["docker", "exec", identity, "wget", "-qO-",
                               "http://127.0.0.1:8080/metrics"], timeout=15))
    for name in (*COUNTERS, "blocks_processed"):
        if type(value.get(name)) is not int or value[name] < 0:
            raise ValueError("missing native metric: " + name)
    return {name: value[name] for name in (*COUNTERS, "blocks_processed")}


def preflight(identity, image, volume, utility_image):
    expected = {"id": identity, "image": image, "volume": volume}
    container = inspect(identity)
    bind(container, expected)
    if not healthy(container) or writers(volume) != [identity]:
        raise ValueError("healthy sole source writer required")
    utility = json.loads(command(["docker", "image", "inspect", utility_image]))[0]
    if utility["Id"] != utility_image:
        raise ValueError("immutable local helper image required")
    size = int(command(["docker", "exec", identity, "du", "-sk", "/app/data"]).split()[0]) * 1024
    entries = int(command(["docker", "exec", identity, "sh", "-c",
                           "find /app/data -print | wc -l"]).strip())
    filesystem = command(["docker", "exec", identity, "df", "-Pk", "/app/data"]).splitlines()
    free = int(filesystem[-1].split()[3]) * 1024
    if not 0 < size <= MAX_BYTES or not 1 < entries < MAX_FILES:
        raise ValueError("source exceeds reviewed copy bounds")
    if free < 2 * MAX_BYTES:
        raise ValueError("space required for backup and separate working clone")
    return {
        "schema": "chats-cold-snapshot-preflight-v1", **expected,
        "configuration_sha256": configuration(container),
        "utility_image": utility_image, "recorded_at": int(time.time()),
        "source_started_at": container["State"]["StartedAt"],
        "live_allocated_bytes": size, "live_entries": entries,
        "filesystem_free_bytes": free, "metrics": metrics(identity),
        "max_bytes": MAX_BYTES, "max_entries": MAX_FILES,
        "source_changed": False, "snapshot_created": False,
    }


def write(path, value, fresh=False):
    if fresh and path.exists():
        raise ValueError("receipt already exists")
    fd, temporary_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        if fresh:
            # Linking publishes without replacing a concurrently created file.
            os.link(temporary, path)
        else:
            os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def recover(expected, baseline, previous_start, timeout, readonly_helper=None, *, start_if_stopped=True):
    """Same original container; no image/config/volume replacement or rollback."""
    deadline = time.monotonic() + timeout
    last_error = None
    live_sample = None
    while time.monotonic() < deadline:
        try:
            source = inspect(expected["id"])
            bind(source, expected)
            # Never start a second writer; a foreign attachment is an actual
            # ownership conflict, not a condition this tool may repair.
            attached = writers(expected["volume"])
            for identity in attached:
                if identity == expected["id"]:
                    continue
                if not readonly_helper or identity != readonly_helper["id"]:
                    raise ValueError("foreign source-volume attachment during recovery")
                reader = inspect(identity)
                bind_helper(reader, readonly_helper)
            if not source["State"]["Running"]:
                live_sample = None
                if not start_if_stopped:
                    raise ValueError("read-only recovery verification never starts a writer")
                command(["docker", "start", expected["id"]])
            elif healthy(source) and source["State"]["StartedAt"] != previous_start:
                after = metrics(expected["id"])
                current = (source["State"]["StartedAt"], after["blocks_processed"])
                if all(after[name] >= baseline[name] for name in COUNTERS):
                    # blocks_processed is initialized from the retained/pruned
                    # header partition at startup. It is not a durable lifetime
                    # counter. Require advancement within THIS unchanged new
                    # process, never a comparison with its predecessor's uptime.
                    if live_sample and live_sample[0] == current[0] and current[1] > live_sample[1]:
                        return after
                    live_sample = current
                else:
                    live_sample = None
            else:
                live_sample = None
        except ValueError:
            raise
        except (RuntimeError, subprocess.TimeoutExpired) as exc:
            last_error = type(exc).__name__
        time.sleep(2)
    raise TimeoutError("unchanged source recovery not proven: " + str(last_error))


def snapshot(expected, volume, helper, receipt, stop_timeout=180,
             copy_timeout=1800, recovery_timeout=3600):
    """Called only after the public DEV owner grants a writer-stop slot."""
    if not re.fullmatch(r"kasanova_chats_cold_[a-z0-9_]{1,48}", volume):
        raise ValueError("new task-owned backup volume name required")
    if not re.fullmatch(r"kasanova-chats-cold-[a-z0-9-]{1,48}", helper):
        raise ValueError("new task-owned helper name required")
    if expected.get("schema") != "chats-cold-snapshot-preflight-v1" or \
            expected.get("max_bytes") != MAX_BYTES or \
            expected.get("max_entries") != MAX_FILES:
        raise ValueError("unsupported preflight")
    fresh = preflight(expected["id"], expected["image"], expected["volume"],
                      expected["utility_image"])
    if fresh["configuration_sha256"] != expected["configuration_sha256"]:
        raise ValueError("source changed since reviewed preflight")
    for kind, name in (("volume", volume), ("container", helper)):
        probe = subprocess.run(["docker", kind, "inspect", name], capture_output=True)
        if probe.returncode == 0:
            raise ValueError("snapshot resource already exists")
        # Absence must be distinguished from Docker transport/daemon failure.
        command(["docker", "info", "--format", "{{.ServerVersion}}"])
    result = {"schema": "chats-cold-snapshot-v1", "status": "RUNNING",
              "preflight": fresh, "backup_volume": volume, "helper": helper,
              "routing_changed": False, "source_configuration_changed": False,
              "source_recovered": False, "backup_complete": False}
    run_id = str(uuid.uuid4())
    result["run_id"] = run_id
    write(receipt, result, fresh=True)
    signal_started = False
    stop_attempted = False
    helper_created = False
    helper_identity = None
    failure = None
    old_handlers = {}

    def interrupted(signum, _frame):
        nonlocal signal_started
        if not signal_started:
            signal_started = True
            raise InterruptedError("snapshot interrupted; recovering source")

    try:
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            old_handlers[sig] = signal.signal(sig, interrupted)
        command(["docker", "volume", "create", "--label",
                 "io.kasanova.purpose=chats-cold-backup", "--label",
                 "io.kasanova.run=" + run_id, volume])
        created_volume = json.loads(command(["docker", "volume", "inspect", volume]))[0]
        if created_volume.get("Labels", {}).get("io.kasanova.run") != run_id:
            raise ValueError("backup volume was not created by this run")
        created_id = command(["docker", "create", "--name", helper, "--label",
                 "io.kasanova.run=" + run_id, "--network", "none",
                 "--read-only", "--user", "0:0", "--memory", "256m",
                 "--cpus", "1", "--pids-limit", "64", "--cap-drop", "ALL",
                 "--cap-add", "CHOWN", "--cap-add", "DAC_OVERRIDE",
                 "--cap-add", "FOWNER", "--security-opt", "no-new-privileges:true",
                 "--mount", "type=volume,source=" + expected["volume"] + ",target=/from,readonly",
                 "--mount", "type=volume,source=" + volume + ",target=/to",
                 "--entrypoint", "python", expected["utility_image"], "-B", "-c", COPY]).decode().strip()
        if not re.fullmatch(r"[a-f0-9]{64}", created_id):
            raise ValueError("Docker did not return an immutable helper identity")
        helper_created = True
        helper_identity = {"id": created_id, "image": expected["utility_image"],
                           "run_id": run_id, "source_volume": expected["volume"],
                           "backup_volume": volume}
        created_helper = inspect(created_id)
        bind_helper(created_helper, helper_identity)
        helper_identity["configuration_sha256"] = configuration(created_helper)
        result["helper_id"] = helper_identity["id"]
        source = inspect(expected["id"])
        bind(source, expected)
        if not healthy(source) or writers(expected["volume"]) != [expected["id"]]:
            raise ValueError("source ownership/health changed before stop")
        result["metrics_before_stop"] = metrics(expected["id"])
        write(receipt, result)
        # Register the recovery obligation before sending the signal. No forced
        # SIGKILL fallback: a slow shutdown leaves snapshot incomplete.
        stop_attempted = True
        # Docker stop marks the writer manually stopped, suppressing its restart
        # policy while the copy runs. The configured signal is already SIGINT.
        # An infinite daemon timeout prevents an implicit SIGKILL; only the
        # local CLI observation is bounded, with recovery still owed on timeout.
        command(["docker", "stop", "--timeout", "-1", expected["id"]], timeout=stop_timeout)
        source = inspect(expected["id"])
        bind(source, expected)
        if source["State"]["ExitCode"] != 0 or writers(expected["volume"]):
            raise ValueError("clean sole-writer shutdown not proven")
        result["source_stop_exit"] = 0
        result["source_stopped_at"] = int(time.time())
        write(receipt, result)
        bind_helper(inspect(helper_identity["id"]), helper_identity)
        command(["docker", "start", "--attach", helper_identity["id"]], timeout=copy_timeout)
        copied = inspect(helper_identity["id"])
        bind_helper(copied, helper_identity)
        if copied["State"]["Running"] or copied["State"]["ExitCode"] != 0 or \
                writers(expected["volume"]):
            raise ValueError("copy completion or continued quiescence failed")
        data = json.loads(command(["docker", "logs", helper_identity["id"]], timeout=30))
        result["copy"] = {key: data[key] for key in ("inventory_sha256", "bytes", "entries")}
        result["backup_complete"] = True
    except BaseException as exc:
        failure = exc
        result.update(status="FAIL", error_type=type(exc).__name__, error=str(exc))
    finally:
        # A second terminal signal must not interrupt source recovery.
        for sig in old_handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            if helper_created and helper_identity:
                current_helper = inspect(helper_identity["id"])
                bind_helper(current_helper, helper_identity)
                if current_helper["State"]["Running"]:
                    command(["docker", "stop", "--time", "30", helper_identity["id"]], timeout=45)
        except BaseException as exc:
            result.update(status="FAIL", helper_cleanup_error_type=type(exc).__name__)
            failure = failure or exc
        try:
            if stop_attempted:
                result["recovery_metrics"] = recover(
                    expected, result["metrics_before_stop"],
                    fresh["source_started_at"], recovery_timeout, helper_identity)
                result["source_recovered"] = True
        except BaseException as exc:
            result.update(status="FAIL", recovery_error_type=type(exc).__name__,
                          recovery_error=str(exc))
            failure = failure or exc
        if failure is None:
            result["status"] = "PASS"
        result["finished_at"] = int(time.time())
        try:
            write(receipt, result)
        finally:
            for sig, handler in old_handlers.items():
                signal.signal(sig, handler)
    if failure:
        raise RuntimeError("snapshot failed; inspect host receipt and source recovery") from failure
    return result


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    scan = modes.add_parser("preflight", help="read-only; no snapshot or writer stop")
    for name in ("container", "image", "volume", "utility-image"):
        scan.add_argument("--" + name, required=True)
    scan.add_argument("--output", type=Path, required=True)
    execute = modes.add_parser("snapshot", help="requires a coordinated writer-stop window")
    execute.add_argument("--preflight", type=Path, required=True)
    execute.add_argument("--preflight-sha256", required=True)
    execute.add_argument("--backup-volume", required=True)
    execute.add_argument("--helper", required=True)
    execute.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "preflight":
        if not re.fullmatch(r"[a-f0-9]{64}", args.container) or any(
                not re.fullmatch(r"sha256:[a-f0-9]{64}", value)
                for value in (args.image, args.utility_image)):
            parser.error("full immutable container and image identities required")
        value = preflight(args.container, args.image, args.volume, args.utility_image)
        write(args.output, value, fresh=True)
        print(json.dumps(value, sort_keys=True))
    else:
        raw = args.preflight.read_bytes()
        if hashlib.sha256(raw).hexdigest() != args.preflight_sha256:
            parser.error("preflight artifact hash mismatch")
        expected = json.loads(raw)
        lockname = "/tmp/kasanova-chats-cold-" + hashlib.sha256(expected["volume"].encode()).hexdigest() + ".lock"
        fd = os.open(lockname, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            print(json.dumps(snapshot(expected, args.backup_volume, args.helper,
                                      args.receipt), sort_keys=True))
        finally:
            os.close(fd)


if __name__ == "__main__":
    main()
