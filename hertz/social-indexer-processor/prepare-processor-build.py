#!/usr/bin/env python3
"""Prepare only clean pinned processor source, reviewed patch/tests and real lock."""
import argparse
import hashlib
import json
import pathlib
import subprocess
import tomllib

HERE = pathlib.Path(__file__).resolve().parent
SOURCE = HERE.parent.parent / "vendor/k-indexer"
PIN = "917863f49623aa6802e5ad4c0e246581470d93cb"


def read_regular(path):
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"Missing regular source file: {path.name}")
    return path.read_bytes()


def tracked_source(source):
    revision = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True, timeout=10
    ).strip()
    if revision != PIN:
        raise ValueError("Review the processor patch against the new upstream pin")
    changed = subprocess.check_output(
        ["git", "-C", str(source), "status", "--porcelain", "--untracked-files=no"],
        text=True,
        timeout=10,
    )
    if changed:
        raise ValueError("Pinned upstream tracked source has local changes")
    # Untracked or ignored files never enter the Docker source context.
    names = subprocess.check_output(
        ["git", "-C", str(source), "ls-files", "-z", "--", "Cargo.toml",
         "K-transaction-processor/Cargo.toml", "K-transaction-processor/src"],
        timeout=10,
    ).decode().split("\0")
    files = {}
    for name in filter(None, names):
        path = pathlib.PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("Unsafe tracked source path")
        files[name] = read_regular(source / name)
    if not {"Cargo.toml", "K-transaction-processor/Cargo.toml",
            "K-transaction-processor/src/k_protocol.rs"}.issubset(files):
        raise ValueError("Pinned processor source shape differs")
    return files


def prepare(source=SOURCE, destination=HERE / ".processor-build", lock=HERE / "Cargo.lock"):
    source = pathlib.Path(source).resolve()
    destination = pathlib.Path(destination).absolute()
    lock = pathlib.Path(lock)
    if destination.exists() or destination.is_symlink():
        raise ValueError("Occupied processor build context; preserve it")
    # Fail before creating a context until the real accepted build lock is present.
    lock_bytes = read_regular(lock)
    parsed_lock = tomllib.loads(lock_bytes.decode())
    if not any(p.get("name") == "K-transaction-processor"
               for p in parsed_lock.get("package", [])):
        raise ValueError("Resolved processor dependency lock required")
    files = tracked_source(source)
    files["Cargo.lock"] = lock_bytes
    files["Dockerfile"] = read_regular(HERE / "Dockerfile.processor")
    files["monotonic-profiles.patch"] = read_regular(HERE / "patches/monotonic-profiles.patch")
    for name in ("profile_guard_tests.rs", "signed-profile-fixture.json"):
        files["K-transaction-processor/src/" + name] = read_regular(HERE / name)
    files[".dockerignore"] = (
        "**\n!Dockerfile\n!Cargo.toml\n!Cargo.lock\n!monotonic-profiles.patch\n"
        "!K-transaction-processor/\n!K-transaction-processor/Cargo.toml\n"
        "!K-transaction-processor/src/\n!K-transaction-processor/src/**\n"
    ).encode()
    destination.mkdir(mode=0o700)
    for name, data in files.items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(data)
    binding = {
        "upstream": PIN,
        "cargo_lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
        "files": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())},
    }
    with (destination / "source-binding.json").open("x") as stream:
        json.dump(binding, stream, sort_keys=True, indent=2)
        stream.write("\n")
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=pathlib.Path, default=SOURCE)
    parser.add_argument("--destination", type=pathlib.Path, default=HERE / ".processor-build")
    parser.add_argument("--lock", type=pathlib.Path, default=HERE / "Cargo.lock")
    args = parser.parse_args()
    print(prepare(args.upstream, args.destination, args.lock))
