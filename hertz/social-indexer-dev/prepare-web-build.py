#!/usr/bin/env python3
"""Prepare only pinned web source and the reviewed compatibility patch (no env files)."""
import pathlib
import shutil
import subprocess

HERE = pathlib.Path(__file__).resolve().parent
SOURCE = HERE.parent.parent / "vendor/k-indexer"
PIN = "917863f49623aa6802e5ad4c0e246581470d93cb"


def prepare():
    actual = subprocess.check_output(["git", "-C", str(SOURCE), "rev-parse", "HEAD"], text=True).strip()
    if actual != PIN:
        raise SystemExit(f"Review compatibility patch against new upstream pin: {actual}")
    changed = subprocess.check_output(["git", "-C", str(SOURCE), "status", "--porcelain", "--untracked-files=no"], text=True)
    if changed:
        raise SystemExit("Pinned upstream source has local changes; refusing build")
    destination = HERE / ".web-build"
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir()
    for relative in ["Cargo.toml", "K-webserver/Cargo.toml"]:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(SOURCE / relative, target)
    target = destination / "K-webserver/src"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(SOURCE / "K-webserver/src", target)
    shutil.copy2(HERE / "Dockerfile.web", destination / "Dockerfile")
    for patch in ["historical-author-keys.patch", "exact-vote-details.patch"]:
        shutil.copy2(HERE / "patches" / patch, destination / patch)
    (destination / ".dockerignore").write_text("**\n!Dockerfile\n!Cargo.toml\n!K-webserver/\n!K-webserver/Cargo.toml\n!K-webserver/src/\n!K-webserver/src/**\n!historical-author-keys.patch\n!exact-vote-details.patch\n")
    print(destination)


if __name__ == "__main__":
    prepare()
