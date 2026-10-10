#!/usr/bin/env python3
"""Build/test patched production source against an explicitly isolated database.

The caller owns PostgreSQL/toolchain lifecycle and captures stdout/stderr. This
runner never starts a server, reads deployment credentials, or deploys an image.
"""
import hashlib
import os
import re
import shutil
import subprocess

from test_web_compat import HERE, SOURCE, HistoricalAuthorCompatibility


def main():
    if not os.environ.get("SOCIAL_SEARCH_TEST_DATABASE_URL"):
        raise SystemExit("Set SOCIAL_SEARCH_TEST_DATABASE_URL to the isolated social_search_contract database")
    revision = subprocess.check_output(["git", "-C", str(SOURCE), "rev-parse", "HEAD"], text=True).strip()
    if revision != "917863f49623aa6802e5ad4c0e246581470d93cb":
        raise SystemExit("Review patches against the changed upstream revision")
    if subprocess.check_output(["git", "-C", str(SOURCE), "status", "--porcelain", "--untracked-files=no"], text=True):
        raise SystemExit("Upstream tracked source must remain clean")
    for name in ["historical-author-keys.patch", "exact-vote-details.patch", "content-search.patch"]:
        print(name, hashlib.sha256((HERE / "patches" / name).read_bytes()).hexdigest(), flush=True)
    fixture = HistoricalAuthorCompatibility
    try:
        fixture.setUpClass()
        manifest = fixture.root / "Cargo.toml"
        shutil.copy2(SOURCE / "Cargo.toml", manifest)
        manifest.write_text(re.sub(r"members = \[.*?\]", 'members = ["K-webserver"]', manifest.read_text(), flags=re.S))
        subprocess.run(["cargo", "test", "--bin", "K-webserver", "--features", "search-contract", "--", "--nocapture"], cwd=fixture.root, check=True)
        subprocess.run(["cargo", "build", "--release", "--bin", "K-webserver"], cwd=fixture.root, check=True)
    finally:
        if hasattr(fixture, "temp"):
            fixture.tearDownClass()


if __name__ == "__main__":
    main()
