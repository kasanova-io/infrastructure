"""Verify content negotiation, cache validation and rollover with the actual gateway."""

import gzip
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from prepare_static_assets import prepare
from test_cloud_sign_in_gateway import Gateway


class StaticAssetsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name) / "input"
        self.source.mkdir()
        (self.source / "flutter_bootstrap.js").write_bytes(b"bootstrap")
        self.gateway = Gateway("dev-site.Caddyfile")
        self.addCleanup(self.gateway.close)

    def activate_fixture(self, release, data):
        (self.source / "main.dart.js").write_bytes(data)
        output = Path(self.temp.name) / release
        prepare(self.source, output)
        for path in output.iterdir():
            target = self.gateway.root / path.name
            shutil.copyfile(path, target)
            os.utime(target, (0, 0))  # The deterministic wallet archive uses the Unix epoch.

    def test_revalidation_negotiation_and_equal_length_release_change(self):
        old = b"fixture_old_wallet();" * 300
        new = old.replace(b"old", b"new")
        tags = {}
        for release, expected in (("first", old), ("second", new)):
            self.activate_fixture(release, expected)
            for encoding in ("identity", "gzip", "br"):
                with self.subTest(release=release, encoding=encoding):
                    request = {"Accept-Encoding": encoding}
                    if encoding in tags:
                        request["If-None-Match"] = tags[encoding]
                    status, headers, body = self.gateway.request("GET", "/wallet/main.dart.js", request)
                    self.assertEqual(status, 200)
                    self.assertEqual(headers["Content-Length"], str(len(body)))
                    self.assertEqual(headers.get("Content-Encoding", "identity"), encoding)
                    self.assertIn("Accept-Encoding", headers["Vary"])
                    self.assertIn("no-cache", headers["Cache-Control"])
                    self.assertEqual(headers["Etag"], f'"{hashlib.sha256(body).hexdigest()}"')
                    if encoding == "br":
                        body = subprocess.run(["brotli", "-dc"], input=body, capture_output=True, check=True).stdout
                    elif encoding == "gzip":
                        body = gzip.decompress(body)
                    self.assertEqual(body, expected)
                    tags[encoding] = headers["Etag"]
                    request["If-None-Match"] = tags[encoding]
                    status, _, body = self.gateway.request("GET", "/wallet/main.dart.js", request)
                    self.assertEqual((status, body), (304, b""))
            self.assertEqual(len(set(tags.values())), 3)
        _, headers, _ = self.gateway.request("GET", "/wallet/main.dart.js", {"Accept-Encoding": "gzip, deflate, br, zstd"})
        self.assertEqual(headers["Content-Encoding"], "br")
        for suffix in (".etag", ".br", ".gz", ".br.etag"):
            self.assertEqual(self.gateway.request("GET", "/wallet/main.dart.js" + suffix)[0], 404)

    def test_existing_build_without_sidecars_and_public_api_cache_policy(self):
        data = b"existing wallet file" * 100
        (self.gateway.root / "main.dart.js").write_bytes(data)
        status, _, body = self.gateway.request("GET", "/wallet/main.dart.js", {"Accept-Encoding": "gzip"})
        self.assertEqual((status, gzip.decompress(body)), (200, data))
        status, headers, _ = self.gateway.request("GET", "/wallet/backend/api/v1/environment")
        self.assertEqual(status, 200)
        self.assertIn("no-store", headers["Cache-Control"])
