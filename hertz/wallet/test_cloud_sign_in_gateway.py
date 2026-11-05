"""Exercise the real Caddy routes with loopback-only public-data fixtures.

Run: python3 -m unittest discover -s hertz/wallet -p test_cloud_sign_in_gateway.py
Requires the installed Caddy CLI; no real backend, credential or provider used.
"""

import http.client
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest


SOURCE = Path(__file__).resolve().parent
FORGED = {
    "Authorization": "Bearer fixture-forged",
    "Cookie": "fixture-forged",
    "X-API-Key": "fixture-forged",
    "X-User-Id": "fixture-admin",
    "X-QA-Explore-NFT-Mode": "all",
}


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Echo(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"path": self.path, "headers": dict(self.headers)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "max-age=3600")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class Gateway:
    def __init__(self, site):
        self.temp = tempfile.TemporaryDirectory(prefix="kasanova-cloud-gateway-")
        self.root = Path(self.temp.name)
        self.process = None
        self.echo = None
        try:
            binary = shutil.which("caddy")
            if binary is None:
                raise RuntimeError("Caddy CLI is required for gateway verification")
            self.echo = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Echo)
            self.thread = threading.Thread(target=self.echo.serve_forever, daemon=True)
            self.thread.start()
            self.edge_port = free_port()
            self.wallet_port = free_port()
            (self.root / "index.html").write_text("fixture-wallet-spa")
            edge = (
                (SOURCE / "edge-route.Caddyfile")
                .read_text()
                .replace("{args[0]}:8080", f"{{args[0]}}:{self.wallet_port}")
            )
            site = (SOURCE / site).read_text()
            prefix = "{\n    admin off\n    auto_https off\n}\n"
            if not site.startswith(prefix):
                raise RuntimeError("Unexpected site global-options block")
            site = site.removeprefix(prefix)
            site = site.replace(":8080 {", f"http://127.0.0.1:{self.wallet_port} {{")
            site = site.replace("root * /srv", f"root * {self.root}")
            replacements = {
                "PUBLIC_UPSTREAM": f"127.0.0.1:{self.echo.server_port}",
                "PUBLIC_API_HOST": "fixture-api.invalid",
                "PUBLIC_CONFIG_API_KEY": "fixture-environment-key",
                "PLATFORM_APP_VERSION": "0.9.0",
            }
            for key, value in replacements.items():
                site = site.replace("{$" + key + "}", value)
            configuration = (
                prefix + "\n" + edge + "\n" + site + "\n"
                f"http://127.0.0.1:{self.edge_port} {{\n"
                "    import kasanova-wallet-path 127.0.0.1\n"
                '    handle {\n        respond "fixture-root-fallback" 200\n    }\n}\n'
            )
            config = self.root / "Caddyfile"
            config.write_text(configuration)
            self.log = (self.root / "caddy.log").open("w")
            environment = {
                key: os.environ[key]
                for key in ("PATH", "HOME", "TMPDIR", "LANG")
                if key in os.environ
            }
            environment.update(
                XDG_CONFIG_HOME=str(self.root / "config"),
                XDG_DATA_HOME=str(self.root / "data"),
                GOMAXPROCS="2",
                GOMEMLIMIT="128MiB",
            )
            self.process = subprocess.Popen(
                [binary, "run", "--config", str(config), "--adapter", "caddyfile"],
                env=environment,
                stdout=self.log,
                stderr=subprocess.STDOUT,
            )
            for _ in range(100):
                if self.process.poll() is not None:
                    raise RuntimeError((self.root / "caddy.log").read_text())
                try:
                    if self.request("GET", "/wallet/regular-route")[0] == 200:
                        break
                except OSError:
                    pass
                time.sleep(0.05)
            else:
                raise RuntimeError("Fixture Caddy did not become ready")
        except BaseException:
            self.close()
            raise

    def request(self, method, path, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.edge_port, timeout=3)
        try:
            connection.request(method, path, headers=headers or {})
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def close(self):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.echo is not None:
            self.echo.shutdown()
            self.echo.server_close()
            self.thread.join(timeout=2)
        if hasattr(self, "log"):
            self.log.close()
        self.temp.cleanup()


class CloudGatewayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gateways = {}
        try:
            for name, site in (
                ("prod", "site.Caddyfile"),
                ("dev", "dev-site.Caddyfile"),
            ):
                cls.gateways[name] = Gateway(site)
        except BaseException:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        for gateway in cls.gateways.values():
            gateway.close()

    def test_metadata_uses_fixed_public_path_without_any_api_credentials(self):
        for environment, gateway in self.gateways.items():
            with self.subTest(environment=environment):
                status, headers, body = gateway.request(
                    "GET",
                    "/.well-known/assetlinks.json?user_id=admin&path=/api/v1/admin",
                    FORGED,
                )
                self.assertEqual(status, 200)
                value = json.loads(body)
                self.assertEqual(value["path"], "/.well-known/assetlinks.json")
                upstream = {key.lower(): item for key, item in value["headers"].items()}
                self.assertEqual(upstream["host"], "fixture-api.invalid")
                for key in (
                    "authorization",
                    "cookie",
                    "x-api-key",
                    "x-user-id",
                    "x-qa-explore-nft-mode",
                ):
                    self.assertNotIn(key, upstream)
                self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
                self.assertEqual(headers["Cache-Control"], "no-cache")

    def test_metadata_never_becomes_a_write_or_generic_proxy(self):
        for environment, gateway in self.gateways.items():
            for method in ("HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
                with self.subTest(environment=environment, method=method):
                    self.assertEqual(
                        gateway.request(method, "/.well-known/assetlinks.json", FORGED)[
                            0
                        ],
                        404,
                    )
            for path in (
                "/.well-known/assetlinks.json/extra",
                "/.well-known/other.json",
            ):
                with self.subTest(environment=environment, path=path):
                    self.assertEqual(
                        gateway.request("GET", path, FORGED)[2],
                        b"fixture-root-fallback",
                    )

    def test_existing_environment_key_is_only_used_for_existing_environment_route(self):
        for environment, gateway in self.gateways.items():
            with self.subTest(environment=environment):
                status, _headers, body = gateway.request(
                    "GET", "/wallet/backend/api/v1/environment?user_id=admin", FORGED
                )
                self.assertEqual(status, 200)
                value = json.loads(body)
                self.assertEqual(value["path"], "/api/v1/environment")
                upstream = {key.lower(): item for key, item in value["headers"].items()}
                self.assertEqual(upstream["x-api-key"], "fixture-environment-key")
                self.assertEqual(upstream["x-platform"], "web")
                for key in (
                    "authorization",
                    "cookie",
                    "x-user-id",
                    "x-qa-explore-nft-mode",
                ):
                    self.assertNotIn(key, upstream)

    def test_existing_backend_permissions_and_dev_read_scope_are_preserved(self):
        for environment, gateway in self.gateways.items():
            for path in (
                "admin",
                "wallets",
                "vault-backups",
                "provider-recovery",
                "fee-service/kns",
            ):
                with self.subTest(environment=environment, path=path):
                    self.assertEqual(
                        gateway.request(
                            "GET", "/wallet/backend/api/v1/" + path, FORGED
                        )[0],
                        404,
                    )
            self.assertEqual(
                gateway.request("POST", "/wallet/backend/api/v1/environment", FORGED)[
                    0
                ],
                404,
            )
            status, _headers, body = gateway.request(
                "GET",
                "/wallet/backend/api/v1/tokens/krc20/search?q=TESTER&limit=20",
                FORGED,
            )
            self.assertEqual(status, 200 if environment == "dev" else 404)
            if environment == "dev":
                self.assertEqual(
                    json.loads(body)["path"],
                    "/api/v1/tokens/krc20/search?q=TESTER&limit=20",
                )
                self.assertEqual(
                    gateway.request(
                        "GET",
                        "/wallet/backend/api/v1/tokens/krc20/search?q=TESTER&user_id=admin",
                        FORGED,
                    )[0],
                    404,
                )

    def test_wallet_redirect_spa_and_asset_behavior_is_preserved(self):
        for gateway in self.gateways.values():
            status, headers, _body = gateway.request("GET", "/wallet")
            self.assertEqual(status, 308)
            self.assertEqual(headers["Location"], "/wallet/")
            self.assertEqual(
                gateway.request("GET", "/wallet/regular-route")[2],
                b"fixture-wallet-spa",
            )
            self.assertEqual(gateway.request("GET", "/wallet/missing.js")[0], 404)

    def test_provider_csp_keeps_exact_script_and_stylesheet_sources(self):
        for gateway in self.gateways.values():
            _status, headers, _body = gateway.request("GET", "/wallet/")
            policy = dict(
                (part.strip().split()[0], part.strip().split()[1:])
                for part in headers["Content-Security-Policy"].split(";")
                if part.strip()
            )
            self.assertEqual(
                set(policy["script-src"]),
                {
                    "'self'",
                    "'unsafe-eval'",
                    "https://browser.sentry-cdn.com",
                    "https://accounts.google.com/gsi/client",
                    "https://cdn.apple-cloudkit.com/ck/2/cloudkit.js",
                },
            )
            self.assertEqual(
                set(policy["style-src"]),
                {"'self'", "'unsafe-inline'", "https://accounts.google.com/gsi/style"},
            )
            self.assertEqual(policy["frame-ancestors"], ["'none'"])
            self.assertIn(
                "publickey-credentials-get=(self)", headers["Permissions-Policy"]
            )


if __name__ == "__main__":
    unittest.main()
