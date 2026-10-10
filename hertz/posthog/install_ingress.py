#!/usr/bin/env python3
"""Append the reviewed PostHog route, validate all routes, and reload in place."""
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent
LIVE = Path('/opt/caddy/config/Caddyfile')
BLOCK = (ROOT / 'Caddyfile').read_bytes()
STAGED_CONTAINER = '/tmp/kasanova-posthog-Caddyfile'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    operations = ROOT / 'operations'
    operations.mkdir(mode=0o700, exist_ok=True)
    with Path('/opt/caddy/config/.posthog-install.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        before = LIVE.read_bytes()
        if b'posthog.kasanova.io' in before:
            if BLOCK not in before:
                raise RuntimeError('Existing PostHog route differs; preserve it')
            print(json.dumps({'ingress': 'already configured'}))
            return
        candidate = before + b'\n' + BLOCK
        backup = operations / 'Caddyfile.before-posthog'
        if backup.exists():
            raise RuntimeError('Existing original gateway backup must not be overwritten')
        backup.write_bytes(before)
        backup.chmod(0o600)
        staged = operations / 'Caddyfile.candidate'
        staged.write_bytes(candidate)
        staged.chmod(0o600)
        subprocess.run(['docker', 'cp', str(staged), 'caddy:' + STAGED_CONTAINER], check=True, capture_output=True)
        check = subprocess.run(['docker', 'exec', 'caddy', 'caddy', 'validate', '--adapter', 'caddyfile', '--config', STAGED_CONTAINER], capture_output=True)
        if check.returncode:
            raise RuntimeError('Complete Caddy configuration validation failed: ' + check.stderr.decode()[-1000:])
        if LIVE.read_bytes() != before:
            raise RuntimeError('Gateway changed concurrently; preserve newer configuration')
        # The gateway mounts this file, so retain its inode rather than renaming it.
        LIVE.write_bytes(candidate)
        reload = subprocess.run(['docker', 'exec', 'caddy', 'caddy', 'reload', '--adapter', 'caddyfile', '--config', '/etc/caddy/Caddyfile'], capture_output=True)
        if reload.returncode:
            if LIVE.read_bytes() == candidate:
                LIVE.write_bytes(before)
                subprocess.run(['docker', 'exec', 'caddy', 'caddy', 'reload', '--config', '/etc/caddy/Caddyfile'], check=True, capture_output=True)
            raise RuntimeError('Gateway reload failed; original configuration restored when safe')
        report = {'ingress': 'installed', 'hostname': 'posthog.kasanova.io',
                  'original_sha256': digest(before), 'installed_sha256': digest(candidate),
                  'original_routes_preserved_byte_for_byte': candidate[:-len(BLOCK)-1] == before,
                  'full_configuration_validated': True, 'hot_reload_successful': True}
        (operations / 'ingress-verification.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report))


if __name__ == '__main__':
    main()
