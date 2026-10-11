#!/usr/bin/env python3
"""Prepare the pinned official hobby stack without modifying shared host packages."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import shutil
import subprocess
import tarfile
import urllib.request

ROOT = Path(__file__).resolve().parent
UPSTREAM_REVISION = '7f64c1ca669dabc45a3dde86e398d3ef84066290'
SOURCE_PREFIXES = ('docker/clickhouse/', 'docker/temporal/', 'docker/livestream/',
                   'docker/kafka/', 'docker/postgres-init-scripts/', 'posthog/idl/',
                   'posthog/user_scripts/', 'products/')


def write_private(path, value):
    path.write_text(value)
    path.chmod(0o600)


def main():
    os.umask(0o077)
    revision = os.environ.get('DEPLOYMENT_REVISION')
    if not revision:
        raise RuntimeError('DEPLOYMENT_REVISION is required')
    if shutil.disk_usage(ROOT).free < 10 * 1024**3:
        raise RuntimeError('Need 10 GiB free before preparing upstream source')
    env = ROOT / '.env'
    if not env.exists():
        values = {'DOMAIN': 'posthog.kasanova.io', 'REGISTRY_URL': 'posthog/posthog',
                  'POSTHOG_APP_TAG': 'latest', 'POSTHOG_NODE_TAG': 'latest',
                  'POSTHOG_SECRET': secrets.token_hex(32), 'ENCRYPTION_SALT_KEYS': secrets.token_hex(16),
                  'BROWSERLESS_SECRET': secrets.token_hex(32), 'POSTGRES_PASSWORD': secrets.token_hex(32),
                  'OBJECT_STORAGE_ACCESS_KEY_ID': 'kasanova_' + secrets.token_hex(12),
                  'OBJECT_STORAGE_SECRET_ACCESS_KEY': secrets.token_hex(32),
                  'SEAWEEDFS_DOCKER_NAME': 'kasanova_posthog_seaweedfs',
                  'OPT_OUT_CAPTURE': 'true', 'TLS_BLOCK': '', 'CADDY_TLS_BLOCK': '',
                  'CADDY_HOST': 'http://:80', 'DEPLOYMENT_REVISION': revision}
        write_private(env, ''.join(k + '=' + v + '\n' for k, v in values.items()))
    elif 'DEPLOYMENT_REVISION=' + revision not in env.read_text().splitlines():
        raise RuntimeError('Existing deployment identity differs; preserve configuration for explicit upgrade')
    outer = json.loads(subprocess.check_output(['docker', 'inspect', 'caddy']))[0]
    outer_ip = outer['NetworkSettings']['Networks']['caddy_caddy_net']['IPAddress']
    inner = subprocess.run(['docker', 'inspect', 'kasanova_posthog-proxy-1'], capture_output=True)
    trusted_ips = ['127.0.0.1', outer_ip]
    if inner.returncode == 0:
        trusted_ips.append(json.loads(inner.stdout)[0]['NetworkSettings']['Networks']['kasanova_posthog_default']['IPAddress'])
    settings = dict(line.split('=', 1) for line in env.read_text().splitlines() if '=' in line)
    settings.update(POSTHOG_OUTER_PROXY_IP=outer_ip, POSTHOG_TRUSTED_PROXIES=','.join(trusted_ips))
    write_private(env, ''.join(k + '=' + v + '\n' for k, v in settings.items()))
    replacements = {'postgres://posthog:posthog@db': 'postgres://posthog:${POSTGRES_PASSWORD}@db',
                    'POSTGRES_PASSWORD: posthog': 'POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}',
                    'POSTGRES_PWD=posthog': 'POSTGRES_PWD=${POSTGRES_PASSWORD}',
                    'PGPASSWORD=posthog': 'PGPASSWORD=${POSTGRES_PASSWORD}',
                    'object_storage_root_user': '${OBJECT_STORAGE_ACCESS_KEY_ID}',
                    'object_storage_root_password': '${OBJECT_STORAGE_SECRET_ACCESS_KEY}'}
    for source, target in [('docker-compose.base.yml', 'docker-compose.base.yml'),
                           ('docker-compose.hobby.yml', 'compose.upstream.yaml'),
                           ('.env.services', '.env.services')]:
        content = (ROOT / 'upstream' / source).read_text()
        if source == 'docker-compose.base.yml':
            content = content.replace(
                '${CADDY_HOST:-http://localhost:8000} {',
                '${CADDY_HOST:-http://localhost:8000} {\n'
                '                    handle /kasanova-ingest/* {\n'
                '                        reverse_proxy analytics-ingress:8099\n'
                '                    }')
        for old, new in replacements.items():
            content = content.replace(old, new)
        if source == 'docker-compose.base.yml':
            # The upstream bucket bootstrap shell otherwise swallows SIGTERM.
            content = content.replace('                WEED_PID=$$!\n',
                '                WEED_PID=$$!\n'
                '                trap \'kill -TERM $$WEED_PID; wait $$WEED_PID; exit 0\' TERM INT\n')
            # The gateway joins a shared network whose other projects also use
            # aliases such as "web". Route to this project's unique names.
            content = re.sub(r'(reverse_proxy\s+)([a-z][a-z0-9-]*)(:[0-9]+)',
                             lambda match: match[1] + 'kasanova_posthog-' + match[2] + '-1' + match[3], content)
            content = content.replace('                ${CADDY_TLS_BLOCK:-}',
                                      '                ${CADDY_TLS_BLOCK:-}\n'
                                      '                    servers {\n'
                                      '                        trusted_proxies static ${POSTHOG_OUTER_PROXY_IP}\n'
                                      '                    }')
        write_private(ROOT / target, content)
    archive = ROOT / 'upstream-source.tar.gz'
    if not (ROOT / 'upstream-source.json').exists():
        with urllib.request.urlopen('https://codeload.github.com/PostHog/posthog/tar.gz/' + UPSTREAM_REVISION, timeout=120) as response, archive.open('wb') as output:
            shutil.copyfileobj(response, output)
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        count = 0
        with tarfile.open(archive, 'r:gz') as source:
            for member in source:
                relative = '/'.join(PurePosixPath(member.name).parts[1:])
                if not relative.startswith(SOURCE_PREFIXES):
                    continue
                if '..' in PurePosixPath(relative).parts or PurePosixPath(relative).is_absolute():
                    raise RuntimeError('Unsafe source archive path')
                target = ROOT / 'posthog' / relative
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    target.chmod(0o755)
                elif member.isfile():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source.extractfile(member) as stream, target.open('wb') as output:
                        shutil.copyfileobj(stream, output)
                    target.chmod(0o755 if member.mode & 0o111 else 0o644)
                    count += 1
        write_private(ROOT / 'upstream-source.json', json.dumps({'revision': UPSTREAM_REVISION, 'archive_sha256': digest, 'extracted_files': count, 'prepared_at': dt.datetime.now(dt.timezone.utc).isoformat()}, indent=2) + '\n')
        archive.unlink()
    for directory in (ROOT / 'posthog').rglob('*'):
        if directory.is_dir():
            directory.chmod(0o755)
    compose = ROOT / 'compose'
    compose.mkdir(exist_ok=True)
    for name, content in {'start': '#!/bin/bash\nset -e\n/compose/wait\n./bin/migrate\nexec ./bin/docker-server\n',
                          'temporal-django-worker': '#!/bin/bash\nset -e\nexec ./bin/temporal-django-worker\n',
                          'wait': '#!/usr/bin/env python3\nimport socket,time\nfor host,port in [("clickhouse",9000),("db",5432)]:\n while True:\n  try:\n   with socket.create_connection((host,port),timeout=5): break\n  except OSError: time.sleep(5)\n'}.items():
        (compose / name).write_text(content)
        (compose / name).chmod(0o755)
    (ROOT / 'share').mkdir(exist_ok=True)
    (ROOT / 'share').chmod(0o755)
    print(json.dumps({'prepared': True, 'revision': revision, 'upstream': UPSTREAM_REVISION, 'credentials_printed': False}))


if __name__ == '__main__':
    main()
