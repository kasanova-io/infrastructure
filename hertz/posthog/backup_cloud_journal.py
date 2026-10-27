#!/usr/bin/env python3
"""Online Cloud-relay recovery snapshot; never stops or restarts a service."""
import argparse
from contextlib import closing
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile

from analytics_ingress import validate_cloud_routes
from backup import journal_inventory
from import_amplitude_events import atomic_json


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def snapshot(journal, config, runtime, output):
    os.umask(0o077)
    # Never create an empty source or overwrite a prior recovery point.
    if journal.is_symlink() or not journal.is_file():
        raise RuntimeError('Missing or aliased authoritative journal')
    validate_cloud_routes(json.loads(config.read_text())['routes'])
    config_sha = sha(config)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    target = output / 'ingress.sqlite'
    with closing(sqlite3.connect(journal.resolve().as_uri() + '?mode=ro', uri=True)) as src:
        with closing(sqlite3.connect(target)) as dst:
            src.backup(dst)
    inventory = journal_inventory(target)
    # Restore independently from the sealed copy, never query the moving source
    # for equality: WAL traffic may continue throughout this online snapshot.
    with tempfile.TemporaryDirectory(prefix='.cloud-journal-restore-', dir=output) as temporary:
        restored = Path(temporary) / 'ingress.sqlite'
        shutil.copyfile(target, restored)
        if sha(restored) != sha(target) or journal_inventory(restored) != inventory:
            raise RuntimeError('Independent journal restore differs')
    shutil.copyfile(config, output / 'ingress.json')
    if sha(config) != config_sha or sha(output / 'ingress.json') != config_sha:
        raise RuntimeError('Private configuration changed during snapshot')
    names = ['analytics_ingress.py', 'native_cloud_proxy.py', 'compose.cloud.yaml',
             'Caddyfile.cloud', '.env']
    for name in names:
        source = runtime / name
        source_sha = sha(source)
        shutil.copyfile(source, output / name)
        if sha(source) != source_sha or sha(output / name) != source_sha:
            raise RuntimeError('Runtime changed during snapshot')
    files = {p.name: {'sha256': sha(p), 'bytes': p.stat().st_size}
             for p in output.iterdir() if p.is_file()}
    for name in files:
        with (output / name).open('rb') as stream:
            os.fsync(stream.fileno())
    result = {'state': 'verified', 'completed_at': dt.datetime.now(dt.timezone.utc).isoformat(),
              'files': files, 'journal': inventory, 'independent_restore_verified': True,
              'live_services_stopped': 0, 'native_stack_backup': False}
    atomic_json(output / 'manifest.json', result)
    descriptor = os.open(output, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ('journal', 'config', 'runtime', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    result = snapshot(args.journal, args.config, args.runtime, args.output)
    print(json.dumps({'state': result['state'], 'files': len(result['files']),
                      'independent_restore_verified': result['independent_restore_verified']}))
