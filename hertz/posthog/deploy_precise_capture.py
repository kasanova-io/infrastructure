#!/usr/bin/env python3
"""Preserve the precise capture image, then replace only the owned capture service."""
import datetime as dt
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess

from import_amplitude_events import atomic_json

ROOT = Path(__file__).resolve().parent
COMPOSE = ['docker', 'compose', '-f', 'compose.upstream.yaml', '-f', 'compose.hertz.yaml', '-f', 'images.lock.yaml']


def main():
    os.umask(0o077)
    os.chdir(ROOT)
    evidence = ROOT / 'operations/capture-precise-build'
    manifest = json.loads((evidence / 'build-manifest.json').read_text())
    image_id = manifest['runtime_image_id']
    image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', image_id]))[0]
    if (image['Id'] != image_id or image['Config']['Labels'].get('io.kasanova.capture.float-roundtrip') != 'true'
        or image['Config']['Labels'].get('io.kasanova.capture.source-revision') != manifest['source']['revision']):
        raise RuntimeError('Precise capture image identity differs')
    # Check loader/runtime compatibility without service credentials or networking.
    # This revision has no CLI help path: it immediately reads configuration.
    # With no credentials/network, reaching its exact missing-REDIS error proves
    # the executable loaded; it does not establish service acceptance.
    loader = subprocess.run(['docker', 'run', '--rm', '--network', 'none', '--memory', '256m', '--cpus', '1',
        '--entrypoint', '/usr/local/bin/capture', image_id], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if loader.returncode != 101 or b'EnvVarMissing { name: "REDIS_URL" }' not in loader.stdout:
        raise RuntimeError('Precise capture did not reach its expected configuration boundary')
    with (ROOT / 'operations/.backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = json.loads(subprocess.check_output(COMPOSE + ['config', '--format', 'json']))
        if before['name'] != 'kasanova_posthog': raise RuntimeError('Unexpected Compose ownership')
        archive_dir = ROOT / 'custom-images'
        archive_dir.mkdir(mode=0o700, exist_ok=True)
        archive = archive_dir / ('capture-' + image_id.removeprefix('sha256:') + '.tar.gz')
        receipt = archive.with_suffix('.json')
        if not receipt.exists():
            temporary = archive.with_suffix('.partial')
            process = subprocess.Popen(['docker', 'save', image_id], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            try:
                with temporary.open('wb') as file, gzip.GzipFile(fileobj=file, mode='wb', mtime=0) as output:
                    while block := process.stdout.read(1024 * 1024): output.write(block)
                if process.wait() != 0: raise RuntimeError('Custom capture image export failed')
            finally:
                if process.poll() is None: process.terminate(); process.wait()
                process.stdout.close()
            temporary.replace(archive)
            h = hashlib.sha256()
            with archive.open('rb') as stream:
                while block := stream.read(1024 * 1024): h.update(block)
            atomic_json(receipt, dict(manifest, archive_name=archive.name, archive_sha256=h.hexdigest(),
                restore_instruction='Verify SHA256, gzip -dc the archive into docker load, and verify the exact runtime_image_id before Compose starts'))
        preserved = json.loads(receipt.read_text())
        h = hashlib.sha256()
        with archive.open('rb') as stream:
            while block := stream.read(1024 * 1024): h.update(block)
        if preserved['runtime_image_id'] != image_id or preserved['archive_sha256'] != h.hexdigest():
            raise RuntimeError('Preserved custom image differs')
        pin = ROOT / 'images.lock.yaml'
        old = pin.read_bytes()
        current = json.loads(old)
        rollback = evidence / 'images.lock.before-precision.json'
        if not rollback.exists(): rollback.write_bytes(old); rollback.chmod(0o600)
        current['services']['capture']['image'] = image_id
        current['services']['capture']['pull_policy'] = 'never'
        atomic_json(pin, current)
        try:
            after = json.loads(subprocess.check_output(COMPOSE + ['config', '--format', 'json']))
            expected = json.loads(json.dumps(before))
            expected['services']['capture']['image'] = image_id
            expected['services']['capture']['pull_policy'] = 'never'
            if after != expected: raise RuntimeError('Compose update changed more than capture image policy')
            result = subprocess.run(COMPOSE + ['up', '-d', '--no-build', '--no-deps', 'capture'],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            (evidence / 'deployment.log').write_bytes(result.stdout)
            if result.returncode: raise RuntimeError('Capture replacement failed; inspect private deployment.log')
        except Exception:
            pin.write_bytes(old)
            # Restore the previous service specification if recreation failed.
            subprocess.run(COMPOSE + ['up', '-d', '--no-build', '--no-deps', 'capture'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            raise
        running = json.loads(subprocess.check_output(['docker', 'inspect', 'kasanova_posthog-capture-1']))[0]
        if running['Image'] != image_id or not running['State']['Running']:
            raise RuntimeError('Precise capture replacement is not running')
        report = {'deployed_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'runtime_image_id': image_id,
            'only_capture_service_recreated': True, 'custom_image_archive': str(archive),
            'archive_sha256': preserved['archive_sha256'], 'image_archive_in_runtime_backup': True,
            'previous_image_lock': str(rollback), 'application_connector_changed': False,
            'precision_and_repair_acceptance_pending': True}
        atomic_json(evidence / 'deployment.json', report)
        print(json.dumps(report), flush=True)


if __name__ == '__main__': main()
