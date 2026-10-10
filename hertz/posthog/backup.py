#!/usr/bin/env python3
"""Cold checkpoint of this PostHog project; never stop or copy another project."""
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parent
PROJECT = 'kasanova_posthog'
BACKUPS = Path('/home/ren/kasanova-archives/posthog')
STORAGE = {'db', 'clickhouse', 'zookeeper', 'kafka', 'redis7', 'valkey',
           'objectstorage', 'seaweedfs', 'elasticsearch'}


def run(args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def inventory(root):
    result = {}
    for p in sorted(root.rglob('*')):
        s = p.lstat()
        item = {'mode': stat.S_IMODE(s.st_mode), 'uid': s.st_uid, 'gid': s.st_gid}
        if p.is_symlink():
            item.update(type='symlink', target=os.readlink(p))
        elif p.is_file():
            item.update(type='file', size=s.st_size, sha256=sha(p))
        elif p.is_dir():
            item['type'] = 'directory'
        else:
            raise RuntimeError('Unexpected special file in stopped persistent volume')
        result[p.relative_to(root).as_posix()] = item
    return result


def main():
    if os.geteuid() != 0:
        raise RuntimeError('Run through sudo to preserve persistent volume ownership')
    os.umask(0o077)
    owner = ROOT.stat()
    def save(path, data):
        path.write_text(json.dumps(data, indent=2) + '\n')
        path.chmod(0o600)
        os.chown(path, owner.st_uid, owner.st_gid)
    with (ROOT / 'operations' / '.backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ids = run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + PROJECT]).decode().split()
        if not ids:
            raise RuntimeError('No owned PostHog containers found')
        containers = json.loads(run(['docker', 'inspect', *ids]))
        services = {c['Config']['Labels']['com.docker.compose.service']: c for c in containers}
        if not STORAGE.issubset(services):
            raise RuntimeError('Required persistent services are missing')
        volumes = {}
        rows = []
        for c in containers:
            if c['Config']['Labels'].get('com.docker.compose.project') != PROJECT:
                raise RuntimeError('Unexpected project ownership')
            mounts = []
            for m in c['Mounts']:
                if m['Type'] == 'bind':
                    if not Path(m['Source']).resolve().is_relative_to(ROOT):
                        raise RuntimeError('Refuse a bind mount outside the PostHog runtime')
                elif m['Type'] == 'volume':
                    volumes[m['Name']] = m['Source']
                else:
                    raise RuntimeError('Unexpected persistent mount type')
                mounts.append({k: m.get(k) for k in ('Type', 'Name', 'Source', 'Destination', 'RW')})
            rows.append({'service': c['Config']['Labels']['com.docker.compose.service'],
                         'name': c['Name'].lstrip('/'), 'image': c['Config']['Image'], 'mounts': mounts})
        all_ids = run(['docker', 'ps', '-aq']).decode().split()
        all_containers = json.loads(run(['docker', 'inspect', *all_ids]))
        for c in all_containers:
            if c['Id'] not in ids and any(m.get('Name') in volumes for m in c['Mounts']):
                # docker ps emits short IDs; compare full IDs below instead.
                if not any(c['Id'].startswith(i) for i in ids):
                    raise RuntimeError('Persistent volume is shared with another project')
        stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        backup = BACKUPS / stamp
        backup.mkdir(parents=True, mode=0o700, exist_ok=False)
        os.chown(BACKUPS, owner.st_uid, owner.st_gid)
        os.chown(backup, owner.st_uid, owner.st_gid)
        BACKUPS.chmod(0o700)
        baseline = {
            'postgres': json.loads(run(['docker', 'exec', services['db']['Name'], 'psql', '-U', 'posthog', '-d', 'posthog', '-At', '-c',
                "SELECT json_build_object('users',(SELECT json_agg(json_build_object('id',id,'email',email)) FROM posthog_user),'teams',(SELECT json_agg(json_build_object('id',id,'name',name)) FROM posthog_team));"])),
            'clickhouse': json.loads(run(['docker', 'exec', services['clickhouse']['Name'], 'clickhouse-client', '--query',
                "SELECT team_id,event,count() AS rows,uniqExact(uuid) AS unique_events FROM posthog.events GROUP BY team_id,event ORDER BY team_id,event FORMAT JSON"]))['data'],
            'diagnostic': json.loads((ROOT / 'operations' / 'diagnostic-marker.json').read_text()),
        }
        # This initial checkpoint is valid only before PROD history/live capture.
        # A steady-state backup also needs a proven ingestion drain protocol.
        credentials = json.loads(Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json').read_text())
        baseline['project_ids'] = {k: v['id'] for k, v in credentials['projects'].items()}
        if any(int(row['team_id']) == baseline['project_ids']['prod'] and int(row['unique_events']) for row in baseline['clickhouse']):
            raise RuntimeError('Initial cold checkpoint requires empty PROD; use a drained backup procedure after import')
        report = {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'project': PROJECT,
                  'backup_path': str(backup), 'state': 'prepared', 'containers': rows, 'volumes': volumes,
                  'baseline': baseline, 'archives': {}}
        save(backup / 'manifest.json', report)
        running = [c for c in containers if c['State']['Running']]
        original_names = [c['Name'].lstrip('/') for c in running]
        save(ROOT / 'operations' / 'backup-running.json', {'backup_path': str(backup), 'original_running_containers': original_names})
        stopped = False
        try:
            stopped = True
            proxy = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] == 'proxy']
            apps = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] not in STORAGE | {'proxy'}]
            stores = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] in STORAGE]
            # ClickHouse's replicated/Kafka engines need their dependencies alive
            # while shutting down. Stop it before Kafka, ZooKeeper and Postgres.
            ordered_stores = [[services[s]['Name'] for s in group if services[s]['State']['Running']]
                              for group in (('clickhouse',), ('kafka',), ('zookeeper',),
                                            ('redis7', 'valkey', 'objectstorage', 'seaweedfs', 'elasticsearch'), ('db',))]
            for group in (proxy, apps, *ordered_stores):
                if group:
                    run(['docker', 'stop', '--time', '15' if group == proxy else '30' if group == apps else '120', *group])
            states = json.loads(run(['docker', 'inspect', *ids]))
            report['shutdown_exit_codes'] = {c['Config']['Labels']['com.docker.compose.service']: c['State']['ExitCode'] for c in states}
            save(backup / 'shutdown-results.json', report['shutdown_exit_codes'])
            if any(c['State']['Running'] or c['State']['OOMKilled'] for c in states):
                raise RuntimeError('An owned process remains running or was OOM-killed')
            if any(c['State']['ExitCode'] == 137 for c in states
                   if c['Config']['Labels']['com.docker.compose.service'] in STORAGE):
                raise RuntimeError('Clean shutdown was not achieved; do not certify checkpoint')
            report['scope'] = 'Initial empty-PROD checkpoint; persistent services must stop cleanly. Stateless setup services may require termination; no live PROD capture is connected.'
            report['backup_program_sha256'] = sha(Path(__file__))
            report.update(state='all_owned_writers_stopped', stopped_at=dt.datetime.now(dt.timezone.utc).isoformat())
            save(backup / 'manifest.json', report)
            print(json.dumps({'state': report['state'], 'backup_path': str(backup)}), flush=True)
            for name, source in sorted(volumes.items()):
                archive = backup / (name + '.tar.gz')
                entries = inventory(Path(source))
                save(backup / (name + '.inventory.json'), entries)
                run(['tar', '--numeric-owner', '--xattrs', '--acls', '--sparse', '-czf', str(archive), '-C', source, '.'])
                archive.chmod(0o600)
                os.chown(archive, owner.st_uid, owner.st_gid)
                report['archives'][archive.name] = {'sha256': sha(archive), 'bytes': archive.stat().st_size,
                    'inventory': name + '.inventory.json', 'inventory_sha256': sha(backup / (name + '.inventory.json')),
                    'files': sum(v['type'] == 'file' for v in entries.values())}
                save(backup / 'manifest.json', report)
                print(json.dumps({'volume_checkpointed': name, 'bytes': archive.stat().st_size}), flush=True)
            config = backup / 'runtime.tar.gz'
            run(['tar', '--numeric-owner', '--exclude=./operations', '--exclude=./__pycache__', '-czf', str(config), '-C', str(ROOT), '.'])
            os.chown(config, owner.st_uid, owner.st_gid)
            report['archives'][config.name] = {'sha256': sha(config), 'bytes': config.stat().st_size}
            credentials = backup / 'bootstrap.json'
            credentials.write_bytes(Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json').read_bytes())
            os.chown(credentials, owner.st_uid, owner.st_gid)
            report['archives'][credentials.name] = {'sha256': sha(credentials), 'bytes': credentials.stat().st_size}
            report.update(state='cold_checkpoint_complete', completed_at=dt.datetime.now(dt.timezone.utc).isoformat())
            save(backup / 'manifest.json', report)
        except BaseException as error:
            report.update(state='rejected_checkpoint', failure_type=type(error).__name__)
            save(backup / 'manifest.json', report)
            raise
        finally:
            if stopped:
                storage_names = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] in STORAGE]
                core_names = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] in {'web', 'proxy'}]
                other_names = [c['Name'] for c in running if c['Name'] not in storage_names + core_names]
                try:
                    if storage_names:
                        run(['docker', 'start', *storage_names])
                    if core_names:
                        run(['docker', 'start', *core_names])
                    for attempt in range(120):
                        try:
                            req = urllib.request.Request('http://127.0.0.1:18084/_health', headers={'Host': 'posthog.kasanova.io'})
                            with urllib.request.urlopen(req, timeout=3) as response:
                                if response.status == 200:
                                    break
                        except Exception:
                            pass
                        time.sleep(2)
                finally:
                    if other_names:
                        run(['docker', 'start', *other_names])
                save(backup / 'restart.json', {'original_running_containers_restarted': original_names,
                     'completed_at': dt.datetime.now(dt.timezone.utc).isoformat()})
                print(json.dumps({'owned_services_restarted': len(original_names)}), flush=True)
        print(json.dumps({'state': report['state'], 'backup_path': str(backup), 'volume_count': len(volumes)}), flush=True)


if __name__ == '__main__':
    main()
