#!/usr/bin/env python3
"""Cold checkpoint of this PostHog project; never stop or copy another project."""
import argparse
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
import re
import sqlite3
import signal
import shutil
from contextlib import closing

ROOT = Path(__file__).resolve().parent
PROJECT = 'kasanova_posthog'
BACKUPS = Path('/home/ren/kasanova-archives/posthog')
RESTORES = Path('/home/ren/kasanova-archives/posthog-restores')
PRODUCTION_RESERVE_BYTES = 10 * 1024**3
SNAPSHOT_OVERHEAD_BYTES = 2 * 1024**3
STORAGE = {'db', 'clickhouse', 'zookeeper', 'kafka', 'redis7', 'valkey',
           'objectstorage', 'seaweedfs', 'elasticsearch'}
CAPTURE = {'capture', 'capture-logs', 'replay-capture'}
INGESTION = {'ingestion-general', 'ingestion-sessionreplay', 'ingestion-error-tracking',
             'ingestion-logs', 'ingestion-traces', 'plugins'}
# The pinned Go live-preview consumers use enable.auto.commit=false and have no
# manual commit call. Broker "lag" is not a persistence acknowledgment for them.
# Their Kafka and Redis bytes are still included in every checkpoint.
NON_COMMITTING_PREVIEW_GROUPS = {'livestream', 'livestream-session-recordings'}


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


def parse_group_lags(output, expected):
    groups = {}
    current = None
    for line in output.splitlines():
        fields = line.split()
        if fields and fields[0] == 'GROUP':
            current = fields[1]
        elif fields and fields[0] == 'TOTAL-LAG':
            if current is None or current in groups or not re.fullmatch(r'[0-9]+', fields[1]):
                raise RuntimeError('Unrecognized consumer-lag output')
            groups[current] = int(fields[1])
    if set(groups) != set(expected):
        raise RuntimeError('Missing or unexpected consumer groups in lag report')
    return groups


def consumer_lags(kafka):
    listing = run(['docker', 'exec', kafka, 'rpk', 'group', 'list']).decode()
    rows = [line.split() for line in listing.splitlines()[1:] if line.strip()]
    if not rows or any(len(row) != 3 for row in rows):
        raise RuntimeError('Unrecognized consumer group inventory')
    groups = [row[1] for row in rows]
    if not {'group1', 'clickhouse-ingestion', 'clickhouse-ingestion-historical'}.issubset(groups):
        raise RuntimeError('Required event ingestion consumer groups are absent')
    summary = run(['docker', 'exec', kafka, 'rpk', 'group', 'describe', '--print-summary', *groups]).decode()
    return parse_group_lags(summary, groups)


def ingestion_drained(lags):
    if not {'group1', 'clickhouse-ingestion', 'clickhouse-ingestion-historical'}.issubset(lags):
        raise RuntimeError('Required ingestion groups are absent')
    return all(lag == 0 for group, lag in lags.items() if group not in NON_COMMITTING_PREVIEW_GROUPS)


def journal_inventory(path):
    """Read every committed journal row, including pending work, without changing it."""
    if path.is_symlink() or not path.is_file():
        raise RuntimeError('Missing or aliased analytics journal')
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as con:
        con.execute('PRAGMA query_only=ON')
        if con.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
            raise RuntimeError('Analytics journal integrity failed')
        schema = con.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").fetchall()
        tables = {row[1] for row in schema if row[0] == 'table'}
        if tables != {'records', 'identities', 'history'}:
            raise RuntimeError('Unexpected analytics journal tables')
        result = {'schema_sha256': hashlib.sha256(json.dumps(schema, separators=(',', ':')).encode()).hexdigest(),
                  'tables': {}, 'pending': con.execute('SELECT count(*) FROM records WHERE sent=0').fetchone()[0]}
        for table, ordering in (('records', 'project,id'), ('identities', 'project,device'), ('history', 'project,source_id')):
            digest, count = hashlib.sha256(), 0
            for row in con.execute('SELECT * FROM ' + table + ' ORDER BY ' + ordering):
                digest.update(json.dumps(row, separators=(',', ':'), ensure_ascii=False).encode() + b'\n')
                count += 1
            result['tables'][table] = {'rows': count, 'sha256': digest.hexdigest()}
        return result


def snapshot_journal(source, destination):
    """SQLite backup consolidates committed WAL rows; original volume is also archived."""
    before = journal_inventory(source)
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)) as src:
        with closing(sqlite3.connect(destination)) as dst:
            src.backup(dst)
    if journal_inventory(destination) != before or journal_inventory(source) != before:
        raise RuntimeError('Analytics journal changed or snapshot differs')
    with destination.open('rb') as stream:
        os.fsync(stream.fileno())
    fd = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return before


def logical_bytes(base):
    return sum(p.lstat().st_size for p in [base, *base.rglob('*')]
               if p.is_file() and not p.is_symlink())


def space_anchor(path):
    while not path.exists():
        path = path.parent
    return path


def capacity_requirements(allocations):
    required = {}
    for device, amount in allocations:
        required[device] = required.get(device, 0) + amount
    return {device: amount + PRODUCTION_RESERVE_BYTES for device, amount in required.items()}


def require_filesystem_capacity(allocations):
    anchors = [(space_anchor(path), amount) for path, amount in allocations]
    required = capacity_requirements([(path.stat().st_dev, amount) for path, amount in anchors])
    for anchor, _ in anchors:
        if shutil.disk_usage(anchor).free < required[anchor.stat().st_dev]:
            raise RuntimeError('Insufficient space for checkpoint plus isolated restore and production reserve')
    return [path.stat().st_dev for path, _ in anchors]


def require_live_capacity(checkpoint, restores, docker_storage, volume_bytes, runtime_bytes, journal_bytes):
    # Docker's new isolated volumes are allocated on DockerRootDir, while the
    # extracted runtime uses the restore directory. Sum every simultaneous copy
    # on its actual filesystem; never assume either shares the checkpoint disk.
    checkpoint_bytes = (volume_bytes + runtime_bytes + journal_bytes) * 11 // 10 + SNAPSHOT_OVERHEAD_BYTES
    restore_runtime_bytes = runtime_bytes + SNAPSHOT_OVERHEAD_BYTES
    restore_volume_bytes = volume_bytes + SNAPSHOT_OVERHEAD_BYTES
    devices = require_filesystem_capacity([(checkpoint, checkpoint_bytes),
        (restores, restore_runtime_bytes), (docker_storage, restore_volume_bytes)])
    return {'checkpoint_bytes_budgeted': checkpoint_bytes,
            'restore_runtime_bytes_budgeted': restore_runtime_bytes,
            'restore_volume_bytes_budgeted': restore_volume_bytes,
            'production_reserve_bytes': PRODUCTION_RESERVE_BYTES,
            'all_copies_share_filesystem': len(set(devices)) == 1}


def owned_stack_ready(ingress, health_base='http://127.0.0.1:18084'):
    try:
        current = json.loads(run(['docker', 'inspect', ingress]))[0]
        labels, state = current['Config']['Labels'], current['State']
        if (labels.get('com.docker.compose.project') != PROJECT
            or labels.get('com.docker.compose.service') != 'analytics-ingress'
            or not state['Running'] or state.get('Restarting') or state['OOMKilled']):
            return False
        for path in ('/_health', '/kasanova-ingest/health'):
            req = urllib.request.Request(health_base + path, headers={'Host': 'posthog.kasanova.io'})
            with urllib.request.urlopen(req, timeout=3) as response:
                if response.status != 200:
                    return False
                if path.endswith('ingest/health') and json.load(response).get('status') != 'ok':
                    return False
        return True
    except (subprocess.CalledProcessError, OSError, ValueError, KeyError):
        return False


def interrupt_operation(signum, frame):
    raise InterruptedError('Owned backup/restore interrupted')


def main(offline_import_reconciliation=None, live=False, verify_restore=False):
    if verify_restore and not live:
        raise RuntimeError('Automatic restore verification requires live checkpoint scope')
    if live and offline_import_reconciliation is not None:
        raise RuntimeError('Live and offline checkpoint scopes are mutually exclusive')
    if os.geteuid() != 0:
        raise RuntimeError('Run through sudo to preserve persistent volume ownership')
    if live:
        signal.signal(signal.SIGTERM, interrupt_operation)
    os.umask(0o077)
    owner = ROOT.stat()
    def save(path, data):
        with path.open('w') as stream:
            stream.write(json.dumps(data, indent=2) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        path.chmod(0o600)
        os.chown(path, owner.st_uid, owner.st_gid)
        with path.open('rb') as stream:
            os.fsync(stream.fileno())
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    with (ROOT / 'operations' / '.backup.lock').open('a') as lock:
        os.fchmod(lock.fileno(), 0o600)
        os.fchown(lock.fileno(), owner.st_uid, owner.st_gid)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ids = run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + PROJECT]).decode().split()
        if not ids:
            raise RuntimeError('No owned PostHog containers found')
        containers = json.loads(run(['docker', 'inspect', *ids]))
        services = {c['Config']['Labels']['com.docker.compose.service']: c for c in containers}
        if live and ('analytics-ingress' not in services or not services['analytics-ingress']['State']['Running']):
            raise RuntimeError('Live checkpoint requires the owned running analytics ingress')
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
        if live:
            mounts = [m for m in services['analytics-ingress']['Mounts']
                      if m['Type'] == 'volume' and m['Destination'] == '/var/lib/analytics']
            if len(mounts) != 1 or mounts[0]['Name'] not in volumes:
                raise RuntimeError('Unexpected analytics journal mount')
            docker_storage = Path(json.loads(run(['docker', 'info', '--format', '{{json .DockerRootDir}}']))) / 'volumes'
            if not docker_storage.is_absolute() or not docker_storage.is_dir():
                raise RuntimeError('Unexpected Docker restore storage root')
            capacity = require_live_capacity(backup, RESTORES, docker_storage,
                sum(logical_bytes(Path(v)) for v in volumes.values()), logical_bytes(ROOT),
                logical_bytes(Path(mounts[0]['Source'])))
            for source in [ROOT, *(Path(v) for v in volumes.values())]:
                if shutil.disk_usage(source).free < PRODUCTION_RESERVE_BYTES:
                    raise RuntimeError('Production filesystem reserve is insufficient before live checkpoint')
        baseline = {
            'postgres': json.loads(run(['docker', 'exec', services['db']['Name'], 'psql', '-U', 'posthog', '-d', 'posthog', '-At', '-c',
                "SELECT json_build_object('users',(SELECT json_agg(json_build_object('id',id,'email',email)) FROM posthog_user),'teams',(SELECT json_agg(json_build_object('id',id,'name',name)) FROM posthog_team));"])),
            'clickhouse': json.loads(run(['docker', 'exec', services['clickhouse']['Name'], 'clickhouse-client', '--query',
                "SELECT team_id,event,count() AS rows,uniqExact(uuid) AS unique_events FROM posthog.events GROUP BY team_id,event ORDER BY team_id,event FORMAT JSON"]))['data'],
            'diagnostic': json.loads((ROOT / 'operations' / 'diagnostic-marker.json').read_text()),
        }
        # Nonempty PROD is allowed only for a reconciled, offline historical import.
        # This does not certify recurring backups with SDK clients connected.
        credentials = json.loads(Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json').read_text())
        baseline['project_ids'] = {k: v['id'] for k, v in credentials['projects'].items()}
        prod_rows = sum(int(row['unique_events']) for row in baseline['clickhouse']
                        if int(row['team_id']) == baseline['project_ids']['prod'])
        import_proof = None
        if offline_import_reconciliation is not None:
            import_proof = json.loads(offline_import_reconciliation.read_text())
            required = ['all_uuids_event_names_distinct_ids_and_microsecond_timestamps_verified',
                        'all_original_record_bytes_hashes_and_typed_event_user_properties_verified',
                        'all_native_session_device_ip_mappings_verified']
            if (import_proof['target_project_id'] != baseline['project_ids']['prod']
                or import_proof.get('source_connector_changed') is not False
                or any(import_proof.get(key) is not True for key in required)
                or import_proof['logical_prod_rows'] != prod_rows
                or import_proof['expected_source_records'] != prod_rows):
                raise RuntimeError('Offline import proof does not match persisted PROD')
            normal_path = offline_import_reconciliation.parent / 'normal-query-count-verification.json'
            normal = json.loads(normal_path.read_text())
            physical_prod_rows = sum(int(row['rows']) for row in baseline['clickhouse']
                if int(row['team_id']) == baseline['project_ids']['prod'])
            if (normal.get('normal_posthog_prod_queries_count_each_original_event_once') is not True
                or normal.get('source_sha256') != import_proof['source_sha256']
                or normal['prod_project_id'] != baseline['project_ids']['prod']
                or normal['expected_events'] != prod_rows or normal['normal_query_prod_rows'] != prod_rows
                or physical_prod_rows != prod_rows):
                raise RuntimeError('Normal query proof or current physical PROD rows differ from reconciled source')
        elif prod_rows and not live:
            raise RuntimeError('Nonempty PROD requires verified offline import proof; live ingestion backup is not supported')
        report = {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'project': PROJECT,
                  'backup_path': str(backup), 'state': 'prepared', 'containers': rows, 'volumes': volumes,
                  'baseline': baseline, 'archives': {}}
        if live:
            report['capacity_budget'] = capacity
        if import_proof:
            # Freeze proof bytes in the checkpoint. Later verification runs may
            # update the operational reports at their original mutable paths.
            certificates = [('offline-import-reconciliation.json', offline_import_reconciliation),
                            ('normal-query-count-verification.json', normal_path)]
            for name, source_path in certificates:
                certificate = backup / name
                certificate.write_bytes(source_path.read_bytes())
                certificate.chmod(0o600)
                os.chown(certificate, owner.st_uid, owner.st_gid)
                report['archives'][name] = {'sha256': sha(certificate), 'bytes': certificate.stat().st_size,
                                           'role': 'offline_import_certificate'}
            report['offline_import_proof'] = {'path': str(offline_import_reconciliation),
                'sha256': sha(offline_import_reconciliation), 'records': prod_rows,
                'normal_query_proof_path': str(normal_path), 'normal_query_proof_sha256': sha(normal_path),
                'checkpoint_field_proof': 'offline-import-reconciliation.json',
                'checkpoint_normal_query_proof': 'normal-query-count-verification.json'}
        report['preview_group_offset_semantics'] = {
            'groups': sorted(NON_COMMITTING_PREVIEW_GROUPS),
            'zero_committed_lag_not_a_drain_test': True,
            'source_revision': '397fc8862e1ca5121a8679f0f686afde88d70b14',
            'reason': 'Pinned live-preview consumers disable automatic commits and do not manually commit',
            'queue_volume_bytes_still_checkpointed': True,
        }
        save(backup / 'manifest.json', report)
        running = [c for c in containers if c['State']['Running']]
        original_names = [c['Name'].lstrip('/') for c in running]
        save(ROOT / 'operations' / 'backup-running.json', {'backup_path': str(backup), 'original_running_containers': original_names})
        stopped = False
        try:
            stopped = True
            proxy = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] == 'proxy']
            captures = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] in CAPTURE]
            apps = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] not in STORAGE | CAPTURE | {'proxy'}]
            stores = [c['Name'] for c in running if c['Config']['Labels']['com.docker.compose.service'] in STORAGE]
            # ClickHouse's replicated/Kafka engines need their dependencies alive
            # while shutting down. Stop it before Kafka, ZooKeeper and Postgres.
            ordered_stores = [[services[s]['Name'] for s in group if services[s]['State']['Running']]
                              for group in (('clickhouse',), ('kafka',), ('zookeeper',),
                                            ('redis7', 'valkey', 'objectstorage', 'seaweedfs', 'elasticsearch'), ('db',))]
            ingress = [services['analytics-ingress']['Name']] if live else []
            apps = [name for name in apps if name not in ingress]
            # Stop the public gateway, then the owned journal producer before capture.
            # Committed pending rows stay in the journal and retry after restart.
            for group in (proxy, ingress, captures):
                if group:
                    run(['docker', 'stop', '--time', '15' if group == proxy else '90', *group])
            if live:
                state = json.loads(run(['docker', 'inspect', *ingress]))[0]['State']
                if state['Running'] or state['OOMKilled'] or state['ExitCode'] not in (0, 143):
                    raise RuntimeError('Analytics ingress did not stop safely')
            capture_states = json.loads(run(['docker', 'inspect', *captures])) if captures else []
            report['capture_shutdown_exit_codes'] = {c['Config']['Labels']['com.docker.compose.service']: c['State']['ExitCode'] for c in capture_states}
            save(backup / 'shutdown-results.json', report['capture_shutdown_exit_codes'])
            if any(c['State']['ExitCode'] != 0 or c['State']['Running'] or c['State']['OOMKilled'] for c in capture_states):
                raise RuntimeError('Capture did not drain and stop cleanly')
            consecutive = 0
            report['ingestion_drain_samples'] = []
            for attempt in range(90):
                lags = consumer_lags(services['kafka']['Name'])
                report['ingestion_drain_samples'].append({'at': dt.datetime.now(dt.timezone.utc).isoformat(), 'group_lags': lags})
                consecutive = consecutive + 1 if ingestion_drained(lags) else 0
                if consecutive >= 2: break
                time.sleep(2)
            else:
                raise RuntimeError('Ingestion queues did not drain; checkpoint rejected')
            if apps:
                run(['docker', 'stop', '--time', '90', *apps])
            report['consumer_lags_after_app_shutdown'] = consumer_lags(services['kafka']['Name'])
            if not ingestion_drained(report['consumer_lags_after_app_shutdown']):
                raise RuntimeError('App shutdown left pending ingestion work; checkpoint rejected')
            # Capture a frozen event baseline with ClickHouse and its dependencies alive.
            frozen = json.loads(run(['docker', 'exec', services['clickhouse']['Name'], 'clickhouse-client', '--query',
                "SELECT team_id,event,count() AS rows,uniqExact(uuid) AS unique_events FROM posthog.events GROUP BY team_id,event ORDER BY team_id,event FORMAT JSON"]))['data']
            logical = lambda rows: [{k: v for k, v in row.items() if k != 'rows'} for row in rows]
            if not live and logical(frozen) != logical(baseline['clickhouse']):
                raise RuntimeError('Events changed during offline checkpoint drain')
            baseline['clickhouse'] = frozen
            if live:
                baseline['postgres'] = json.loads(run(['docker', 'exec', services['db']['Name'], 'psql', '-U', 'posthog', '-d', 'posthog', '-At', '-c',
                    "SELECT json_build_object('users',(SELECT json_agg(json_build_object('id',id,'email',email)) FROM posthog_user),'teams',(SELECT json_agg(json_build_object('id',id,'name',name)) FROM posthog_team));"]))
                mounts = [m for m in services['analytics-ingress']['Mounts']
                          if m['Type'] == 'volume' and m['Destination'] == '/var/lib/analytics']
                if len(mounts) != 1 or mounts[0]['Name'] not in volumes:
                    raise RuntimeError('Unexpected analytics journal mount')
                snapshot = backup / 'analytics-journal.sqlite'
                journal = snapshot_journal(Path(mounts[0]['Source']) / 'ingress.sqlite', snapshot)
                os.chown(snapshot, owner.st_uid, owner.st_gid)
                report['analytics_journal'] = {'volume': mounts[0]['Name'], 'relative_path': 'ingress.sqlite',
                    'snapshot': snapshot.name, 'inventory': journal, 'pending_rows_preserved': True}
                report['archives'][snapshot.name] = {'sha256': sha(snapshot), 'bytes': snapshot.stat().st_size,
                                                    'role': 'live_journal_snapshot'}
            for group in ordered_stores:
                if group: run(['docker', 'stop', '--time', '120', *group])
            states = json.loads(run(['docker', 'inspect', *ids]))
            report['shutdown_exit_codes'] = {c['Config']['Labels']['com.docker.compose.service']: c['State']['ExitCode'] for c in states}
            save(backup / 'shutdown-results.json', report['shutdown_exit_codes'])
            if any(c['State']['Running'] or c['State']['OOMKilled'] for c in states):
                raise RuntimeError('An owned process remains running or was OOM-killed')
            if any(c['State']['ExitCode'] == 137 for c in states
                   if c['Config']['Labels']['com.docker.compose.service'] in STORAGE):
                raise RuntimeError('Clean shutdown was not achieved; do not certify checkpoint')
            if any(c['State']['ExitCode'] != 0 for c in states
                   if c['Config']['Labels']['com.docker.compose.service'] in INGESTION):
                raise RuntimeError('Ingestion service did not stop cleanly')
            report['scope'] = ('Steady-state live-client cold checkpoint; public requests paused, accepted and pending journal rows retained, downstream ingestion drained, all owned native database/object-storage/recording volume bytes preserved' if live else ('Reconciled offline PROD event backfill checkpoint' if import_proof else 'Initial empty-PROD checkpoint') + '; capture and ingestion drain verified, persistent services stop cleanly; no SDK clients connected. Recurring live-ingestion backup remains unverified.')
            report['live_checkpoint'] = live
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
            for name in report['archives']:
                with (backup / name).open('rb') as stream:
                    os.fsync(stream.fileno())
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
                    else:
                        if live:
                            raise RuntimeError('Owned gateway did not recover after live backup')
                finally:
                    if other_names:
                        run(['docker', 'start', *other_names])
                if live:
                    for attempt in range(60):
                        if owned_stack_ready(services['analytics-ingress']['Name']):
                            break
                        time.sleep(2)
                    else:
                        raise RuntimeError('Owned gateway and analytics ingress did not recover after restart')
                save(backup / 'restart.json', {'owned_gateway_and_ingress_ready': True if live else None,
                     'original_running_containers_restarted': original_names,
                     'completed_at': dt.datetime.now(dt.timezone.utc).isoformat()})
                print(json.dumps({'owned_services_restarted': len(original_names)}), flush=True)
        if verify_restore:
            result = json.loads(run(['python3', str(ROOT / 'verify_backup_restore.py'), str(backup)]))
            if result.get('analytics_journal_all_rows_and_pending_verified') is not True:
                raise RuntimeError('Live journal restore verification incomplete')
            print(json.dumps({'live_checkpoint_isolated_restore_verified': True, 'backup_path': str(backup)}), flush=True)
        print(json.dumps({'state': report['state'], 'backup_path': str(backup), 'volume_count': len(volumes)}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument('--offline-import-reconciliation', type=Path)
    scope.add_argument('--live', action='store_true', help='Cold checkpoint of active client ingestion, including durable journal')
    parser.add_argument('--verify-restore', action='store_true', help='After live restart, verify this exact checkpoint through the existing isolated restore')
    args = parser.parse_args()
    main(args.offline_import_reconciliation, args.live, args.verify_restore)
