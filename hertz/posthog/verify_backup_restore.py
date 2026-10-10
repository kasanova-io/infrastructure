#!/usr/bin/env python3
"""Restore checkpoint copies, verify every volume byte, and boot isolated PG/CH/ZK."""
import argparse
import datetime as dt
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

from backup import inventory, sha, journal_inventory, interrupt_operation


def run(args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


def choose_restore_subnet(blocked, seed):
    networks = [ipaddress.ip_network(n, strict=False) for n in blocked]
    # Explicit isolated networks do not consume Docker's exhausted default pool.
    # Never overlap a current Docker network or a non-default host route.
    offset = int(hashlib.sha256(seed.encode()).hexdigest()[:8], 16) % 4096
    for step in range(4096):
        index = (offset + step) % 4096
        candidate = ipaddress.ip_network(f'10.{240 + index // 256}.{index % 256}.0/24')
        if not any(n.version == 4 and candidate.overlaps(n) for n in networks):
            return str(candidate)
    raise RuntimeError('No unused private restore subnet is available; preserve existing networks')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('checkpoint', type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError('Run through sudo to restore original file ownership')
    os.umask(0o077)
    checkpoint = args.checkpoint.resolve()
    if checkpoint.parent != Path('/home/ren/kasanova-archives/posthog'):
        raise RuntimeError('Unexpected checkpoint location')
    manifest = json.loads((checkpoint / 'manifest.json').read_text())
    if manifest['state'] != 'cold_checkpoint_complete':
        raise RuntimeError('Checkpoint was not completed')
    for name, expected in manifest['archives'].items():
        if sha(checkpoint / name) != expected['sha256']:
            raise RuntimeError('Checkpoint archive checksum mismatch')
        if 'inventory' in expected and sha(checkpoint / expected['inventory']) != expected['inventory_sha256']:
            raise RuntimeError('Checkpoint inventory checksum mismatch')
    restore_id = 'ph_restore_' + checkpoint.name.lower()
    work = Path('/home/ren/kasanova-archives/posthog-restores') / checkpoint.name
    work.mkdir(parents=True, mode=0o700, exist_ok=False)
    uid, gid = checkpoint.stat().st_uid, checkpoint.stat().st_gid
    os.chown(work, uid, gid)
    os.chown(work.parent, uid, gid)
    volumes = {}
    containers = []
    network_created = False
    report = {'checkpoint': str(checkpoint), 'restore_id': restore_id,
              'started_at': dt.datetime.now(dt.timezone.utc).isoformat(),
              'isolated_network_internal_only': True, 'live_data_mounts_used': False,
              'volume_inventories_verified': [], 'archives_verified': len(manifest['archives'])}
    def save():
        p = checkpoint / 'restore-verification.json'
        p.write_text(json.dumps(report, indent=2) + '\n')
        os.chown(p, uid, gid)
        p.chmod(0o600)
    signal.signal(signal.SIGTERM, interrupt_operation)
    try:
        # Reserve the checked subnet before spending time restoring large volumes.
        network_ids = run(['docker', 'network', 'ls', '-q']).decode().split()
        existing = json.loads(run(['docker', 'network', 'inspect', *network_ids]))
        blocked = [item['Subnet'] for network in existing for item in (network['IPAM'].get('Config') or []) if item.get('Subnet')]
        routes = json.loads(run(['ip', '-j', 'route', 'show']))
        blocked += [route['dst'] for route in routes if route.get('dst') not in (None, 'default', '0.0.0.0/0')]
        subnet = choose_restore_subnet(blocked, restore_id)
        run(['docker', 'network', 'create', '--internal', '--subnet', subnet,
             '--label', 'io.kasanova.restore_id=' + restore_id, restore_id])
        network_created = True
        report['isolated_restore_subnet'] = subnet
        runtime = work / 'runtime'
        runtime.mkdir(mode=0o700)
        run(['tar', '--numeric-owner', '-xpf', str(checkpoint / 'runtime.tar.gz'), '-C', str(runtime)])
        capture = next(c for c in manifest['containers'] if c['service'] == 'capture')
        if capture['image'].startswith('sha256:'):
            receipt_path = runtime / 'custom-images' / ('capture-' + capture['image'].removeprefix('sha256:') + '.tar.json')
            preserved = json.loads(receipt_path.read_text())
            archive = receipt_path.parent / preserved['archive_name']
            if (archive.parent != receipt_path.parent or preserved['runtime_image_id'] != capture['image']
                or sha(archive) != preserved['archive_sha256']):
                raise RuntimeError('Restored custom capture image archive differs')
            report['custom_capture_image_archive_verified'] = True
            report['custom_capture_image_id'] = preserved['runtime_image_id']
            save()
        for original in sorted(manifest['volumes']):
            name = restore_id + '_' + hashlib.sha256(original.encode()).hexdigest()[:16]
            run(['docker', 'volume', 'create', '--label', 'io.kasanova.restore_id=' + restore_id, name])
            volumes[original] = name
            destination = Path(json.loads(run(['docker', 'volume', 'inspect', name]))[0]['Mountpoint'])
            run(['tar', '--numeric-owner', '--xattrs', '--acls', '-xpf', str(checkpoint / (original + '.tar.gz')), '-C', str(destination)])
            expected = json.loads((checkpoint / (original + '.inventory.json')).read_text())
            if inventory(destination) != expected:
                raise RuntimeError('Restored persistent volume bytes or ownership differ')
            report['volume_inventories_verified'].append(original)
            save()
        if manifest.get('live_checkpoint'):
            journal = manifest['analytics_journal']
            if journal['relative_path'] != 'ingress.sqlite' or journal['snapshot'] != 'analytics-journal.sqlite':
                raise RuntimeError('Unexpected restored journal location')
            volume = volumes[journal['volume']]
            mount = Path(json.loads(run(['docker', 'volume', 'inspect', volume]))[0]['Mountpoint'])
            if (journal_inventory(mount / 'ingress.sqlite') != journal['inventory']
                or journal_inventory(checkpoint / journal['snapshot']) != journal['inventory']):
                raise RuntimeError('Restored analytics journal rows differ from checkpoint')
            report['analytics_journal_all_rows_and_pending_verified'] = True
            report['analytics_journal_pending_rows'] = journal['inventory']['pending']
            save()
        services = {c['service']: c for c in manifest['containers']}
        def start(service, memory, extra=()):
            c = services[service]
            name = restore_id + '_' + service
            cmd = ['docker', 'create', '--name', name, '--hostname', service, '--network', restore_id,
                   '--network-alias', service, '--label', 'io.kasanova.restore_id=' + restore_id,
                   '--cgroup-parent', 'kasanova-posthog.slice', '--cpus', '1', '--memory', memory,
                   '--restart', 'no']
            for mount in c['mounts']:
                if mount['Type'] == 'volume':
                    source = volumes[mount['Name']]
                else:
                    source = str(runtime / Path(mount['Source']).relative_to('/home/ren/Kasanova/tools/posthog'))
                cmd += ['--mount', 'type=' + mount['Type'] + ',source=' + source + ',target=' + mount['Destination'] + (',readonly' if not mount['RW'] else '')]
            cmd += list(extra) + [c['image']]
            run(cmd)
            containers.append(name)
            run(['docker', 'start', name])
            return name
        zk = start('zookeeper', '512m')
        pg = start('db', '768m')
        ch = start('clickhouse', '4g', ['--env', 'CLICKHOUSE_SKIP_USER_SETUP=1'])
        for attempt in range(90):
            try:
                pg_data = json.loads(run(['docker', 'exec', pg, 'psql', '-U', 'posthog', '-d', 'posthog', '-At', '-c',
                    "SELECT json_build_object('users',(SELECT json_agg(json_build_object('id',id,'email',email)) FROM posthog_user),'teams',(SELECT json_agg(json_build_object('id',id,'name',name)) FROM posthog_team));"]))
                ch_data = json.loads(run(['docker', 'exec', ch, 'clickhouse-client', '--query',
                    "SELECT team_id,event,count() AS rows,uniqExact(uuid) AS unique_events FROM posthog.events GROUP BY team_id,event ORDER BY team_id,event FORMAT JSON"]))['data']
                break
            except (subprocess.CalledProcessError, json.JSONDecodeError):
                time.sleep(2)
        else:
            raise RuntimeError('Isolated restored databases did not become queryable')
        if pg_data != manifest['baseline']['postgres']:
            raise RuntimeError('Restored owner/projects differ from checkpoint')
        # ReplacingMergeTree can merge repeated setup retries; compare distinct UUIDs.
        unique_counts = lambda data: [{k: v for k, v in row.items() if k != 'rows'} for row in data]
        if unique_counts(ch_data) != unique_counts(manifest['baseline']['clickhouse']):
            raise RuntimeError('Restored event UUID counts differ from checkpoint')
        marker = manifest['baseline']['diagnostic']['uuid']
        sql = "SELECT event,distinct_id,properties FROM posthog.events WHERE uuid = '" + marker + "' FORMAT JSON"
        events = json.loads(run(['docker', 'exec', ch, 'clickhouse-client', '--query', sql]))['data']
        if not events:
            raise RuntimeError('Diagnostic event missing from restored storage')
        for event in events:
            properties = json.loads(event['properties']) if isinstance(event['properties'], str) else event['properties']
            if event['event'] != 'posthog_installation_check' or properties.get('number') != 42 or properties.get('boolean') is not True or properties.get('nested') != {'preserved': 'yes'}:
                raise RuntimeError('Restored event properties differ')
        report.update(postgresql_owner_and_projects_verified=True, clickhouse_distinct_event_counts_verified=True,
                      clickhouse_diagnostic_typed_properties_verified=True,
                      object_storage_and_queue_volume_bytes_verified=True,
                      completed_at=dt.datetime.now(dt.timezone.utc).isoformat())
        save()
    finally:
        # Only resources bearing this exact restore ID may be removed.
        for name in reversed(containers):
            obj = json.loads(run(['docker', 'inspect', name]))[0]
            if obj['Config']['Labels'].get('io.kasanova.restore_id') != restore_id:
                raise RuntimeError('Preserve a container with different ownership')
            run(['docker', 'rm', '-f', name])
        if network_created:
            obj = json.loads(run(['docker', 'network', 'inspect', restore_id]))[0]
            if obj['Labels'].get('io.kasanova.restore_id') != restore_id:
                raise RuntimeError('Preserve a network with different ownership')
            run(['docker', 'network', 'rm', restore_id])
        for name in volumes.values():
            obj = json.loads(run(['docker', 'volume', 'inspect', name]))[0]
            if obj['Labels'].get('io.kasanova.restore_id') != restore_id:
                raise RuntimeError('Preserve a volume with different ownership')
            run(['docker', 'volume', 'rm', name])
        shutil.rmtree(work)
        report['owned_temporary_restore_resources_cleaned'] = True
        save()
    print(json.dumps(report))


if __name__ == '__main__':
    main()
