"""Resumable Cloud export and offline exact-field reconciliation.

Exports use the normal project-bound query API. Verification reads only sealed
source/Cloud files and the source journal; it never sends or repairs events.
"""
import argparse
from contextlib import closing
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import uuid

from cloud_client import CloudClient
from daily_refill import cloud_fields_match, source_wire_envelope
from export_cloud_source import file_sha
from import_amplitude_events import atomic_json, canonical


def export_cloud(credentials, environment, output):
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    client = CloudClient(credentials, environment, output / 'api')
    manifest_path = output / 'manifest.json'
    state = json.loads(manifest_path.read_text()) if manifest_path.exists() else {
        'state': 'exporting', 'cloud_project_id': client.project['id'],
        'api_host': client.config['api_host'],
        'environment': environment, 'pages': [], 'rows': 0, 'last_uuid': '',
        'started_at': dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if (state['cloud_project_id'] != client.project['id'] or state['environment'] != environment
            or state.get('api_host') != client.config['api_host']):
        raise RuntimeError('Cloud export destination changed')
    for page in state['pages']:
        if file_sha(output / page['file']) != page['sha256']:
            raise RuntimeError('Cloud export page changed')
    while state['state'] != 'cloud_export_complete':
        last = state['last_uuid']
        if last and str(uuid.UUID(last)) != last:
            raise RuntimeError('Invalid Cloud export cursor')
        predicate = "toString(uuid) > '" + last + "'" if last else '1=1'
        sql = ('SELECT toString(uuid),argMax(tuple(event,distinct_id,timestamp,properties),created_at) '
               f'FROM events WHERE {predicate} GROUP BY uuid ORDER BY toString(uuid) LIMIT 2000')
        rows = client.query(sql)
        if not rows:
            state.update(state='cloud_export_complete', completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                         limitation='Live Cloud snapshot; extra later events are allowed, source UUIDs must all reconcile')
            atomic_json(manifest_path, state)
            break
        cursor = last
        for row in rows:
            if len(row) != 2 or len(row[1]) != 4 or str(uuid.UUID(row[0])) != row[0] or row[0] <= cursor:
                raise RuntimeError('Cloud page cursor/order/schema changed')
            cursor = row[0]
        name = f'events-{len(state["pages"]):06d}.ndjson.gz'
        partial = output / (name + '.partial')
        if partial.exists() or (output / name).exists():
            raise RuntimeError('Unbound Cloud page exists; preserve it before resuming')
        with gzip.open(partial, 'wt') as stream:
            for row in rows:
                stream.write(canonical(row) + '\n')
        partial.replace(output / name)
        state['pages'].append({'file': name, 'sha256': file_sha(output / name), 'rows': len(rows)})
        state.update(rows=state['rows'] + len(rows), last_uuid=cursor)
        atomic_json(manifest_path, state)
        print(json.dumps({'environment': environment, 'exported_rows': state['rows']}), flush=True)
    return state


def verify_snapshot(snapshot, cloud, environment, ingress_config, output):
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    source_manifest = json.loads((snapshot / 'manifest.json').read_text())
    cloud_manifest = json.loads((cloud / 'manifest.json').read_text())
    if source_manifest['state'] != 'source_export_complete' or cloud_manifest['state'] != 'cloud_export_complete':
        raise RuntimeError('Both source exports must be complete')
    if cloud_manifest['environment'] != environment:
        raise RuntimeError('Cloud export environment differs')
    for name, receipt in source_manifest['files'].items():
        if Path(name).name != name or file_sha(snapshot / name) != receipt['sha256']:
            raise RuntimeError('Sealed native source changed')
    source_project = source_manifest['projects'][environment]['source_project_id']
    config_sha = file_sha(ingress_config)
    routes = json.loads(ingress_config.read_text())['routes']
    targets = set()
    for route in routes.values():
        if route['project_id'] != source_project:
            continue
        if route.get('environment', environment).lower() != environment:
            raise RuntimeError('Source route environment differs')
        cloud_route = route['cloud']
        host = cloud_route['host'].rstrip('/')
        if host not in ('https://us.i.posthog.com', 'https://eu.i.posthog.com'):
            raise RuntimeError('Unexpected configured Cloud origin')
        api_host = 'https://us.posthog.com' if host == 'https://us.i.posthog.com' else 'https://eu.posthog.com'
        targets.add((cloud_route['project_id'], api_host))
    if len(targets) != 1 or (cloud_manifest.get('cloud_project_id'), cloud_manifest.get('api_host')) not in targets:
        raise RuntimeError('Cloud export project/origin differs from configured source destination')
    journal = sqlite3.connect((snapshot / 'ingress.sqlite').resolve().as_uri() + '?mode=ro', uri=True)
    db = sqlite3.connect(output / 'verification.private.sqlite')
    try:
        db.execute('CREATE TABLE source(uuid TEXT PRIMARY KEY,event TEXT NOT NULL,matched INTEGER NOT NULL DEFAULT 0)')
        db.execute('CREATE TABLE seen_cloud(uuid TEXT PRIMARY KEY)')
        source_rows = 0
        with gzip.open(snapshot / (environment + '-events.ndjson.gz'), 'rt') as stream:
            for line in stream:
                row = json.loads(line)
                owned = journal.execute('SELECT native FROM records WHERE project=? AND id=?',
                                        (str(source_project), row['uuid'])).fetchone()
                native = {row['uuid']: json.loads(owned[0])} if owned else {}
                event = source_wire_envelope(row, {'ingress_config': str(ingress_config)}, source_project, native)
                db.execute('INSERT INTO source(uuid,event) VALUES (?,?)', (event['uuid'], canonical(event)))
                source_rows += 1
        expected = source_manifest['projects'][environment]['rows']
        if source_rows != expected:
            raise RuntimeError('Native row count differs from sealed manifest')
        db.commit()
        cloud_rows = 0
        mismatches = []
        previous_uuid = ''
        for page in cloud_manifest['pages']:
            if Path(page['file']).name != page['file'] or file_sha(cloud / page['file']) != page['sha256']:
                raise RuntimeError('Sealed Cloud page changed')
            with gzip.open(cloud / page['file'], 'rt') as stream:
                for line in stream:
                    uid, record = json.loads(line)
                    if str(uuid.UUID(uid)) != uid or uid <= previous_uuid:
                        raise RuntimeError('Cloud UUID repeated or global order changed')
                    previous_uuid = uid
                    try:
                        db.execute('INSERT INTO seen_cloud(uuid) VALUES (?)', (uid,))
                    except sqlite3.IntegrityError:
                        raise RuntimeError('Cloud UUID repeated') from None
                    cloud_rows += 1
                    row = db.execute('SELECT event,matched FROM source WHERE uuid=?', (uid,)).fetchone()
                    if not row:
                        continue
                    if row[1]:
                        raise RuntimeError('Cloud logical UUID repeated across pages')
                    if cloud_fields_match(json.loads(row[0]), record):
                        db.execute('UPDATE source SET matched=1 WHERE uuid=?', (uid,))
                    else:
                        mismatches.append(uid)
            db.commit()
        missing = [row[0] for row in db.execute('SELECT uuid FROM source WHERE matched=0')]
        if cloud_rows != cloud_manifest['rows']:
            raise RuntimeError('Cloud row count differs from sealed manifest')
        if file_sha(ingress_config) != config_sha:
            raise RuntimeError('Configured source/Cloud mappings changed during verification')
        atomic_json(output / 'unmatched-uuids.private.json', {'unmatched': missing, 'field_mismatches': mismatches})
        result = {'state': 'verified' if not missing and not mismatches else 'failed', 'environment': environment,
                  'source_project_id': source_project, 'cloud_project_id': cloud_manifest['cloud_project_id'],
                  'api_host': cloud_manifest['api_host'], 'ingress_config_sha256': config_sha,
                  'source_rows': source_rows, 'cloud_logical_rows': cloud_rows,
                  'matched_source_uuid_headers_timestamps_typed_properties_sessions': source_rows - len(missing),
                  'missing_or_different': len(missing), 'field_mismatches': len(mismatches),
                  'source_manifest_sha256': file_sha(snapshot / 'manifest.json'),
                  'cloud_manifest_sha256': file_sha(cloud / 'manifest.json'),
                  'events_sent': 0, 'persons_mutated': 0,
                  'profile_membership_and_recording_playback_verified': False}
        atomic_json(output / 'result.json', result)
        return result
    finally:
        journal.close()
        db.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    modes = parser.add_subparsers(dest='mode', required=True)
    export = modes.add_parser('export')
    export.add_argument('--credentials', type=Path, required=True)
    export.add_argument('--environment', choices=['prod', 'dev'], required=True)
    export.add_argument('--output', type=Path, required=True)
    verify = modes.add_parser('verify')
    verify.add_argument('--snapshot', type=Path, required=True)
    verify.add_argument('--cloud', type=Path, required=True)
    verify.add_argument('--environment', choices=['prod', 'dev'], required=True)
    verify.add_argument('--ingress-config', type=Path, required=True)
    verify.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.mode == 'export':
        result = export_cloud(args.credentials, args.environment, args.output)
        print(json.dumps({'state': result['state'], 'rows': result['rows']}))
    else:
        result = verify_snapshot(args.snapshot, args.cloud, args.environment, args.ingress_config, args.output)
        print(json.dumps(result))
        if result['state'] != 'verified':
            raise SystemExit(1)


if __name__ == '__main__':
    main()
