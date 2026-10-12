#!/usr/bin/env python3
"""Export native PostHog history and metadata without modifying the live source.

Each private snapshot is immutable. This observes a live source rather than
claiming an atomic database checkpoint; the final migration must reconcile
subsequent events and journal records separately.
"""
import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess

from import_amplitude_events import atomic_json


def remote_stream(host, program, destination, compressed=True):
    partial = destination.with_suffix(destination.suffix + '.partial')
    if destination.exists() or partial.exists():
        raise RuntimeError('Preserve existing snapshot files; choose a new snapshot directory')
    process = subprocess.Popen(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                                host, 'python3 -'], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.stdin.write(program.encode())
    process.stdin.close()
    try:
        with (gzip.open(partial, 'wb') if compressed else partial.open('wb')) as output:
            while block := process.stdout.read(1024 * 1024):
                output.write(block)
        error = process.stderr.read()
        if process.wait() != 0:
            # Errors may include private source values. Keep them in the snapshot.
            private = destination.with_suffix(destination.suffix + '.error')
            private.write_bytes(error)
            private.chmod(0o600)
            raise RuntimeError('Source export failed; private error and partial bytes retained')
        partial.chmod(0o600)
        partial.replace(destination)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait()
        process.stdout.close()
        process.stderr.close()


def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def event_inventory(path):
    counts, first, last, rows = {}, None, None, 0
    db = sqlite3.connect(':memory:')
    try:
        db.execute('CREATE TABLE ids(uuid TEXT PRIMARY KEY)')
        with gzip.open(path, 'rt') as stream:
            for line in stream:
                item = json.loads(line)
                db.execute('INSERT INTO ids VALUES (?)', (item['uuid'],))
                properties = json.loads(item['properties'])
                if not isinstance(properties, dict) or not isinstance(item['distinct_id'], str):
                    raise RuntimeError('Invalid native source event')
                stamp = int(item['timestamp_us'])
                # ClickHouse's UUID type order is not its textual hex order.
                # Equal occurrence times do not imply a source ordering error.
                if last is not None and stamp < last:
                    raise RuntimeError('Native source is not in chronological order')
                first = first if first is not None else stamp
                last = stamp
                rows += 1
                counts[item['event']] = counts.get(item['event'], 0) + 1
        return {'rows': rows, 'unique_uuids': rows, 'event_counts': counts,
                'earliest_timestamp_us': first, 'latest_timestamp_us': last}
    finally:
        db.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-ssh', required=True)
    parser.add_argument('--prod-project', type=int, required=True)
    parser.add_argument('--dev-project', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    if args.prod_project <= 0 or args.dev_project <= 0 or args.prod_project == args.dev_project:
        raise RuntimeError('Source PROD and DEV projects must be distinct positive IDs')
    args.output.mkdir(parents=True, mode=0o700, exist_ok=args.resume)
    manifest = {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'source_ssh': args.source_ssh, 'state': 'exporting', 'projects': {},
                'source_mutated': False, 'atomic_checkpoint': False, 'files': {}}
    if args.resume:
        manifest = json.loads((args.output / 'manifest.json').read_text())
        if manifest['state'] != 'exporting' or manifest['source_ssh'] != args.source_ssh:
            raise RuntimeError('Only the same incomplete source export can resume')
        for name, receipt in manifest['files'].items():
            if file_sha(args.output / name) != receipt['sha256']:
                raise RuntimeError('Existing source snapshot changed')
    atomic_json(args.output / 'manifest.json', manifest)
    for environment, project in [('prod', args.prod_project), ('dev', args.dev_project)]:
        events = args.output / (environment + '-events.ndjson.gz')
        # Read one monthly partition at a time: the original properties include
        # complete source envelopes and an all-history sort exceeds our budget.
        months_sql = ('SELECT DISTINCT toYYYYMM(timestamp) FROM posthog.sharded_events '
                      'WHERE team_id=' + str(project) + ' ORDER BY 1 FORMAT TabSeparated')
        sql = ('SELECT uuid,event,distinct_id,toUnixTimestamp64Micro(timestamp) AS timestamp_us,properties '
               'FROM posthog.sharded_events FINAL WHERE team_id=' + str(project) +
               ' AND toYYYYMM(timestamp)={month} ORDER BY timestamp,uuid FORMAT JSONEachRow')
        program = ('import subprocess,sys\n'
                   "command=['docker','exec','kasanova_posthog-clickhouse-1','clickhouse-client',"
                   "'--max_memory_usage','1073741824','--query']\n"
                   'months=subprocess.check_output(command+[' + repr(months_sql) + ']).decode().splitlines()\n'
                   'for value in months:\n'
                   ' month=int(value)\n'
                   ' sql=' + repr(sql) + '.format(month=month)\n'
                   ' result=subprocess.run(command+[sql],stdout=sys.stdout.buffer)\n'
                   ' if result.returncode:sys.exit(result.returncode)\n')
        if not events.exists():
            remote_stream(args.source_ssh, program, events)
        manifest['projects'][environment] = {'source_project_id': project, **event_inventory(events)}
        manifest['files'][events.name] = {'sha256': file_sha(events), 'bytes': events.stat().st_size}
        atomic_json(args.output / 'manifest.json', manifest)
        print(json.dumps({'exported': environment, 'rows': manifest['projects'][environment]['rows']}), flush=True)

        metadata = args.output / (environment + '-metadata.json.gz')
        program = '''import json,sys
from pathlib import Path
sys.path.insert(0,'/home/ren/Kasanova/tools/posthog')
from recreate_analytics_cohorts import Client
c=Client(Path('/tmp'))
project=PROJECT
result={'project':c.request(f'/api/projects/{project}/'), 'collections':{}}
for collection in ['actions','cohorts','dashboards','insights','persons','event_definitions','property_definitions','feature_flags','experiments','annotations']:
    path=f'/api/projects/{project}/{collection}/?limit=100'
    rows=[]
    visited=set()
    while path:
        if path in visited: raise RuntimeError('Metadata pagination repeated')
        visited.add(path)
        response=c.request(path)
        rows.extend(response.get('results',[]))
        next_path=response.get('next')
        if next_path and not next_path.startswith(c.base+'/'): raise RuntimeError('Unexpected pagination origin')
        path=next_path[len(c.base):] if next_path else None
    result['collections'][collection]=rows
json.dump(result,sys.stdout,ensure_ascii=False)
'''.replace('PROJECT', str(project))
        if not metadata.exists():
            remote_stream(args.source_ssh, program, metadata)
        with gzip.open(metadata, 'rt') as stream:
            data = json.load(stream)
        manifest['projects'][environment]['metadata_counts'] = {
            collection: len(rows) for collection, rows in data['collections'].items()}
        manifest['files'][metadata.name] = {'sha256': file_sha(metadata), 'bytes': metadata.stat().st_size}
        atomic_json(args.output / 'manifest.json', manifest)
    journal = args.output / 'ingress.sqlite'
    code = ("import sqlite3,sys; source=sqlite3.connect('file:/var/lib/analytics/ingress.sqlite?mode=ro',uri=True); "
            "target=sqlite3.connect(':memory:'); source.backup(target); "
            'sys.stdout.buffer.write(target.serialize()); target.close(); source.close()')
    program = ('import subprocess,sys\n'
               "sys.exit(subprocess.run(['docker','exec','kasanova_posthog-analytics-ingress-1',"
               "'python3','-c'," + repr(code) + '],stdout=sys.stdout.buffer).returncode)\n')
    if not journal.exists():
        remote_stream(args.source_ssh, program, journal, compressed=False)
    with sqlite3.connect(journal.resolve().as_uri() + '?mode=ro', uri=True) as con:
        if con.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
            raise RuntimeError('Exported journal failed integrity check')
        manifest['journal_records'] = con.execute('SELECT project,sent,count(*) FROM records GROUP BY project,sent').fetchall()
        manifest['journal_history'] = con.execute('SELECT project,count(*) FROM history GROUP BY project').fetchall()
        manifest['journal_identities'] = con.execute('SELECT project,count(*) FROM identities GROUP BY project').fetchall()
    manifest['files'][journal.name] = {'sha256': file_sha(journal), 'bytes': journal.stat().st_size}
    manifest.update(state='source_export_complete', completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                    limitation='Live component snapshots; final source-to-Cloud delta reconciliation is required')
    atomic_json(args.output / 'manifest.json', manifest)
    print(json.dumps({'state': manifest['state'], 'files': len(manifest['files'])}))


if __name__ == '__main__':
    main()
