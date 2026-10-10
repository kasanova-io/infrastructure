#!/usr/bin/env python3
"""Merge versions of a fully reconciled offline backfill; preserve all logical rows."""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

from import_amplitude_events import atomic_json

ROOT = Path(__file__).resolve().parent
CH = 'kasanova_posthog-clickhouse-1'


def main(evidence):
    os.umask(0o077)
    proof = json.loads((evidence / 'reconciliation.json').read_text())
    required = ['all_uuids_event_names_distinct_ids_and_microsecond_timestamps_verified',
        'all_original_record_bytes_hashes_and_typed_event_user_properties_verified',
        'all_native_session_device_ip_mappings_verified']
    if proof.get('source_connector_changed') is not False or any(proof.get(k) is not True for k in required):
        raise RuntimeError('Complete offline field reconciliation is required before merging versions')
    owned = json.loads(subprocess.check_output(['docker', 'inspect', CH]))[0]
    if owned['Config']['Labels'].get('com.docker.compose.project') != 'kasanova_posthog':
        raise RuntimeError('Unexpected ClickHouse ownership')
    credentials = json.loads(Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json').read_text())
    team = int(credentials['projects']['prod']['id'])
    if proof['target_project_id'] != team: raise RuntimeError('PROD proof differs from credentials')
    def query(sql):
        result = subprocess.run(['docker', 'exec', CH, 'clickhouse-client', '--query', sql],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode:
            (evidence / 'event-merge-error-private.log').write_bytes(result.stderr)
            raise RuntimeError('Owned event-table operation failed; inspect private evidence')
        return result.stdout
    def counts():
        return [json.loads(line) for line in query('SELECT team_id,count() AS rows,uniqExact(uuid) AS uuids '
            'FROM posthog.events GROUP BY team_id ORDER BY team_id FORMAT JSONEachRow').splitlines()]
    def fingerprint(final):
        # Sort fixed-size cryptographic row digests rather than complete private
        # properties. Bind names, identity, exact time and every property byte.
        sql = ('SELECT lower(hex(SHA256(toJSONString(tuple(uuid,event,distinct_id,'
            'toUnixTimestamp64Micro(timestamp),properties))))) AS digest FROM posthog.sharded_events '
            + ('FINAL ' if final else '') + 'WHERE team_id=' + str(team)
            + ' ORDER BY uuid SETTINGS max_threads=2,max_final_threads=2,max_block_size=2048,'
            'max_bytes_before_external_sort=268435456,max_memory_usage=1073741824 FORMAT TSV')
        result = query(sql)
        rows = result.splitlines()
        if len(rows) != proof['logical_prod_rows'] or any(re.fullmatch(rb'[0-9a-f]{64}', row) is None for row in rows):
            raise RuntimeError('Event fingerprint row set differs')
        return hashlib.sha256(result).hexdigest()
    with (ROOT / 'operations/.backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        schema = query('SHOW CREATE TABLE posthog.sharded_events').decode()
        if 'ReplicatedReplacingMergeTree' not in schema or '_timestamp' not in schema:
            raise RuntimeError('Unexpected event version engine')
        before = counts()
        source_rows = next(r for r in before if int(r['team_id']) == team)
        if int(source_rows['uuids']) != proof['logical_prod_rows']:
            raise RuntimeError('Persisted UUID count differs from reconciliation')
        initial = fingerprint(True)
        partitions = [json.loads(line)['partition'] for line in query('SELECT DISTINCT _partition_id AS partition '
            'FROM posthog.sharded_events WHERE team_id=' + str(team) + ' FORMAT JSONEachRow').splitlines()]
        if not partitions or any(re.fullmatch(r'[0-9]{6}', p) is None for p in partitions):
            raise RuntimeError('Unexpected monthly event partition')
        report = {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'before_counts': before,
            'source_reconciliation_sha256': hashlib.sha256((evidence / 'reconciliation.json').read_bytes()).hexdigest(),
            'logical_row_fingerprint_before': initial, 'partitions_merged': [], 'source_connector_changed': False}
        path = evidence / 'event-version-merge.json'
        atomic_json(path, report)
        for p in sorted(partitions):
            query('OPTIMIZE TABLE posthog.sharded_events PARTITION ' + p
                + ' FINAL SETTINGS max_threads=2,max_memory_usage=1073741824')
            report['partitions_merged'].append(p)
            atomic_json(path, report)
            print(json.dumps({'monthly_partition_merged': p}), flush=True)
        after = counts()
        if [{k:v for k,v in r.items() if k != 'rows'} for r in before] != [{k:v for k,v in r.items() if k != 'rows'} for r in after]:
            raise RuntimeError('Merging changed logical environment UUID counts')
        final = fingerprint(False)
        if final != initial: raise RuntimeError('Merging changed reconciled native event fields')
        report.update(completed_at=dt.datetime.now(dt.timezone.utc).isoformat(), after_counts=after,
            logical_row_fingerprint_after=final, all_reconciled_native_row_fields_preserved=True,
            all_environment_unique_uuid_counts_preserved=True)
        atomic_json(path, report)
        print(json.dumps({'event_version_merge_verified': True, 'prod_events': proof['logical_prod_rows'],
            'monthly_partitions': len(partitions), 'source_connector_changed': False}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path)
    args = parser.parse_args()
    main(args.evidence)
