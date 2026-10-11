#!/usr/bin/env python3
"""Synthetic DEV-only end-to-end precision and same-UUID repair check."""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid

from import_amplitude_events import atomic_json, json_values_equal

ROOT = Path(__file__).resolve().parent
CREDENTIALS = Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json')
CH = 'kasanova_posthog-clickhouse-1'


def main(evidence, corrected):
    os.umask(0o077)
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    credentials = json.loads(CREDENTIALS.read_text())
    dev = credentials['projects']['dev']
    if dev['id'] == credentials['projects']['prod']['id']:
        raise RuntimeError('DEV/PROD isolation is missing')
    phase = 'corrected' if corrected else 'baseline'
    capture = json.loads(subprocess.check_output(['docker', 'inspect', 'kasanova_posthog-capture-1']))[0]
    if capture['Config']['Labels'].get('com.docker.compose.project') != 'kasanova_posthog':
        raise RuntimeError('Unexpected capture ownership')
    if corrected and capture['Config']['Labels'].get('io.kasanova.capture.float-roundtrip') != 'true':
        raise RuntimeError('Corrected image is not running')
    source = evidence / 'synthetic-events.json'
    if source.exists():
        events = json.loads(source.read_text())
    else:
        if corrected: raise RuntimeError('Run and preserve baseline first')
        stamp = dt.datetime.now(dt.timezone.utc).isoformat()
        events = []
        for divisor in (1013, 9973, 10000019):
            for numerator in range(1, 201):
                identity = f'{divisor}:{numerator}'
                events.append({'uuid': str(uuid.uuid5(uuid.NAMESPACE_URL, 'kasanova-posthog-precision-20261010:' + identity)),
                    'event': 'posthog_precision_validation', 'timestamp': stamp,
                    'distinct_id': 'posthog-precision-validation:' + identity,
                    'properties': {'fraction': numerator / divisor, 'nested': {'fraction': numerator / divisor},
                        '__amplitude_validation_fixture': True, '$geoip_disable': True,
                        '$process_person_profile': False}})
        atomic_json(source, events)
    marker = evidence / (phase + '-capture.json')
    if not marker.exists():
        payload_events = [dict(e, properties=dict(e['properties'], precision_validation_phase=phase)) for e in events]
        request = urllib.request.Request(credentials['url'] + '/batch/',
            data=json.dumps({'api_key': dev['api_key'], 'historical_migration': True, 'batch': payload_events}, allow_nan=False).encode(),
            headers={'Content-Type': 'application/json', 'User-Agent': 'Kasanova-DEV-precision-validation'})
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.loads(response.read())
            if response.status != 200 or result.get('status') not in (1, 'Ok'):
                raise RuntimeError('Synthetic capture was not acknowledged')
        atomic_json(marker, {'acknowledged': True, 'records': len(events), 'phase': phase})
    ids = ','.join("'" + str(uuid.UUID(e['uuid'])) + "'" for e in events)
    def query(project, final=False):
        sql = ('SELECT uuid,properties FROM posthog.sharded_events ' + ('FINAL ' if final else '')
            + 'WHERE team_id=' + str(int(project)) + ' AND uuid IN (' + ids + ') FORMAT JSONEachRow')
        return [json.loads(line) for line in subprocess.check_output(['docker', 'exec', CH,
            'clickhouse-client', '--query', sql]).splitlines()]
    for attempt in range(180):
        rows = query(dev['id'], final=True)
        if len(rows) == len(events) and all(json.loads(r['properties']).get('precision_validation_phase') == phase for r in rows):
            break
        time.sleep(2)
    else: raise RuntimeError('Synthetic events did not persist the current phase')
    expected = {e['uuid']: e for e in events}
    changed = 0
    seen = set()
    for row in rows:
        if row['uuid'] in seen: raise RuntimeError('Duplicate logical fixture UUID')
        seen.add(row['uuid'])
        p = json.loads(row['properties'])
        e = expected[row['uuid']]['properties']
        if not json_values_equal(p['fraction'], e['fraction']) or not json_values_equal(p['nested'], e['nested']):
            changed += 1
    if seen != set(expected): raise RuntimeError('Synthetic UUID set differs')
    if query(credentials['projects']['prod']['id']): raise RuntimeError('Synthetic fixture leaked into PROD')
    physical = len(query(dev['id']))
    report = {'verified_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'phase': phase,
        'synthetic_only': True, 'source_connector_changed': False, 'target_project_id': dev['id'],
        'logical_records': len(rows), 'physical_records': physical,
        'numeric_value_changes': changed, 'prod_isolation_verified': True,
        'same_uuid_replacement_verified': corrected,
        'running_capture_image_id': capture['Image']}
    atomic_json(evidence / (phase + '-verification.json'), report)
    print(json.dumps(report), flush=True)
    if corrected and changed: raise RuntimeError('Corrected ingestion still changes numeric values')
    if not corrected and not changed: raise RuntimeError('Baseline did not reproduce the ingestion precision issue')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path)
    parser.add_argument('--corrected', action='store_true')
    args = parser.parse_args()
    main(args.evidence, args.corrected)
