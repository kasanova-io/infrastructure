#!/usr/bin/env python3
"""DEV-only synthetic check of explicit missing source IP handling."""
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid
from import_amplitude_events import atomic_json

ROOT = Path(__file__).resolve().parent
CREDENTIALS = Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json')


def main():
    os.umask(0o077)
    evidence = ROOT / 'operations/missing-ip-validation'
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    credentials = json.loads(CREDENTIALS.read_text())
    dev = credentials['projects']['dev']
    fixtures = evidence / 'events.json'
    if not fixtures.exists():
        events = []
        for name, present, value in [('omitted', False, None), ('null', True, None), ('false', True, False), ('empty', True, '')]:
            p = {'case': name, '$geoip_disable': True, '$process_person_profile': False, '__amplitude_validation_fixture': True}
            if present: p['$ip'] = value
            events.append({'uuid': str(uuid.uuid4()), 'event': 'posthog_missing_ip_validation',
                'distinct_id': 'posthog-missing-ip-validation:' + name,
                'timestamp': dt.datetime.now(dt.timezone.utc).isoformat(), 'properties': p})
        atomic_json(fixtures, events)
    events = json.loads(fixtures.read_text())
    if not (evidence / 'capture.json').exists():
        request = urllib.request.Request(credentials['url'] + '/batch/',
            data=json.dumps({'api_key': dev['api_key'], 'historical_migration': True, 'batch': events}).encode(),
            headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.loads(response.read())
            if response.status != 200 or result.get('status') not in ('Ok', 1): raise RuntimeError('Capture rejected')
        atomic_json(evidence / 'capture.json', {'acknowledged': True, 'records': len(events)})
    ids = ','.join("'" + str(uuid.UUID(e['uuid'])) + "'" for e in events)
    sql = 'SELECT properties FROM posthog.sharded_events FINAL WHERE team_id=' + str(int(dev['id'])) + ' AND uuid IN (' + ids + ') FORMAT JSONEachRow'
    for attempt in range(90):
        rows = [json.loads(line) for line in subprocess.check_output(['docker', 'exec', 'kasanova_posthog-clickhouse-1', 'clickhouse-client', '--query', sql]).splitlines()]
        if len(rows) == len(events): break
        time.sleep(2)
    else: raise RuntimeError('Missing-IP fixtures not queryable')
    result = []
    for row in rows:
        p = json.loads(row['properties'])
        result.append({'case': p['case'], 'ip_present': '$ip' in p,
            'ip_type': type(p.get('$ip')).__name__, 'ip_is_null': p.get('$ip') is None,
            'ip_is_false': p.get('$ip') is False})
    atomic_json(evidence / 'verification.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__': main()
