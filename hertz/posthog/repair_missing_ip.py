#!/usr/bin/env python3
"""Preserve four absent source IPs using the official historical queue envelope."""
import argparse
import datetime as dt
import fcntl
import gzip
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

from import_amplitude_events import atomic_json, compare, json_values_equal
from prepare_amplitude_import import RAW, digest, time_us

ROOT = Path(__file__).resolve().parent
CREDENTIALS = Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json')
SOURCE = Path('/home/ren/kasanova-archives/posthog-native-imports/20261010T18')
PROD_EVIDENCE = ROOT / 'operations/amplitude-prod-import-20261010T18'
EVIDENCE = ROOT / 'operations/missing-ip-queue-repair'
TOPIC = 'events_plugin_ingestion_historical'
KAFKA = 'kasanova_posthog-kafka-1'
CH = 'kasanova_posthog-clickhouse-1'


def main(prod):
    os.umask(0o077)
    credentials = json.loads(CREDENTIALS.read_text())
    target = credentials['projects']['prod' if prod else 'dev']
    if credentials['projects']['prod']['id'] == credentials['projects']['dev']['id']:
        raise RuntimeError('Environment isolation missing')
    evidence = PROD_EVIDENCE if prod else EVIDENCE
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    kafka = json.loads(subprocess.check_output(['docker', 'inspect', KAFKA]))[0]
    if kafka['Config']['Labels'].get('com.docker.compose.project') != 'kasanova_posthog':
        raise RuntimeError('Unexpected Kafka ownership')
    with (ROOT / 'operations/.backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if prod:
            proof = json.loads((EVIDENCE / 'queue-repair-verification.json').read_text())
            if proof.get('missing_ip_preserved_through_real_ingestion') is not True:
                raise RuntimeError('DEV missing-IP repair is not verified')
            if (PROD_EVIDENCE / 'reconciliation.json').exists():
                raise RuntimeError('Do not change an accepted backfill')
            preparation = json.loads((SOURCE / 'preparation.json').read_text())
            state = json.loads((PROD_EVIDENCE / 'import-state.json').read_text())
            if (preparation['records'] != 556777 or preparation['source_project_ids'] != ['734469']
                or preparation['events_sha256'] != state['source_sha256']
                or state['acknowledged_records'] != preparation['records']
                or state['target_project_id'] != target['id']
                or digest(SOURCE / 'events.ndjson.gz') != preparation['events_sha256']):
                raise RuntimeError('Recorded complete unaccepted source differs')
            events = []
            with gzip.open(SOURCE / 'events.ndjson.gz', 'rb') as stream:
                for line in stream:
                    e = json.loads(line)
                    if '$ip' not in e['properties']:
                        if json.loads(e['properties'][RAW]).get('ip_address') is not None:
                            raise RuntimeError('Expected an absent source IP')
                        events.append(e)
            if len(events) != 4: raise RuntimeError('Unexpected missing-IP source count')
        else:
            events = [e for e in json.loads((ROOT / 'operations/missing-ip-validation/events.json').read_text())
                if e['properties']['case'] == 'omitted']
            if len(events) != 1 or '$ip' in events[0]['properties']:
                raise RuntimeError('Unexpected synthetic missing-IP fixture')
        marker = evidence / 'missing-ip-queue-checkpoint.json'
        checkpoint = json.loads(marker.read_text()) if marker.exists() else {'acknowledged_uuids': [], 'prod': prod}
        if checkpoint['prod'] != prod: raise RuntimeError('Repair environment differs')
        atomic_json(marker, checkpoint)
        for e in events:
            if e['uuid'] in checkpoint['acknowledged_uuids']: continue
            now = dt.datetime.now(dt.timezone.utc).isoformat()
            # Matches CapturedEvent + headers in the installed capture source.
            # Empty transport IP preserves the source absence; no fake address.
            envelope = {'uuid': e['uuid'], 'distinct_id': e['distinct_id'], 'ip': '',
                'data': json.dumps(e, ensure_ascii=False, separators=(',', ':'), allow_nan=False),
                'now': now, 'token': target['api_key'], 'event': e['event'],
                'timestamp': e['timestamp'], 'historical_migration': True}
            command = ['docker', 'exec', '-i', KAFKA, 'rpk', 'topic', 'produce', TOPIC,
                '-k', target['api_key'] + ':' + e['distinct_id']]
            headers = {'token': target['api_key'], 'distinct_id': e['distinct_id'],
                'uuid': e['uuid'], 'event': e['event'], 'timestamp': str(time_us(e['timestamp']) // 1000),
                'now': now, 'historical_migration': 'true'}
            if e['properties'].get('$session_id'): headers['session_id'] = e['properties']['$session_id']
            for k, value in headers.items(): command.extend(['-H', k + ':' + value])
            result = subprocess.run(command, input=(json.dumps(envelope, ensure_ascii=False, allow_nan=False) + '\n').encode(),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode:
                (evidence / 'queue-produce-error-private.log').write_bytes(result.stderr)
                raise RuntimeError('Historical queue delivery stopped; inspect private evidence')
            checkpoint['acknowledged_uuids'].append(e['uuid'])
            atomic_json(marker, checkpoint)
        ids = ','.join("'" + str(uuid.UUID(e['uuid'])) + "'" for e in events)
        sql = ('SELECT uuid,event,distinct_id,toUnixTimestamp64Micro(timestamp) AS timestamp_us,properties '
            'FROM posthog.sharded_events FINAL WHERE team_id=' + str(int(target['id'])) + ' AND uuid IN (' + ids + ') FORMAT JSONEachRow')
        for attempt in range(90):
            rows = [json.loads(line) for line in subprocess.check_output(['docker', 'exec', CH, 'clickhouse-client', '--query', sql]).splitlines()]
            if len(rows) == len(events) and all('$ip' not in json.loads(r['properties']) for r in rows): break
            time.sleep(2)
        else: raise RuntimeError('Missing source IPs were not preserved by ingestion')
        expected = {e['uuid']: e for e in events}
        if {r['uuid'] for r in rows} != set(expected): raise RuntimeError('Repair UUID set differs')
        for row in rows:
            e = expected[row['uuid']]
            if prod: compare(row, e)
            else:
                p = json.loads(row['properties'])
                if row['event'] != e['event'] or row['distinct_id'] != e['distinct_id'] or int(row['timestamp_us']) != time_us(e['timestamp']):
                    raise RuntimeError('DEV fixture identity/name/time changed')
                if any(not json_values_equal(p.get(k), v) for k, v in e['properties'].items()):
                    raise RuntimeError('DEV fixture properties changed')
        report = {'verified_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'prod': prod,
            'records': len(rows), 'target_project_id': target['id'],
            'missing_ip_preserved_through_real_ingestion': True, 'original_event_fields_unchanged': True,
            'historical_queue_topic': TOPIC, 'source_connector_changed': False,
            'full_prod_reconciliation_still_required': prod}
        atomic_json(evidence / 'queue-repair-verification.json', report)
        print(json.dumps(report), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prod', action='store_true')
    args = parser.parse_args()
    main(args.prod)
