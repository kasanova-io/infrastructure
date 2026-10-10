#!/usr/bin/env python3
"""Resumable offline PROD backfill, followed by complete persisted-field comparison."""
import argparse
import datetime as dt
from decimal import Decimal
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request

from prepare_amplitude_import import RAW, SHA, USER, digest, time_us

ROOT = Path(__file__).resolve().parent
CREDENTIALS = Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json')
CH = 'kasanova_posthog-clickhouse-1'


def atomic_json(path, data):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n')
    temporary.chmod(0o600)
    temporary.replace(path)


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def json_values_equal(left, right):
    # JSON has one numeric type. Preserve exact numeric value without tolerance;
    # permit 1 vs 1.0 while rejecting true vs 1 and even a one-step float change.
    if type(left) in (int, float) and type(right) in (int, float):
        return Decimal(str(left)) == Decimal(str(right))
    if type(left) is not type(right): return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(json_values_equal(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(json_values_equal(a, b) for a, b in zip(left, right))
    return left == right


def compare(native, expected):
    if native['event'] != expected['event'] or native['distinct_id'] != expected['distinct_id']:
        raise RuntimeError('Persisted event name or identity differs')
    if int(native['timestamp_us']) != time_us(expected['timestamp']):
        raise RuntimeError('Persisted timestamp differs')
    properties = json.loads(native['properties']) if isinstance(native['properties'], str) else native['properties']
    if properties[RAW] != expected['properties'][RAW] or properties[SHA] != expected['properties'][SHA]:
        raise RuntimeError('Persisted original record bytes or checksum differ')
    original = json.loads(properties[RAW])
    for key, value in original.get('event_properties', {}).items():
        if key not in properties or not json_values_equal(properties[key], value):
            raise RuntimeError('Persisted source event property value or type differs')
    if not json_values_equal(properties[USER], original.get('user_properties', {})):
        raise RuntimeError('Persisted event-time user properties differ')
    for key in ('$session_id', '$device_id', '$ip'):
        if not json_values_equal(properties.get(key), expected['properties'].get(key)):
            raise RuntimeError('Persisted native session/device/IP mapping differs')


def main(directory, evidence, verify_only=False, repair_unaccepted=False):
    os.umask(0o077)
    preparation = json.loads((directory / 'preparation.json').read_text())
    source = directory / 'events.ndjson.gz'
    if preparation['state'] != 'prepared_not_imported' or preparation['source_project_ids'] != ['734469']:
        raise RuntimeError('Unexpected prepared source project or state')
    if digest(source) != preparation['events_sha256']:
        raise RuntimeError('Prepared import bytes differ from manifest')
    credentials = json.loads(CREDENTIALS.read_text())
    prod = credentials['projects']['prod']
    team = int(prod['id'])
    if team == int(credentials['projects']['dev']['id']):
        raise RuntimeError('PROD and DEV project identifiers are equal')
    owned = json.loads(subprocess.check_output(['docker', 'inspect', CH]))[0]
    if owned['Config']['Labels'].get('com.docker.compose.project') != 'kasanova_posthog':
        raise RuntimeError('Unexpected ClickHouse ownership')
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    state_path = evidence / 'import-state.json'
    state = json.loads(state_path.read_text()) if state_path.exists() else {
        'started_at': dt.datetime.now(dt.timezone.utc).isoformat(),
        'source_sha256': preparation['events_sha256'], 'target_project_id': team,
        'expected_records': preparation['records'], 'acknowledged_records': 0,
        'source_connector_changed': False, 'state': 'prepared',
    }
    if state['source_sha256'] != preparation['events_sha256'] or state['target_project_id'] != team:
        raise RuntimeError('Resume source or target differs')
    if state['acknowledged_records'] > preparation['records']:
        raise RuntimeError('Invalid resume checkpoint')

    def query(sql):
        return json.loads(subprocess.check_output(['docker', 'exec', CH, 'clickhouse-client', '--query', sql + ' FORMAT JSON']))['data']

    def count():
        return int(query('SELECT uniqExact(uuid) AS n FROM posthog.events WHERE team_id=' + str(team))[0]['n'])

    with (ROOT / 'operations/.backup.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if repair_unaccepted:
            if verify_only or not state_path.exists() or (evidence / 'reconciliation.json').exists():
                raise RuntimeError('Repair requires a recorded, unaccepted backfill and no successful reconciliation')
            if state['acknowledged_records'] != preparation['records'] or count() != preparation['records']:
                raise RuntimeError('Repair requires the recorded complete event count in the unaccepted backfill')
            capture = json.loads(subprocess.check_output(['docker', 'inspect', 'kasanova_posthog-capture-1']))[0]
            if (capture['Config']['Labels'].get('com.docker.compose.project') != 'kasanova_posthog'
                or capture['Config']['Labels'].get('io.kasanova.capture.float-roundtrip') != 'true'
                or not capture['State']['Running']):
                raise RuntimeError('Repair requires the running, owned capture image with the precise parser')
            state.setdefault('repair_attempts', []).append({'started_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'previous_acknowledged_records': state['acknowledged_records'],
                'strategy': 'Reingest identical UUID/name/timestamp/distinct_id tuples after the parser correction; newer _timestamp versions replace prior values'})
            state.update(acknowledged_records=0, state='repair_prepared')
            atomic_json(state_path, state)
        if state['acknowledged_records'] == 0 and not state_path.exists() and count() != 0:
            raise RuntimeError('Fresh offline backfill requires empty PROD')
        index = evidence / 'verification.sqlite'
        # This disposable index has no external writers. Rebuild on each invocation.
        if index.exists(): index.unlink()
        con = sqlite3.connect(index)
        con.execute('CREATE TABLE expected (uuid TEXT PRIMARY KEY, envelope TEXT, seen INTEGER DEFAULT 0)')
        try:
            previous = None
            total = 0
            with gzip.open(source, 'rb') as stream:
                for line in stream:
                    item = json.loads(line)
                    chronology = (time_us(item['timestamp']), item['uuid'])
                    if previous is not None and chronology < previous:
                        raise RuntimeError('Prepared source is not chronological')
                    if item['properties'].get('__amplitude_validation_fixture'):
                        raise RuntimeError('PROD import contains a test fixture')
                    raw = item['properties'][RAW].encode('utf-8')
                    original = json.loads(raw)
                    if hashlib.sha256(raw).hexdigest() != item['properties'][SHA] or original['uuid'] != item['uuid']:
                        raise RuntimeError('Original source UUID or raw-record checksum differs')
                    con.execute('INSERT INTO expected(uuid,envelope) VALUES (?,?)', (item['uuid'], line.decode('utf-8')))
                    previous = chronology
                    total += 1
                    if total % 10000 == 0: con.commit()
            con.commit()
            if total != preparation['records']: raise RuntimeError('Prepared source record count differs')
            print(json.dumps({'state': 'full_source_preflight_passed', 'records': total}), flush=True)
            atomic_json(state_path, state)
            if not verify_only:
                skipped = 0
                batch = []

                def send(events):
                    payload = json.dumps({'api_key': prod['api_key'], 'historical_migration': True, 'batch': events},
                                         ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()
                    if len(payload) > 10 * 1024**2: raise RuntimeError('Batch exceeds conservative payload limit')
                    request = urllib.request.Request(credentials['url'] + '/batch/', data=payload,
                        headers={'Content-Type': 'application/json', 'User-Agent': 'Kasanova-offline-Amplitude-backfill'})
                    state.update(state='sending', pending_batch_records=len(events))
                    atomic_json(state_path, state)
                    try:
                        with urllib.request.urlopen(request, timeout=60) as response:
                            result = json.loads(response.read())
                            if response.status != 200 or result.get('status') not in (1, 'Ok'):
                                raise RuntimeError('Capture did not acknowledge the complete batch')
                    except Exception:
                        # Do not hide an ambiguous delivery. A resumed invocation uses
                        # the same UUID/event/timestamp/distinct_id tuple for deduplication.
                        state['state'] = 'delivery_uncertain_or_rejected'
                        atomic_json(state_path, state)
                        raise RuntimeError('Capture stopped; inspect private checkpoint before resuming') from None
                    state['acknowledged_records'] += len(events)
                    state.update(state='acknowledged', pending_batch_records=0)
                    atomic_json(state_path, state)
                    if state['acknowledged_records'] % 20000 == 0 or state['acknowledged_records'] == total:
                        print(json.dumps({'acknowledged_records': state['acknowledged_records'], 'expected_records': total}), flush=True)
                        # Bound outstanding ingestion work on the shared host.
                        for attempt in range(180):
                            if count() >= state['acknowledged_records'] - 5000: break
                            time.sleep(2)
                        else: raise RuntimeError('Ingestion backlog did not drain; import paused')

                with gzip.open(source, 'rb') as stream:
                    for line in stream:
                        skipped += 1
                        if skipped <= state['acknowledged_records']: continue
                        batch.append(json.loads(line))
                        if len(batch) == 1000:
                            send(batch); batch = []
                    if batch: send(batch)
            for attempt in range(180):
                if count() >= total: break
                time.sleep(2)
            else: raise RuntimeError('Acknowledged events are not fully queryable')
            state['state'] = 'verifying_persisted_fields'
            atomic_json(state_path, state)
            sql = ('SELECT uuid,event,distinct_id,toUnixTimestamp64Micro(timestamp) AS timestamp_us,properties '
                   'FROM posthog.sharded_events FINAL WHERE team_id=' + str(team) + ' FORMAT JSONEachRow')
            native = subprocess.Popen(['docker', 'exec', CH, 'clickhouse-client', '--query', sql], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            rows = 0
            try:
                for line in native.stdout:
                    value = json.loads(line)
                    row = con.execute('SELECT envelope,seen FROM expected WHERE uuid=?', (value['uuid'],)).fetchone()
                    if row is None or row[1]: raise RuntimeError('Unexpected or repeated logical PROD UUID')
                    compare(value, json.loads(row[0]))
                    con.execute('UPDATE expected SET seen=1 WHERE uuid=?', (value['uuid'],))
                    rows += 1
                    if rows % 20000 == 0: con.commit()
                if native.wait() != 0: raise RuntimeError('Persisted event query failed')
            finally:
                if native.poll() is None: native.terminate(); native.wait()
                native.stdout.close()
            con.commit()
            if rows != total or con.execute('SELECT count(*) FROM expected WHERE seen=0').fetchone()[0]:
                raise RuntimeError('Full native UUID set differs from prepared source')
            report = {'verified_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'source_sha256': preparation['events_sha256'], 'target_project_id': team,
                'logical_prod_rows': rows, 'expected_source_records': total,
                'all_uuids_event_names_distinct_ids_and_microsecond_timestamps_verified': True,
                'all_original_record_bytes_hashes_and_typed_event_user_properties_verified': True,
                'all_native_session_device_ip_mappings_verified': True,
                'json_number_values_compared_exactly_without_tolerance': True,
                'original_json_number_lexemes_preserved_in_original_record': True,
                'source_connector_changed': False,
                'scope': 'Offline retained-event backfill only; full archive, current profiles, future identities, reporting and replay remain separate',
                'physical_counts': query('SELECT count() AS rows,uniqExact(uuid) AS unique_uuids FROM posthog.events WHERE team_id=' + str(team))[0]}
            atomic_json(evidence / 'reconciliation.json', report)
            state.update(state='persisted_fields_verified', completed_at=report['verified_at'])
            atomic_json(state_path, state)
            print(json.dumps(report), flush=True)
        except BaseException as error:
            state['last_failure'] = {'at': dt.datetime.now(dt.timezone.utc).isoformat(),
                                     'stage': state['state'], 'error_type': type(error).__name__}
            atomic_json(state_path, state)
            raise
        finally:
            con.close()
            if index.exists(): index.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('prepared_directory', type=Path)
    parser.add_argument('evidence_directory', type=Path)
    parser.add_argument('--verify-only', action='store_true')
    parser.add_argument('--repair-unaccepted-backfill', action='store_true')
    args = parser.parse_args()
    main(args.prepared_directory, args.evidence_directory, args.verify_only, args.repair_unaccepted_backfill)
