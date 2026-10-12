#!/usr/bin/env python3
"""Prepare unchanged native events for Cloud; no network or source mutations."""
import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path

from export_cloud_source import event_inventory, file_sha
from import_amplitude_events import atomic_json, canonical, json_values_equal
from prepare_amplitude_import import EPOCH, time_us


def envelope(row):
    props = json.loads(row['properties']) if isinstance(row['properties'], str) else row['properties']
    if not isinstance(props, dict):
        raise RuntimeError('Expected native property object')
    if not isinstance(row['distinct_id'], str) or not 0 < len(row['distinct_id']) <= 200:
        raise RuntimeError('Cloud would reject or truncate the source identity')
    if not isinstance(row['event'], str) or not row['event']:
        raise RuntimeError('Source event has no name')
    stamp = int(row['timestamp_us'])
    return {'uuid': row['uuid'], 'event': row['event'], 'distinct_id': row['distinct_id'],
            'timestamp': (EPOCH + dt.timedelta(microseconds=stamp)).isoformat(), 'properties': props}


ORIGINAL = '__kasanova_posthog_original_event'
ORIGINAL_SHA = '__kasanova_posthog_original_event_sha256'


def retain_original(event):
    if ORIGINAL in event['properties'] or ORIGINAL_SHA in event['properties']:
        raise RuntimeError('Original native envelope metadata collision')
    raw = canonical(event)
    return {**event, 'properties':{**event['properties'], ORIGINAL:raw,
            ORIGINAL_SHA:hashlib.sha256(raw.encode()).hexdigest()}}


def prepare(source, destination, prepared_at, retain_native_original=False):
    os.umask(0o077)
    manifest = json.loads((source / 'manifest.json').read_text())
    if manifest['state'] != 'source_export_complete':
        raise RuntimeError('Source snapshot has not completed')
    # Verify all exports, not just the event files: identities and metadata are
    # part of the migration and remain independently recoverable.
    for name, receipt in manifest['files'].items():
        if Path(name).name != name or file_sha(source / name) != receipt['sha256']:
            raise RuntimeError('Source snapshot checksum mismatch')
    destination.mkdir(parents=True, mode=0o700, exist_ok=False)
    boundary = time_us(prepared_at.isoformat()) - 48 * 3600 * 1000000
    result = {'state': 'prepared_not_imported', 'prepared_at': prepared_at.isoformat(),
              'source_manifest_sha256': file_sha(source / 'manifest.json'),
              'source_directory': str(source.resolve()), 'projects': {}}
    for environment, project in manifest['projects'].items():
        original = source / (environment + '-events.ndjson.gz')
        inventory = event_inventory(original)
        if inventory['rows'] != project['rows']:
            raise RuntimeError('Source event count differs from sealed manifest')
        output = destination / (environment + '-events.ndjson.gz')
        counts = {'historical_eligible': 0, 'recent': 0, 'rows': 0}
        with gzip.open(original, 'rt') as input_stream, gzip.GzipFile(filename=str(output), mode='wb', mtime=0) as stream:
            for line in input_stream:
                row = json.loads(line)
                event = envelope(row)
                if time_us(event['timestamp']) != int(row['timestamp_us']):
                    raise RuntimeError('Source occurrence time changed')
                props = json.loads(row['properties']) if isinstance(row['properties'], str) else row['properties']
                if not json_values_equal(event['properties'], props):
                    raise RuntimeError('Source native property values changed')
                if retain_native_original:
                    event = retain_original(event)
                encoded = canonical(event).encode() + b'\n'
                if len(encoded) > 10 * 1024**2:
                    raise RuntimeError('One native event exceeds the conservative Cloud batch limit')
                stream.write(encoded)
                counts['rows'] += 1
                counts['historical_eligible' if int(row['timestamp_us']) <= boundary else 'recent'] += 1
        output.chmod(0o600)
        result['projects'][environment] = {'source_project_id': project['source_project_id'],
                                         **counts, 'sha256': file_sha(output),
                                         'event_counts': inventory['event_counts']}
    result['person_property_catchup_required'] = True
    result['complete_original_native_envelope_embedded'] = retain_native_original
    result['live_first_sequence'] = ('Live routing is separately authorized first. Before history sends, '
                                    'protect newer Cloud person properties; reconcile them again afterward. '
                                    'Recent records use regular ingestion, not historical_migration=true.')
    atomic_json(destination / 'manifest.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--retain-native-original',action='store_true')
    args = parser.parse_args()
    receipt = prepare(args.source, args.output, dt.datetime.now(dt.timezone.utc),args.retain_native_original)
    print(json.dumps({'state': receipt['state'], 'projects': {
        env: {key: value[key] for key in ['rows', 'historical_eligible', 'recent']}
        for env, value in receipt['projects'].items()}}))
