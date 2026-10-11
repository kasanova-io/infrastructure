#!/usr/bin/env python3
"""Prepare a private, chronological, reversible native import; never sends events."""
import argparse
import collections
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import uuid
import zipfile

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
RAW = '__amplitude_original_record'
SHA = '__amplitude_record_sha256'
USER = '__amplitude_user_properties'
RESERVED = {RAW, SHA, USER, '$set', '$session_id', '$device_id', '$geoip_disable', '$ip'}

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(1024*1024), b''): h.update(b)
    return h.hexdigest()

def time_us(value):
    t = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if t.tzinfo is None: t = t.replace(tzinfo=dt.timezone.utc)
    delta = t.astimezone(dt.timezone.utc) - EPOCH
    return (delta.days*86400 + delta.seconds)*1000000 + delta.microseconds

def session_key(event):
    value = event.get('session_id')
    if value is None or int(value) < 0: return None
    return json.dumps([str(event['app']), event.get('device_id'), str(value)], separators=(',', ':'))

def session_uuid(key, first_us):
    source_ms = int(json.loads(key)[2])
    milliseconds = min(source_ms, first_us//1000)
    if not 0 <= milliseconds < 2**48: raise ValueError('Invalid source session timestamp')
    entropy = int.from_bytes(hashlib.sha256(key.encode()).digest()[:10], 'big')
    value = (milliseconds << 80) | (7 << 76) | (((entropy >> 64) & 0xfff) << 64) | (2 << 62) | (entropy & ((1 << 62)-1))
    return str(uuid.UUID(int=value))

def convert(raw, identity, sessions):
    event = json.loads(raw)
    aid = str(event['amplitude_id'])
    canonical = identity['canonical_map'][aid]
    distinct = identity['user_ids'].get(canonical) or 'amplitude:' + str(event['app']) + ':' + canonical
    if not isinstance(distinct, str) or not 0 < len(distinct) <= 200:
        raise ValueError('Distinct ID would be empty or truncated')
    if not isinstance(event['event_type'], str) or not event['event_type'] or event['event_type'].startswith('$'):
        raise ValueError('Invalid or vendor-reserved event name')
    # Preserve event property values and types; metadata lives in a checked namespace.
    properties = event.get('event_properties') or {}
    user_properties = event.get('user_properties') or {}
    if not isinstance(properties, dict) or not isinstance(user_properties, dict):
        raise ValueError('Source properties are not objects')
    if RESERVED.intersection(properties): raise ValueError('Native metadata would overwrite a source property')
    properties = dict(properties)
    properties.update({RAW: raw.decode('utf-8'), SHA: hashlib.sha256(raw).hexdigest(),
                       USER: user_properties, '$set': user_properties, '$geoip_disable': True})
    if event.get('device_id') is not None: properties['$device_id'] = event['device_id']
    if event.get('ip_address') is not None: properties['$ip'] = event['ip_address']
    key = session_key(event)
    if key is not None: properties['$session_id'] = session_uuid(key, sessions[key])
    timestamp = (EPOCH + dt.timedelta(microseconds=time_us(event['event_time']))).isoformat()
    source_uuid = str(uuid.UUID(event['uuid']))
    if source_uuid != event['uuid']: raise ValueError('Source UUID would need normalization')
    return {'event': event['event_type'], 'distinct_id': distinct, 'uuid': source_uuid,
            'timestamp': timestamp, 'properties': properties}

def entries(component):
    summary = json.loads((component/'prod/events-summary.json').read_text())
    if not summary['coverage_complete']: raise ValueError('Incomplete source upload coverage')
    total = 0
    for w in summary['windows']:
        if w['status'] == 404: continue
        if w['status'] != 200: raise ValueError('Unsuccessful source export window')
        path = component/'prod'/w['path']
        if digest(path) != w['sha256']: raise ValueError('Original export checksum mismatch')
        count = 0
        with zipfile.ZipFile(path) as z:
            for name in z.namelist():
                if name.endswith('/'): continue
                with z.open(name) as stream:
                    decoded = gzip.GzipFile(fileobj=stream) if name.endswith('.gz') else stream
                    try:
                        for raw in decoded:
                            if not raw.strip(): continue
                            count += 1
                            yield raw
                    finally:
                        if decoded is not stream: decoded.close()
        if count != w['records']: raise ValueError('Export window record receipt differs')
        total += count
    if total != summary['records']: raise ValueError('Source component count differs')

def prepare(components, identity_path, destination):
    os.umask(0o077)
    if destination.exists(): raise ValueError('Import destination must be new')
    if shutil.disk_usage(destination.parent).free < 3*1024**3: raise ValueError('Need 3 GiB temporary space')
    identity = json.loads(identity_path.read_text())
    if identity.get('missing_profile_ids'): raise ValueError('Source identity profiles incomplete')
    destination.mkdir(mode=0o700)
    database = destination/'index.sqlite'
    con = sqlite3.connect(database)
    con.execute('CREATE TABLE events (uuid TEXT PRIMARY KEY, t INTEGER, raw BLOB)')
    sessions = {}
    source_projects = set()
    count = 0
    try:
        for component in components:
            for raw in entries(component):
                e = json.loads(raw); t = time_us(e['event_time'])
                source_projects.add(str(e['app']))
                if len(source_projects)>1: raise ValueError('Cross-project identity mapping is not permitted')
                con.execute('INSERT INTO events VALUES (?, ?, ?)', (e['uuid'], t, raw))
                key = session_key(e)
                if key is not None: sessions[key] = min(sessions.get(key, t), t)
                count += 1
                if count % 10000 == 0: con.commit()
        con.commit()
        con.execute('CREATE INDEX chronology ON events(t,uuid)'); con.commit()
        all_path = destination/'events.ndjson.gz'
        sample_path = destination/'validation-sample.ndjson'
        selected = set(); sample_count = 0; names = collections.Counter(); latest = None
        with gzip.GzipFile(filename=str(all_path), mode='wb', mtime=0) as out, sample_path.open('wb') as sample:
            for _, _, raw in con.execute('SELECT uuid,t,raw FROM events ORDER BY t,uuid'):
                native = convert(raw, identity, sessions)
                # Reverse the envelope independently and compare original typed properties.
                source = json.loads(raw)
                if json.loads(native['properties'][RAW]) != source: raise ValueError('Original record roundtrip failed')
                for k,v in source.get('event_properties',{}).items():
                    if native['properties'].get(k) != v: raise ValueError('Source property value changed')
                line = json.dumps(native,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()+b'\n'
                out.write(line);names[native['event']]+=1;latest=line
                if native['event'] not in selected:
                    selected.add(native['event']); sample.write(line);sample_count+=1
            # Include the newest event as a boundary probe, without duplicate UUIDs.
            if latest and json.loads(latest)['event'] in selected:
                latest_uuid=json.loads(latest)['uuid']
                sample.flush()
                if latest_uuid not in {json.loads(x)['uuid'] for x in sample_path.read_bytes().splitlines()}:
                    sample.write(latest);sample_count+=1
        report = {'created_at':dt.datetime.now(dt.timezone.utc).isoformat(),
                  'state':'prepared_not_imported','source_components':[str(x) for x in components],
                  'source_component_manifests':{str(x):digest(x/'SHA256SUMS') for x in components},
                  'identity_map_sha256':digest(identity_path),'source_project_ids':sorted(source_projects),
                  'records':count,'native_event_types':len(names),'session_ids_mapped':len(sessions),
                  'validation_sample_records':sample_count,'original_record_bytes_embedded':True,
                  'source_event_names_and_properties_unchanged':True,'chronological':True,
                  'events_sha256':digest(all_path),'sample_sha256':digest(sample_path),
                  'historical_migration_required':True,'target_project':None,
                  'current_profile_import_and_future_client_aliasing_verified':False}
        (destination/'preparation.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({k:report[k] for k in ['state','records','native_event_types','session_ids_mapped','validation_sample_records']}))
    finally:
        con.close()
        if database.exists(): database.unlink()

if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--component',type=Path,action='append',required=True)
    p.add_argument('--identity-map',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();prepare(a.component,a.identity_map,a.output)
