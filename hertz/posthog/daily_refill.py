#!/usr/bin/env python3
"""Missing-event reconciliation: original Amplitude -> Hertz -> matching Cloud.

Export upload-time windows overlap. A successful watermark requires complete
source coverage and persisted native UUID/field read-back, never capture ACK alone.
Existing wallet identities and native PostHog identification events are retained.
"""
import argparse
import base64
from collections import defaultdict
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
from types import SimpleNamespace
import urllib.parse
import urllib.request

from archive_amplitude import Archive, HOUR
from analytics_ingress import Journal
from cloud_client import CloudClient
from import_amplitude_events import atomic_json, canonical, compare, json_values_equal
from prepare_amplitude_import import convert, entries, session_key, session_uuid, time_us
from prepare_cloud_import import envelope
from recreate_analytics_cohorts import Client as HertzClient

UTC = dt.timezone.utc


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def sql_quote(value):
    return "'" + str(value).replace('\\','\\\\').replace("'","\\'") + "'"


def source_rows(project, columns, predicate='1=1'):
    if not isinstance(project,int) or project <= 0:
        raise ValueError('Source project must be positive')
    sql=(f'SELECT {columns} FROM posthog.sharded_events FINAL WHERE team_id={project} '
         f'AND ({predicate}) FORMAT JSONEachRow')
    process=subprocess.Popen(['docker','exec','kasanova_posthog-clickhouse-1','clickhouse-client',
         '--max_memory_usage','1073741824','--query',sql],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    try:
        for line in process.stdout:
            yield json.loads(line)
        if process.wait()!=0:raise RuntimeError('Native source query failed')
    finally:
        if process.poll() is None:process.terminate();process.wait()
        process.stdout.close()


def source_query(project, columns, predicate='1=1'):
    return list(source_rows(project, columns, predicate))




def source_people(client, project):
    path=f'/api/projects/{project}/persons/?limit=100';people=[];visited=set()
    while path:
        if path in visited:raise RuntimeError('Person pagination repeated')
        visited.add(path);result=client.request(path);people.extend(result['results']);next_path=result.get('next')
        if next_path and not next_path.startswith(client.base+'/'):raise RuntimeError('Person pagination origin changed')
        path=next_path[len(client.base):] if next_path else None
    return people


def source_profile_snapshot(config, project, evidence):
    client=HertzClient(evidence);people=source_people(client,project)
    atomic_json(evidence/'hertz-persons-before-private.json',people)
    return people


def source_profile_catchup(config, project, before, exclude_uuids, started, evidence):
    # Refill accepts only empty historical person snapshots (see preflight).
    # Never manufacture profile events or overwrite native SDK property updates.
    client=HertzClient(evidence);after=source_people(client,project)
    by_id={i:p for p in after for i in p['distinct_ids']}
    for person in before:
        now=by_id.get(person['distinct_ids'][0])
        if not now or set(now['distinct_ids'])!=set(person['distinct_ids']):
            raise RuntimeError('Existing Hertz identity changed during refill; native reconciliation required')
    atomic_json(evidence/'hertz-profile-reconciliation.json',{
        'existing_identity_memberships_verified':len(before),
        'historical_person_property_operations_sent':0,
        'synthetic_profile_events_sent':0,'new_aliases_generated':0,
        'concurrent_native_profile_changes_preserved':True})


def source_ids(project, journal_path=None):
    # Exact original insert IDs bridge Amplitude UUIDs and owned transport UUIDv5.
    rows=source_rows(project,"uuid,properties")
    uuids=set();inserts=set();sessions={}
    for row in rows:
        uuids.add(row['uuid']);props=json.loads(row['properties'])
        original=props.get('__amplitude_original_record')
        transport=props.get('__kasanova_transport_record')
        raw=json.loads(original or transport or '{}')
        insert=raw.get('$insert_id') if original else raw.get('insert_id')
        if insert:inserts.add(str(insert))
        if original:
            key=session_key(raw)
            if key and props.get('$session_id'):
                known=sessions.setdefault(key,props['$session_id'])
                if known!=props['$session_id']:raise RuntimeError('Existing session mapping conflict')
    if journal_path:
        # Committed transport acknowledgements count even while Hertz is down.
        with closing(sqlite3.connect(Path(journal_path).resolve().as_uri()+'?mode=ro',uri=True)) as journal:
            for record_id,raw in journal.execute('SELECT id,raw FROM records WHERE project=?',(str(project),)):
                uuids.add(record_id);insert=json.loads(raw).get('insert_id') or json.loads(raw).get('$insert_id')
                if insert:inserts.add(str(insert))
            inserts.update(row[0] for row in journal.execute('SELECT source_id FROM history WHERE project=?',(str(project),)))
    return uuids,inserts,sessions


def extend_identity(identity, archive, raw_events, evidence):
    # Preserve all old canonical bindings; only add source-verified new nodes.
    result=json.loads(canonical(identity));new={str(json.loads(raw)['amplitude_id']) for raw in raw_events}-set(result['canonical_map'])
    for identifier in sorted(new):
        fingerprint=hashlib.sha256(identifier.encode()).hexdigest()
        receipt=archive.fetch('/api/2/useractivity',{'user':identifier,'limit':1,'direction':'latest'},archive.base/'new-profiles'/f'{fingerprint}.json')
        if receipt['status']!=200:raise RuntimeError('New source profile lookup failed')
        data=json.loads((archive.base/receipt['path']).read_text())['userData'];root=str(data['canonical_amplitude_id'])
        root=result['canonical_map'].get(root,root);nodes={identifier,str(data['canonical_amplitude_id']),*[str(x) for x in data.get('merged_amplitude_ids',[])]}
        user=data.get('user_id')
        if any(data.get(k) for k in ['aliased_amplitude_id','aliasing_profiles','aliasing_user_ids']):
            raise RuntimeError('New source identity needs complete alias graph reconciliation')
        for node in nodes:
            known=result['canonical_map'].get(node)
            if known is not None and known!=root:raise RuntimeError('Existing canonical identity would change')
            result['canonical_map'][node]=root
        if user:
            known=result['user_ids'].get(root)
            if known is not None and known!=user:raise RuntimeError('Source user identity conflict')
            if root in identity['canonical_map'].values() and not identity['user_ids'].get(root):
                raise RuntimeError('Previously anonymous native identity requires original source identification evidence')
            result['user_ids'][root]=user
        atomic_json(evidence/f'identity-profile-{fingerprint}.json',receipt)
    for key,value in identity['canonical_map'].items():
        if result['canonical_map'][key]!=value:raise RuntimeError('Prior identity map changed')
    atomic_json(evidence/'resolved-identity-map-private.json',result)
    return result


def export_window(config, state, output):
    amplitude=config['amplitude'];previous=dt.datetime.fromisoformat(state['amplitude_upload_cutoff'])
    end=(dt.datetime.now(UTC)-dt.timedelta(hours=3)).replace(minute=0,second=0,microsecond=0)
    start=max(dt.datetime.fromisoformat(config['amplitude_initial_floor']),previous-dt.timedelta(hours=48))
    if end<=start:raise RuntimeError('Export cutoff has not advanced beyond source floor')
    os.environ['KASANOVA_REFILL_SOURCE_KEY']=amplitude['api_key'];os.environ['KASANOVA_REFILL_SOURCE_SECRET']=amplitude['secret_key']
    args=SimpleNamespace(output=output,environment='prod',project_key_var='KASANOVA_REFILL_SOURCE_KEY',
        secret_var='KASANOVA_REFILL_SOURCE_SECRET',host=amplitude['host'],start=start.strftime('%Y%m%dT%H'),
        cutoff=end.strftime('%Y%m%dT%H'),timeout_seconds=180)
    archive=Archive(args);windows=[];cursor=start
    ranges=[]
    while cursor<end:
        next_hour=min(cursor+HOUR,end);ranges.append((cursor,next_hour));cursor=next_hour
    with ThreadPoolExecutor(max_workers=3) as pool:
        for group in pool.map(lambda value:archive.window(*value),ranges):windows.extend(group)
    windows.sort(key=lambda value:value['start']);cursor=args.start
    for window in windows:
        if window['status'] not in (200,404) or window['start']!=cursor:raise RuntimeError('Amplitude export coverage gap/failure')
        if window['status']==200 and window['project_ids']!=[str(amplitude['project_id'])]:raise RuntimeError('Amplitude source project differs')
        cursor=window['end_exclusive']
    if cursor!=args.cutoff:raise RuntimeError('Incomplete export cutoff')
    summary={'coverage_complete':True,'project_key_sha256':archive.fingerprint,'start':args.start,
        'cutoff_exclusive':args.cutoff,'windows':windows,'records':sum(w.get('records',0) for w in windows)}
    atomic_json(archive.base/'events-summary.json',summary)
    return archive,end



def verify_hertz(project, expected):
    by_id={e['uuid']:e for e in expected};ids=','.join(sql_quote(x) for x in by_id)
    for attempt in range(30):
        rows=source_query(project,'uuid,event,distinct_id,toUnixTimestamp64Micro(timestamp) AS timestamp_us,properties',f'uuid IN ({ids})')
        if len(rows)==len(expected):
            for row in rows:
                wanted=by_id[row['uuid']];props=json.loads(row['properties'])
                if row['event']!=wanted['event'] or row['distinct_id']!=wanted['distinct_id'] or int(row['timestamp_us'])!=time_us(wanted['timestamp']):
                    raise RuntimeError('Native source event header differs')
                if any(key not in props or not json_values_equal(props[key],value) for key,value in wanted['properties'].items()):
                    raise RuntimeError('Native source typed property differs')
            return rows
        time.sleep(2)
    raise RuntimeError('Hertz missing source UUIDs after capture')


def refill_amplitude(config, state, output, exported=None):
    archive,end=exported or export_window(config,state,output)
    unique={}
    for raw in entries(output):
        item=json.loads(raw);uid=item['uuid']
        if uid in unique and json.loads(unique[uid])!=item:raise RuntimeError('Conflicting duplicate source UUID')
        unique[uid]=raw
    identity=json.loads(Path(config['identity_map']).read_text());identity=extend_identity(identity,archive,list(unique.values()),output)
    project=config['hertz_projects']['prod']['id'];existing,inserts,old_sessions=source_ids(project,config['journal'])
    sessions={}
    for raw in unique.values():
        event=json.loads(raw);key=session_key(event)
        if key:sessions[key]=min(sessions.get(key,time_us(event['event_time'])),time_us(event['event_time']))
    prepared=[]
    for raw in unique.values():
        event=json.loads(raw)
        if event['uuid'] in existing or str(event.get('$insert_id','')) in inserts:continue
        native=convert(raw,identity,sessions);key=session_key(event)
        if key in old_sessions:native['properties']['$session_id']=old_sessions[key]
        prepared.append(native)
    prepared.sort(key=lambda e:(time_us(e['timestamp']),e['uuid']))
    with gzip.open(output/'amplitude-missing-native.ndjson.gz','wt') as stream:
        for event in prepared:stream.write(canonical(event)+'\n')
    atomic_json(output/'amplitude-preparation.json',{'source_records':len(unique),'missing':len(prepared),
        'source_uuid_and_insert_id_dedup':True,'originals_retained':True,'source_project':config['amplitude']['project_id']})
    started=dt.datetime.now(UTC)
    # Amplitude user_properties are archived intact. An old nonempty $set can
    # overwrite current native profiles, which have no public timestamp/CAS
    # guard. Stop before any capture rather than manufacture repair activity.
    if any(e['properties'].get('$set') for e in prepared):
        raise RuntimeError('Historical nonempty person snapshots require a supported profile-preserving import')
    before=source_profile_snapshot(config,project,output) if prepared else []
    routes=json.loads(Path(config['ingress_config']).read_text())['routes'];journal=Journal(config['journal'],routes)
    # Commit native payloads and retry identities together. The existing worker
    # owns retries to both destinations, even if this process exits immediately.
    queued=journal.enqueue_history(prepared,project)
    # A failed prior run or an SDK commit before preflight can already own an
    # export event without native persistence. Verify those durable payloads as
    # well; insert-ID dedup alone is not a successful-source-leg certificate.
    source_inserts={str(json.loads(raw).get('$insert_id')) for raw in unique.values() if json.loads(raw).get('$insert_id')}
    owned={e['uuid']:e for e in queued}
    with closing(sqlite3.connect(Path(config['journal']).resolve().as_uri()+'?mode=ro',uri=True)) as con:
        for native,raw in con.execute('SELECT native,raw FROM records WHERE project=?',(str(project),)):
            original=json.loads(raw);insert=original.get('insert_id') or original.get('$insert_id')
            if insert and str(insert) in source_inserts:
                event=json.loads(native);owned[event['uuid']]=event
    queued=list(owned.values())
    atomic_json(output/'journal-ownership.json',{'durable_native_uuids':[e['uuid'] for e in queued],
        'original_retry_bindings_atomic':True,'source_uuids':[e['uuid'] for e in prepared]})
    for index in range(0,len(queued),100):
        batch=queued[index:index+100]
        verify_hertz(project,batch)
        atomic_json(output/f'hertz-batch-{index:06d}.json',{'persisted_fields_verified':True,'uuids':[e['uuid'] for e in batch]})
    if prepared:source_profile_catchup(config,project,before,{e['uuid'] for e in prepared},started,output)
    return end,len(prepared),identity


def cloud_persons(client):
    people=[];visited=set();path=f"/api/projects/{client.project['id']}/persons/?limit=100"
    while path:
        if path in visited:raise RuntimeError('Cloud person pagination repeated')
        visited.add(path);response=client.request(path);people.extend(response['results']);next_path=response.get('next')
        if next_path and not next_path.startswith(client.config['api_host']+'/'):raise RuntimeError('Cloud pagination origin changed')
        path=next_path[len(client.config['api_host']):] if next_path else None
    return people


def cloud_fields_match(event, record):
    name,identity,stamp,props=record
    props=json.loads(props) if isinstance(props,str) else props
    if name!=event['event'] or identity!=event['distinct_id'] or time_us(stamp)!=time_us(event['timestamp']):return False
    for key,value in event['properties'].items():
        if key=='$transformations_succeeded':
            archived=json.loads(props.get('__kasanova_posthog_original_event','{}'))
            if not json_values_equal(archived.get('properties',{}).get(key),value):return False
        elif key in ('$set','$set_once') and isinstance(value,dict):
            # Cloud adds its own person-property names (for example geo-IP).
            # Preserve every source operation's complete typed value while
            # allowing those additional backend properties on the same event.
            got=props.get(key)
            if not isinstance(got,dict) or any(k not in got or not json_values_equal(got[k],v) for k,v in value.items()):return False
        elif key not in props or not json_values_equal(props[key],value):return False
    return True


def source_wire_envelope(row, config, source):
    event=envelope(row);raw=event['properties'].get('__kasanova_transport_record')
    if raw:
        routes=json.loads(Path(config['ingress_config']).read_text())['routes']
        route=next(r for r in routes.values() if r['project_id']==source)
        wire=Journal.convert(json.loads(raw),raw,event['uuid'],event['distinct_id'],route)
        # Native storage owns its persisted timestamp. This excludes only the
        # source backend's added metadata, not any SDK or converted field.
        event['properties']=wire['properties']
    return event


def cloud_readback(client, selected):
    """Read full fields with a bounded scan and stable global latest versions."""
    if len(selected)>20:
        return {uid:record for index in range(0,len(selected),20)
                for uid,record in cloud_readback(client,selected[index:index+20]).items()}
    ids=','.join(sql_quote(row['uuid']) for row in selected)
    header_sql=f'SELECT uuid,argMax(tuple(event,distinct_id,timestamp,toString(created_at)),created_at) FROM events WHERE uuid IN ({ids}) GROUP BY uuid LIMIT 100'
    for attempt in range(3):
        headers={row[0]:row[1] for row in client.query(header_sql)}
        stamps=[int(row['timestamp_us']) for row in selected]+[time_us(row[2]) for row in headers.values()]
        lower=dt.datetime.fromtimestamp(min(stamps)//1000000-1,UTC).strftime('%Y-%m-%d %H:%M:%S')
        upper=dt.datetime.fromtimestamp(max(stamps)//1000000+2,UTC).strftime('%Y-%m-%d %H:%M:%S')
        bounds=f"timestamp >= toDateTime({sql_quote(lower)},'UTC') AND timestamp <= toDateTime({sql_quote(upper)},'UTC')"
        fields_sql=f'SELECT uuid,argMax(tuple(event,distinct_id,timestamp,properties,toString(created_at)),created_at) FROM events WHERE {bounds} AND uuid IN ({ids}) GROUP BY uuid LIMIT 100'
        fields={row[0]:row[1] for row in client.query(fields_sql)}
        after={row[0]:row[1] for row in client.query(header_sql)}
        if headers==after and headers.keys()==fields.keys() and all(
                fields[uid][:3]==header[:3] and fields[uid][4]==header[3]
                for uid,header in headers.items()):
            return {uid:record[:4] for uid,record in fields.items()}
        if attempt<2:time.sleep(1)
    raise RuntimeError('Cloud latest UUID versions changed or full-field read was incomplete')


def cloud_gap(config, state, environment, evidence, end):
    c=CloudClient(config['cloud_credentials'],environment,evidence);source=config['hertz_projects'][environment]['id']
    floor=max(dt.datetime.fromisoformat(config['native_delta_floor']),dt.datetime.fromisoformat(state['native_cutoff'])-dt.timedelta(hours=48))
    predicate=f"_timestamp >= toDateTime({sql_quote(floor.strftime('%Y-%m-%d %H:%M:%S'))},'UTC') AND _timestamp < toDateTime({sql_quote(end.strftime('%Y-%m-%d %H:%M:%S'))},'UTC')"
    rows=source_query(source,'uuid,event,distinct_id,toUnixTimestamp64Micro(timestamp) AS timestamp_us,properties',predicate)
    evidence.mkdir(parents=True,exist_ok=True,mode=0o700)
    with gzip.open(evidence/'native-source-delta-private.ndjson.gz','wt') as stream:
        for row in rows:stream.write(canonical(row)+'\n')
    missing=0;repaired=0;checked=0
    for index in range(0,len(rows),100):
        selected=rows[index:index+100]
        actual=cloud_readback(c,selected)
        expected=[];events=[];absent=0;stale=0
        for row in selected:
            wire=source_wire_envelope(row,config,source)
            # Normal forwarding intentionally has Cloud's backend enrichment.
            # Do not replay an already faithful SDK event merely to add a blob.
            if row['uuid'] in actual and cloud_fields_match(wire,actual[row['uuid']]):
                expected.append(wire);continue
            event=envelope(row);original=canonical(event);props=event['properties']
            if props.get('$set') or props.get('$set_once') or props.get('$unset'):
                raise RuntimeError('Missing/stale native person updates require profile-preserving reconciliation')
            if '__kasanova_posthog_original_event' in props:raise RuntimeError('Original envelope namespace collision')
            props['__kasanova_posthog_original_event']=original;props['__kasanova_posthog_original_event_sha256']=hashlib.sha256(original.encode()).hexdigest()
            expected.append(event)
            if row['uuid'] not in actual:events.append(event);absent+=1
            elif not cloud_fields_match(event,actual[row['uuid']]):events.append(event);stale+=1
        older=[e for e in events if time_us(e['timestamp'])<=time_us(dt.datetime.now(UTC).isoformat())-48*3600*1000000]
        newer=[e for e in events if e not in older]
        if older:c.capture(older,historical=True)
        if newer:c.capture(newer)
        if events:
            for attempt in range(30):
                actual=cloud_readback(c,selected)
                if all(e['uuid'] in actual and cloud_fields_match(e,actual[e['uuid']]) for e in expected):break
                time.sleep(2)
            else:raise RuntimeError('Cloud native UUID or exact fields missing/stale after capture')
        checked+=len(selected);missing+=absent;repaired+=stale
        atomic_json(evidence/f'reconciliation-{index:06d}.json',{'source_uuids':[x['uuid'] for x in selected],'missing_captured':absent,'existing_versions_repaired':stale,'persisted_source_wire_fields_verified':True,'source_backend_metadata_retained_in_delta_archive':True,'latest_logical_uuid_readback':True})
    if missing or repaired:
        # Native source updates above restore identity relationships; compare
        # only affected people in two paginated passes, never synthetic $set.
        affected={row['distinct_id'] for row in rows}
        source_client=HertzClient(evidence)
        people=[p for p in source_people(source_client,source) if affected.intersection(p['distinct_ids'])]
        cloud_people=cloud_persons(c)
        by_id={i:p for p in cloud_people for i in p['distinct_ids']}
        mismatches=[]
        for person in people:
            got=by_id.get(person['distinct_ids'][0])
            if not got or set(got['distinct_ids'])!=set(person['distinct_ids']):
                mismatches.append(person['id']);continue
            # Cloud creates its own internal creator-event metadata. Retain the
            # original in the sealed native person archive, never add activity
            # merely to make this backend-generated metadata match.
            if any(k!='$creator_event_uuid' and (k not in got['properties'] or not json_values_equal(got['properties'][k],v)) for k,v in person['properties'].items()):
                mismatches.append(person['id'])
        atomic_json(evidence/'affected-source-persons-private.json',people)
        if mismatches:
            atomic_json(evidence/'profile-differences-private.json',{'source_person_ids':mismatches})
            raise RuntimeError('Cloud affected native profiles differ; no synthetic profile repair performed')
        atomic_json(evidence/'profile-reconciliation.json',{'affected_source_persons_verified':len(people),
            'source_custom_properties_and_memberships_verified':True,'synthetic_profile_events_sent':0,
            'internal_creator_metadata_retained_in_source_archive':True})
    return {'source_events_checked':checked,'missing_events_captured_and_verified':missing,'existing_versions_repaired_and_verified':repaired}


def run(config_path):
    os.umask(0o077);config=json.loads(config_path.read_text());root=Path(config['runtime']);state_path=Path(config['state']);state=json.loads(state_path.read_text())
    if config['amplitude']['project_id']!=state['amplitude_project_id']:raise RuntimeError('Refill source project changed')
    if len({v['id'] for v in config['hertz_projects'].values()})!=2:raise RuntimeError('Hertz environments overlap')
    if config['hertz_ingestion_host']!='http://127.0.0.1:18084':raise RuntimeError('Unexpected private Hertz ingestion origin')
    evidence=root/'operations/refill'/dt.datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ');evidence.mkdir(parents=True,mode=0o700)
    with (root/'operations/.refill.lock').open('a') as own_lock:
        fcntl.flock(own_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        exported=export_window(config,state,evidence)
        # Avoid mutating the native database/journal while backup owns runtime.
        with (root/'operations/.backup.lock').open('a') as backup_lock:
            fcntl.flock(backup_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            end,added,identity=refill_amplitude(config,state,evidence,exported)
            native_end=dt.datetime.now(UTC).replace(microsecond=0)
            cloud={env:cloud_gap(config,state,env,evidence/env,native_end) for env in ['prod','dev']}
            result={'state':'verified','amplitude_missing_verified_in_hertz':added,'cloud':cloud,'completed_at':dt.datetime.now(UTC).isoformat(),'native_cutoff':native_end.isoformat(),'amplitude_upload_cutoff':end.isoformat()}
            atomic_json(evidence/'success.json',result)
            # Advance shared state only after both legs persisted and verified.
            atomic_json(Path(config['identity_map']),identity)
            state.update(native_cutoff=native_end.isoformat(),amplitude_upload_cutoff=end.isoformat(),last_success=str(evidence))
            atomic_json(state_path,state)
            print(json.dumps(result),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True);args=parser.parse_args()
    try:run(args.config)
    except Exception as error:
        print(json.dumps({'state':'failed','error_type':type(error).__name__,'watermarks_not_advanced':True}),flush=True)
        raise SystemExit(1)
