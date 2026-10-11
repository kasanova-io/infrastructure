#!/usr/bin/env python3
"""Capture one prepared sample in an isolated project and compare persisted fields."""
import argparse
import datetime as dt
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request

from prepare_amplitude_import import RAW, SHA, USER, time_us

CREDENTIALS = Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json')

def main(sample, evidence, existing_dev=False):
    os.umask(0o077)
    credentials=json.loads(CREDENTIALS.read_text());base=credentials['url']
    evidence.mkdir(mode=0o700,parents=True,exist_ok=True)
    marker=evidence/'validation-project-private.json'
    jar=http.cookiejar.CookieJar();client=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    def req(path,payload=None):
        headers={'Content-Type':'application/json','Referer':base+'/login','User-Agent':'Kasanova-Amplitude-import-validation'}
        token=next((c.value for c in jar if c.name=='posthog_csrftoken'),None)
        if token:headers['X-CSRFToken']=token
        request=urllib.request.Request(base+path,data=None if payload is None else json.dumps(payload).encode(),headers=headers)
        try:
            with client.open(request,timeout=45) as r:return json.loads(r.read())
        except urllib.error.HTTPError as error:
            body=json.loads(error.read())
            (evidence/'last-http-error.json').write_text(json.dumps({'status':error.code,'detail':body.get('detail'),'code':body.get('code')})+'\n')
            raise
    req('/api/login/',{'email':credentials['email'],'password':credentials['password']})
    state=json.loads(marker.read_text()) if marker.exists() else None
    if state is None and existing_dev:
        state=dict(credentials['projects']['dev'],name='DEV import validation fixtures')
        marker.write_text(json.dumps(state)+'\n');marker.chmod(0o600)
    if state is None:
        name='Amplitude migration validation '+evidence.name
        project=req('/api/organizations/'+credentials['organization_id']+'/projects/',{'name':name,'autocapture_opt_out':True})
        if project['id'] in [v['id'] for v in credentials['projects'].values()]:raise RuntimeError('Validation project is not isolated')
        state={'id':project['id'],'name':name,'api_key':project['api_token']}
        marker.write_text(json.dumps(state)+'\n');marker.chmod(0o600)
    events=[json.loads(line) for line in sample.read_bytes().splitlines()]
    if existing_dev and any(not e['distinct_id'].startswith('amplitude-import-validation:') or e['properties'].get('__amplitude_validation_fixture') is not True for e in events):
        raise RuntimeError('Existing DEV accepts only explicit validation fixtures with separate test identities')
    sample_sha=hashlib.sha256(sample.read_bytes()).hexdigest()
    if state.get('sample_sha256') not in (None,sample_sha):raise RuntimeError('Cannot resume using another sample')
    state['sample_sha256']=sample_sha
    if not state.get('capture_acknowledged'):
        result=req('/batch/',{'api_key':state['api_key'],'historical_migration':True,'batch':events})
        state['capture_acknowledged']=True
        state['capture_response']=result
        marker.write_text(json.dumps(state)+'\n')
    ids=', '.join("'"+e['uuid']+"'" for e in events)
    sql='SELECT uuid, event, distinct_id, timestamp, properties FROM events WHERE uuid IN ('+ids+') LIMIT '+str(len(events)*2)
    def query(project):
        result=req('/api/projects/'+str(project)+'/query/',{'query':{'kind':'HogQLQuery','query':sql},'refresh':'force_blocking'})
        return result['results']
    for attempt in range(90):
        try:
            rows=query(state['id'])
            if len(rows)>=len(events):break
        except urllib.error.HTTPError as e:
            if e.code not in (500,502,503):raise
        time.sleep(2)
    else:raise RuntimeError('Sample was acknowledged but not fully queryable')
    expected={e['uuid']:e for e in events}
    if len(rows)!=len(expected):raise RuntimeError('Sample row count differs or duplicate events exist')
    seen=set()
    for id,event,distinct,timestamp,properties in rows:
        if id in seen:raise RuntimeError('Duplicate native UUID')
        seen.add(id);e=expected[id]
        if event!=e['event'] or distinct!=e['distinct_id'] or time_us(timestamp)!=time_us(e['timestamp']):raise RuntimeError('Native event identity, name or timestamp changed')
        p=json.loads(properties) if isinstance(properties,str) else properties
        if p[RAW]!=e['properties'][RAW] or p[SHA]!=e['properties'][SHA]:raise RuntimeError('Original record bytes or checksum changed')
        source=json.loads(p[RAW])
        for key,value in source.get('event_properties',{}).items():
            if key not in p or p[key]!=value:raise RuntimeError('Original event property changed')
        if p[USER]!=source.get('user_properties',{}):raise RuntimeError('Event-time source user properties changed')
        if p.get('$session_id')!=e['properties'].get('$session_id'):raise RuntimeError('Session mapping changed')
    if seen!=set(expected):raise RuntimeError('Native sample UUIDs differ')
    for environment in credentials['projects'].values():
        if environment['id']==state['id']:continue
        if query(environment['id']):raise RuntimeError('Migration sample leaked into existing DEV or PROD')
    report={'verified_at':dt.datetime.now(dt.timezone.utc).isoformat(),'validation_project_id':state['id'],'sample_sha256':sample_sha,'persisted_rows':len(rows),'event_types':len({e['event'] for e in events}),'fixture_uuid_event_name_timestamp_and_distinct_id_verified':True,'original_record_bytes_and_typed_event_user_properties_verified':True,'session_ids_verified':True,'fixture_ids_absent_from_other_live_project_verified':True,'explicit_dev_fixtures':existing_dev,'prod_history_imported':0,'connector_changed':False,'current_profile_import_and_reporting_replay_parity_verified':False}
    (evidence/'validation.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('sample',type=Path);p.add_argument('evidence',type=Path);p.add_argument('--existing-dev-fixtures',action='store_true');a=p.parse_args();main(a.sample,a.evidence,a.existing_dev_fixtures)
