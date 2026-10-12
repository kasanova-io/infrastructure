#!/usr/bin/env python3
"""Checksum-bound native history capture, with durable acknowledgement receipts.

Capture acknowledgement is not a persisted-field or person-profile verdict.
Complete reconciliation remains an explicit, separate required step.
"""
import argparse
import datetime as dt
import fcntl
import gzip
import hashlib
import json
import os
import concurrent.futures
from collections import deque
from pathlib import Path

from cloud_client import CloudClient
from export_cloud_source import file_sha
from import_amplitude_events import atomic_json, canonical
from prepare_amplitude_import import time_us


def batches(source, now, maximum=100, byte_limit=5*1024*1024):
    boundary = time_us(now.isoformat()) - 48*3600*1000000
    pending, size, mode = [], 0, None
    with gzip.open(source, 'rt') as stream:
        for line in stream:
            event = json.loads(line)
            historical = time_us(event['timestamp']) <= boundary
            length = len(canonical(event).encode())
            if length > byte_limit:
                raise RuntimeError('Single source event exceeds conservative batch limit')
            if pending and (len(pending)==maximum or size+length>byte_limit or historical!=mode):
                yield pending, mode
                pending, size = [], 0
            mode = historical
            pending.append(event)
            size += length
    if pending:
        yield pending, mode


def import_events(directory, environment, credentials, evidence, profile_plan, send=False, workers=1):
    os.umask(0o077)
    if workers not in range(1,5):
        raise ValueError('Capture concurrency must be1to4')
    manifest_path = directory / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    if manifest['state'] != 'prepared_not_imported':
        raise RuntimeError('Prepared source is not sealed')
    expected = manifest['projects'][environment]
    source = directory / (environment+'-events.ndjson.gz')
    if file_sha(source) != expected['sha256']:
        raise RuntimeError('Prepared event bytes changed')
    client = CloudClient(credentials, environment, evidence)
    plan = json.loads(profile_plan.read_text())
    if (plan.get('environment') != environment or plan.get('cloud_project_id') != client.project['id']
            or plan.get('source_manifest_sha256') != file_sha(manifest_path)
            or plan.get('restore_source_and_newer_live_person_properties') is not True):
        raise RuntimeError('Bound person-profile reconciliation plan required')
    state_path = evidence / 'capture-state.json'
    state = json.loads(state_path.read_text()) if state_path.exists() else {
        'source_sha256':expected['sha256'], 'cloud_project_id':client.project['id'],
        'source_manifest_sha256':file_sha(manifest_path), 'profile_plan_sha256':file_sha(profile_plan),
        'batch_boundary_at':dt.datetime.now(dt.timezone.utc).isoformat(), 'acknowledged_records':0,
        'acknowledged_batches':0, 'state':'prepared', 'persisted_fields_verified':False,
        'person_profiles_verified':False}
    for key,value in [('source_sha256',expected['sha256']),('cloud_project_id',client.project['id']),
                      ('source_manifest_sha256',file_sha(manifest_path)),('profile_plan_sha256',file_sha(profile_plan))]:
        if state[key] != value:
            raise RuntimeError('Resume source, destination or profile plan changed')
    if not send:
        return {**state, 'source_records':expected['rows'], 'network_writes':0}
    lock = evidence / '.capture.lock'
    with lock.open('a') as stream:
        fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        atomic_json(state_path,state)
        encountered = 0
        boundary = dt.datetime.fromisoformat(state['batch_boundary_at'])
        def acknowledge(index, events, historical):
            receipt_path = evidence / f'batch-{index:06d}.json'
            event_sha = hashlib.sha256(canonical(events).encode()).hexdigest()
            binding = {'source_sha256':state['source_sha256'], 'cloud_project_id':client.project['id'],
                       'batch_sha256':event_sha, 'records':len(events), 'historical_migration':historical}
            if receipt_path.exists():
                receipt = json.loads(receipt_path.read_text())
                if any(receipt.get(k)!=v for k,v in binding.items()) or not receipt.get('capture_acknowledged'):
                    raise RuntimeError('Existing capture receipt changed')
            else:
                # Independent openers avoid sharing mutable HTTP handler state.
                sender = CloudClient(credentials, environment, evidence)
                if sender.project != client.project or sender.config != client.config:
                    raise RuntimeError('Destination changed during capture')
                sender.capture(events,historical)
                receipt = {**binding,'capture_acknowledged':True,
                           'acknowledged_at':dt.datetime.now(dt.timezone.utc).isoformat()}
                atomic_json(receipt_path,receipt)
            return index, len(events)

        def checkpoint(future):
            nonlocal encountered
            index, count = future.result()
            encountered += count
            # Advance only the contiguous, receipt-backed source prefix. Later
            # completed receipts survive a failure and are reused on resume.
            state.update(acknowledged_batches=index+1,acknowledged_records=encountered,state='capturing')
            atomic_json(state_path,state)
            if (index+1)%10==0:
                print(json.dumps({'environment':environment,'capture_acknowledged_records':encountered}),flush=True)

        pending = deque()
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        try:
            for index,(events,historical) in enumerate(batches(source,boundary)):
                pending.append(pool.submit(acknowledge,index,events,historical))
                if len(pending) >= workers * 2:
                    checkpoint(pending.popleft())
            while pending:
                checkpoint(pending.popleft())
        finally:
            # Bounded in-flight work may finish and save receipts; no successful
            # reconciliation is reported after an error or missing checkpoint.
            pool.shutdown(wait=True,cancel_futures=True)
        if encountered != expected['rows']:
            raise RuntimeError('Prepared record count changed')
        state.update(state='capture_acknowledged_reconciliation_required',
                     capture_completed_at=dt.datetime.now(dt.timezone.utc).isoformat())
        atomic_json(state_path,state)
        return state


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--prepared',type=Path,required=True)
    p.add_argument('--environment',choices=['prod','dev'],required=True)
    p.add_argument('--credentials',type=Path,required=True)
    p.add_argument('--evidence',type=Path,required=True)
    p.add_argument('--profile-plan',type=Path,required=True)
    p.add_argument('--send',action='store_true')
    p.add_argument('--workers',type=int,default=1)
    a=p.parse_args()
    result=import_events(a.prepared,a.environment,a.credentials,a.evidence,a.profile_plan,a.send,a.workers)
    print(json.dumps({'state':result['state'],'acknowledged_records':result['acknowledged_records'],
                      'persisted_fields_verified':False,'person_profiles_verified':False}))
