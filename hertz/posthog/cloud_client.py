"""Bounded PostHog Cloud API/capture transport; private errors stay in evidence."""
import json
import http.client
import hashlib
from pathlib import Path
import re
import time
import urllib.error
import urllib.request
import uuid

from import_amplitude_events import atomic_json, canonical
from analytics_ingress import precise_json, cloud_event


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('Cloud redirect refused')


class CloudClient:
    def __init__(self, credentials, environment, evidence):
        self.config = json.loads(Path(credentials).read_text())
        if environment not in ('prod', 'dev'):
            raise ValueError('Explicit source environment required')
        region = self.config['region']
        if region not in ('us', 'eu'):
            raise ValueError('Unknown Cloud region')
        if (self.config['api_host'], self.config['ingestion_host']) != (
                f'https://{region}.posthog.com', f'https://{region}.i.posthog.com'):
            raise ValueError('Unexpected Cloud origin')
        self.project = self.config['projects'][environment]
        if self.config['projects']['prod']['id'] == self.config['projects']['dev']['id']:
            raise ValueError('Cloud environments must remain separate')
        self.evidence = Path(evidence)
        self.evidence.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, path, payload=None, method=None, retry=True, timeout=180, deadline=None):
        if not path.startswith('/') or path.startswith('//'):
            raise ValueError('Only same-origin API paths allowed')
        return self._request(self.config['api_host'] + path, payload, method,
                             {'Authorization':'Bearer ' + self.config['personal_api_key']}, retries=5 if retry else 1,
                             timeout=timeout, deadline=deadline)

    def _request(self, url, payload, method, headers, retries=5, timeout=180, deadline=None):
        for attempt in range(retries):
            remaining=None if deadline is None else deadline-time.monotonic()
            if remaining is not None and remaining<=0:raise TimeoutError('Cloud query deadline expired')
            request = urllib.request.Request(url, data=None if payload is None else precise_json(payload).encode(),
                method=method, headers={**headers, 'Content-Type':'application/json'})
            try:
                with self.opener.open(request, timeout=timeout if remaining is None else min(timeout,remaining)) as response:
                    # A redirect must never send personal credentials to another origin.
                    if response.url != url:
                        raise RuntimeError('Unexpected Cloud response URL')
                    if response.status == 204:
                        return {}
                    raw=self._read(response,deadline)
                    if not raw and method in ('PATCH','PUT','DELETE'):
                        return {}
                    try:
                        return json.loads(raw)
                    except json.JSONDecodeError:
                        atomic_json(self.evidence / f'cloud-invalid-response-{time.time_ns()}.json',
                                    {'status':response.status, 'method':method or ('POST' if payload else 'GET'),
                                     'content_type':response.headers.get('Content-Type'),
                                     'bytes':len(raw), 'body':raw.decode(errors='replace')})
                        raise RuntimeError('Cloud returned invalid JSON; private response retained') from None
            except urllib.error.HTTPError as error:
                body = self._read(error,deadline).decode(errors='replace')
                atomic_json(self.evidence / f'cloud-http-error-{time.time_ns()}.json',
                            {'status':error.code, 'body':body, 'attempt':attempt})
                if error.code not in (429,500,502,503,504) or attempt == retries - 1:
                    raise RuntimeError(f'Cloud HTTP{error.code}; private error retained') from error
                retry = error.headers.get('Retry-After','')
                delay = min(60, int(retry)) if retry.isdigit() else min(30,2**attempt)
                self._delay(delay,deadline)
            except (TimeoutError, urllib.error.URLError, http.client.HTTPException, ConnectionError) as error:
                if deadline is not None and time.monotonic()>=deadline:
                    raise TimeoutError('Cloud query deadline expired') from error
                if attempt == retries - 1:
                    if deadline is not None:raise RuntimeError('Cloud query transport failed; bound job receipt retained') from error
                    raise RuntimeError('Cloud transport failed; deterministic UUID retry remains safe')
                self._delay(2**attempt,deadline)

    @staticmethod
    def _read(response,deadline):
        if deadline is None:return response.read()
        chunks=[]
        while True:
            if time.monotonic()>=deadline:raise TimeoutError('Cloud query deadline expired')
            part=response.read1(65536)
            if not part:return b''.join(chunks)
            chunks.append(part)

    @staticmethod
    def _delay(seconds,deadline):
        if deadline is None:time.sleep(seconds);return
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('Cloud query deadline expired')
        time.sleep(min(seconds,remaining))

    def query_node(self, query, deadline_seconds=900):
        """Submit once and poll only that project-bound read-only asynchronous job."""
        if not isinstance(query,dict) or not isinstance(query.get('kind'),str):
            raise ValueError('A native query node is required')
        project=self.project['id']
        if type(project) is not int or project<=0:raise ValueError('Cloud query project must be positive')
        if type(deadline_seconds) not in (int,float) or not 60<=deadline_seconds<=900:
            raise ValueError('Cloud query deadline must be bounded to60–900seconds')
        # Freeze caller data; the job ID is chosen before submission so an
        # ambiguous POST can never lead to an accidental second submission.
        query=json.loads(precise_json(query));query_id=uuid.uuid4().hex
        operation=self.evidence/'queries'/query_id;operation.mkdir(parents=True,mode=0o700)
        # Reserve one maximum in-flight socket wait (30s) plus10s cleanup.
        # read1 avoids a streaming body delaying the deadline indefinitely.
        overall=time.monotonic()+deadline_seconds;deadline=overall-40
        base=f'/api/projects/{project}/query/';path=base+query_id+'/'
        atomic_json(operation/'binding.json',{'api_host':self.config['api_host'],'project_id':project,
            'query_id':query_id,'query_node':query,'query_sha256':hashlib.sha256(precise_json(query).encode()).hexdigest(),
            'deadline_seconds':deadline_seconds,'submission_retries':0})
        submitted=False
        try:
            submitted=True
            response=self.request(base,{'query':query,'refresh':'force_async','client_query_id':query_id},
                method='POST',retry=False,timeout=30,deadline=deadline)
            while True:
                atomic_json(operation/'last-response-private.json',response)
                if not isinstance(response,dict):raise RuntimeError('Cloud query response must be an object')
                status=response.get('query_status')
                if status is None:
                    result=response
                else:
                    if not isinstance(status,dict) or not isinstance(status.get('id'),str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',status['id']) or status['id']!=query_id:
                        raise RuntimeError('Cloud asynchronous query ID changed or is unsafe')
                    if status.get('team_id',project)!=project:raise RuntimeError('Cloud asynchronous query project changed')
                    if type(status.get('complete')) is not bool or type(status.get('error')) is not bool:
                        raise RuntimeError('Cloud asynchronous query status is incomplete')
                    if status['error']:raise RuntimeError('Cloud asynchronous query failed; private evidence retained')
                    if not status['complete']:
                        self._delay(2,deadline)
                        response=self.request(path,retry=True,timeout=30,deadline=deadline)
                        continue
                    result=status.get('results')
                if not isinstance(result,dict) or not isinstance(result.get('results'),list) or result.get('error'):
                    raise RuntimeError('Completed Cloud query has no valid results')
                nested=result.get('query_status')
                if nested is not None and (not isinstance(nested,dict) or nested.get('error') is not False or nested.get('complete') is not True):
                    raise RuntimeError('Cloud query results are incomplete or failed')
                if time.monotonic()>deadline:raise TimeoutError('Cloud query deadline expired')
                atomic_json(operation/'complete-private.json',result)
                atomic_json(operation/'success.json',{'query_id':query_id,'project_id':project,
                    'complete':True,'error':False,'result_rows':len(result['results']),'submissions':1})
                return result
        except BaseException as error:
            cancellation={'query_id':query_id,'project_id':project,'query_cancellation_only':True,'acknowledged':False}
            if submitted:
                try:
                    cancelled=self.request(path,method='DELETE',retry=False,timeout=10,deadline=overall)
                    atomic_json(operation/'cancellation-response-private.json',cancelled)
                    cancellation['acknowledged']=True
                except BaseException:
                    cancellation['outstanding_job_possible']=True
            atomic_json(operation/'failure.json',{'error_type':type(error).__name__,'no_completed_result_certified':True,
                       'no_poll_resubmission':True,'cancellation':cancellation})
            raise

    def capture(self, events, historical=False):
        body = {'api_key':self.project['token'], 'batch':[cloud_event(event) for event in events]}
        if historical:
            body['historical_migration'] = True
        result = self._request(self.config['ingestion_host'] + '/batch/', body, None, {})
        if result.get('status') not in (1,'Ok'):
            raise RuntimeError('Cloud did not acknowledge capture')
        return result

    def query(self, sql):
        return self.query_node({'kind':'HogQLQuery','query':sql})['results']
