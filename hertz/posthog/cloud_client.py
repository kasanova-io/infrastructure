"""Bounded PostHog Cloud API/capture transport; private errors stay in evidence."""
import json
import http.client
from pathlib import Path
import time
import urllib.error
import urllib.request

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

    def request(self, path, payload=None, method=None, retry=True):
        if not path.startswith('/') or path.startswith('//'):
            raise ValueError('Only same-origin API paths allowed')
        return self._request(self.config['api_host'] + path, payload, method,
                             {'Authorization':'Bearer ' + self.config['personal_api_key']}, retries=5 if retry else 1)

    def _request(self, url, payload, method, headers, retries=5):
        for attempt in range(retries):
            request = urllib.request.Request(url, data=None if payload is None else precise_json(payload).encode(),
                method=method, headers={**headers, 'Content-Type':'application/json'})
            try:
                with self.opener.open(request, timeout=180) as response:
                    # A redirect must never send personal credentials to another origin.
                    if response.url != url:
                        raise RuntimeError('Unexpected Cloud response URL')
                    if response.status == 204:
                        return {}
                    raw = response.read()
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
                body = error.read().decode(errors='replace')
                atomic_json(self.evidence / f'cloud-http-error-{time.time_ns()}.json',
                            {'status':error.code, 'body':body, 'attempt':attempt})
                if error.code not in (429,500,502,503,504) or attempt == retries - 1:
                    raise RuntimeError(f'Cloud HTTP{error.code}; private error retained') from error
                retry = error.headers.get('Retry-After','')
                delay = min(60, int(retry)) if retry.isdigit() else min(30,2**attempt)
                time.sleep(delay)
            except (TimeoutError, urllib.error.URLError, http.client.HTTPException, ConnectionError):
                if attempt == retries - 1:
                    raise RuntimeError('Cloud transport failed; deterministic UUID retry remains safe')
                time.sleep(2**attempt)

    def capture(self, events, historical=False):
        body = {'api_key':self.project['token'], 'batch':[cloud_event(event) for event in events]}
        if historical:
            body['historical_migration'] = True
        result = self._request(self.config['ingestion_host'] + '/batch/', body, None, {})
        if result.get('status') not in (1,'Ok'):
            raise RuntimeError('Cloud did not acknowledge capture')
        return result

    def query(self, sql):
        result = self.request(f"/api/projects/{self.project['id']}/query/", {
            'query':{'kind':'HogQLQuery','query':sql}, 'refresh':'force_blocking'})
        if 'results' not in result:
            raise RuntimeError('Blocking query returned no result')
        return result['results']
