#!/usr/bin/env python3
"""Exercise real HTTPS login, capture and project-scoped queries without logging secrets."""
import datetime as dt
import http.cookiejar
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent
CREDENTIALS = Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json')


def main():
    os.umask(0o077)
    credentials = json.loads(CREDENTIALS.read_text())
    base = credentials['url']
    jar = http.cookiejar.CookieJar()
    client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    def request(path, payload=None):
        headers = {'Content-Type': 'application/json', 'Referer': base + '/login',
                   'User-Agent': 'Kasanova-PostHog-installation-verification'}
        token = next((c.value for c in jar if c.name == 'csrftoken'), None)
        if token:
            headers['X-CSRFToken'] = token
        req = urllib.request.Request(base + path, data=None if payload is None else json.dumps(payload).encode(), headers=headers)
        with client.open(req, timeout=30) as response:
            return response.status, response.read()

    report = {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'url': base}
    report['https_health_status'], _ = request('/_health')
    report['login_page_status'], html = request('/login')
    assets = [x for x in re.findall(r'<script[^>]+src=["\']([^"\']+)', html.decode()) if x.startswith('/') and '.js' in x]
    if not assets:
        raise RuntimeError('Login page has no local JavaScript asset')
    report['frontend_asset_status'], _ = request(assets[0])
    status, body = request('/api/login/', {'email': credentials['email'], 'password': credentials['password']})
    if status != 200 or not json.loads(body).get('success'):
        raise RuntimeError('Owner login did not succeed')
    status, body = request('/api/user/')
    if status != 200 or json.loads(body).get('email') != credentials['email']:
        raise RuntimeError('Authenticated owner identity differs')
    report['owner_login_verified'] = True
    ops = ROOT / 'operations'
    ops.mkdir(mode=0o700, exist_ok=True)
    marker_path = ops / 'diagnostic-marker.json'
    marker = json.loads(marker_path.read_text()) if marker_path.exists() else {'uuid': str(uuid.uuid4()), 'timestamp': dt.datetime.now(dt.timezone.utc).isoformat()}
    marker_path.write_text(json.dumps(marker) + '\n')
    identifier = 'posthog-install-' + marker['uuid']
    event = {'event': 'posthog_installation_check', 'distinct_id': identifier,
             'uuid': marker['uuid'], 'timestamp': marker['timestamp'],
             'properties': {'installation_marker': marker['uuid'], 'environment': 'dev',
                            'number': 42, 'boolean': True, 'nested': {'preserved': 'yes'}}}
    status, _ = request('/batch/', {'api_key': credentials['projects']['dev']['api_key'], 'batch': [event]})
    report['dev_capture_status'] = status
    sql = "SELECT event, distinct_id, properties FROM events WHERE uuid = '" + marker['uuid'] + "'"

    def query(environment):
        project = credentials['projects'][environment]['id']
        status, body = request('/api/projects/' + str(project) + '/query/',
                               {'query': {'kind': 'HogQLQuery', 'query': sql}, 'refresh': 'force_blocking'})
        data = json.loads(body)
        if status != 200 or 'results' not in data:
            raise RuntimeError('Blocking project query did not return results')
        return data['results']

    for attempt in range(60):
        try:
            rows = query('dev')
            if rows:
                break
        except urllib.error.HTTPError as error:
            if error.code not in (500, 502, 503):
                raise
        time.sleep(2)
    else:
        raise RuntimeError('Captured DEV event did not become queryable')
    if len(rows) != 1 or rows[0][:2] != ['posthog_installation_check', identifier]:
        raise RuntimeError('Captured diagnostic event fields changed or were duplicated')
    properties = json.loads(rows[0][2]) if isinstance(rows[0][2], str) else rows[0][2]
    if any(properties.get(k) != v for k, v in event['properties'].items()):
        raise RuntimeError('Captured typed properties changed')
    if query('prod'):
        raise RuntimeError('DEV diagnostic event leaked into PROD')
    report.update({'dev_event_query_and_typed_properties_verified': True,
                   'prod_dev_project_isolation_verified': True,
                   'projects': {k: v['id'] for k, v in credentials['projects'].items()},
                   'diagnostic_uuid': marker['uuid'], 'prod_source_events_imported': 0,
                   'amplitude_connector_changed': False,
                   'completed_at': dt.datetime.now(dt.timezone.utc).isoformat()})
    (ops / 'installation-verification.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
