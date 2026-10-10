#!/usr/bin/env python3
"""Verify normal PostHog queries count each archived PROD event exactly once."""
import argparse
from collections import Counter
import datetime as dt
import gzip
import http.cookiejar
import json
import os
from pathlib import Path
import urllib.request

from import_amplitude_events import atomic_json

CREDENTIALS = Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json')


def main(source, evidence):
    os.umask(0o077)
    credentials = json.loads(CREDENTIALS.read_text())
    base = credentials['url']
    jar = http.cookiejar.CookieJar()
    client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    def request(path, payload):
        headers = {'Content-Type': 'application/json', 'Referer': base + '/login'}
        token = next((c.value for c in jar if c.name == 'posthog_csrftoken'), None)
        if token: headers['X-CSRFToken'] = token
        with client.open(urllib.request.Request(base + path, data=json.dumps(payload).encode(), headers=headers), timeout=180) as response:
            return json.loads(response.read())
    request('/api/login/', {'email': credentials['email'], 'password': credentials['password']})
    def query(project, sql):
        return request('/api/projects/' + str(project) + '/query/', {
            'query': {'kind': 'HogQLQuery', 'query': sql}, 'refresh': 'force_blocking'})['results']
    expected = Counter()
    with gzip.open(source / 'events.ndjson.gz', 'rb') as stream:
        for line in stream: expected[json.loads(line)['event']] += 1
    prod = credentials['projects']['prod']['id']
    rows = query(prod, 'SELECT event,count() FROM events GROUP BY event LIMIT 10000')
    actual = {name: int(count) for name, count in rows}
    aggregate = query(prod, 'SELECT count(),uniqExact(uuid),countIf(properties.__amplitude_validation_fixture = true) FROM events')[0]
    dev = query(credentials['projects']['dev']['id'], "SELECT count(),uniqExact(uuid) FROM events WHERE event = 'posthog_precision_validation'")[0]
    passed = actual == dict(expected) and int(aggregate[0]) == sum(expected.values()) and int(aggregate[1]) == sum(expected.values()) and int(aggregate[2]) == 0
    report = {'verified_at': dt.datetime.now(dt.timezone.utc).isoformat(),
        'prod_project_id': prod, 'expected_events': sum(expected.values()), 'normal_query_prod_rows': int(aggregate[0]),
        'normal_query_unique_uuids': int(aggregate[1]), 'observed_event_types': len(actual),
        'all_event_type_counts_match_source': actual == dict(expected),
        'prod_validation_fixtures': int(aggregate[2]),
        'dev_synthetic_rows': int(dev[0]), 'dev_synthetic_unique_uuids': int(dev[1]),
        'normal_posthog_prod_queries_count_each_original_event_once': passed,
        'source_connector_changed': False}
    atomic_json(evidence / 'normal-query-count-verification.json', report)
    print(json.dumps(report), flush=True)
    if not passed: raise RuntimeError('Normal PostHog query counts do not match the source')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('prepared_directory', type=Path)
    parser.add_argument('evidence', type=Path)
    args = parser.parse_args()
    main(args.prepared_directory, args.evidence)
