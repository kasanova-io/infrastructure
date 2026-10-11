#!/usr/bin/env python3
"""Recreate archived behavioral rules as native, recalculating PostHog cohorts.

No vendor calls, profile changes or PROD event writes. Validate on real DEV first.
Private inputs/evidence stay outside Git. SQL event filters preserve rolling
windows, first-ever activity and the retention bracket without static member lists.
"""
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
import uuid

from import_amplitude_events import atomic_json

CREDENTIALS = Path('/home/ren/Kasanova/secrets/posthog/bootstrap.json')
MARKER = 'Kasanova archived behavioral cohort recreation v1.'
ACTION_NAME = 'All analytics events'
NAMES = [
    'All active users in the last 30 days',
    'New users in the last 30 days',
    '14 day user retention',
    'Active during the last 4 consecutive weeks',
    'Product qualified leads',
    "New users who didn't return",
]


def quote(value):
    return "'" + value.replace('\\', '\\\\').replace("'", "\\'") + "'"


class Client:
    def __init__(self, evidence):
        self.evidence = evidence
        self.credentials = json.loads(CREDENTIALS.read_text())
        self.base = self.credentials['url']
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar))
        result = self.request('/api/login/', {
            'email': self.credentials['email'], 'password': self.credentials['password']})
        if not result.get('success'):
            raise RuntimeError('PostHog owner login failed')

    def request(self, path, payload=None, method=None):
        if not path.startswith('/') or path.startswith('//'):
            raise ValueError('Only same-origin PostHog paths allowed')
        headers = {'Content-Type': 'application/json', 'Referer': self.base + '/login'}
        token = next((x.value for x in self.jar if x.name == 'posthog_csrftoken'), None)
        if token:
            headers['X-CSRFToken'] = token
        data = None if payload is None else json.dumps(payload).encode()
        try:
            with self.opener.open(urllib.request.Request(
                    self.base + path, data=data, method=method, headers=headers), timeout=180) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            # Keep provider error bodies out of stdout; they can contain private identifiers.
            atomic_json(self.evidence / 'last-api-error.json', {
                'path': path, 'status': error.code,
                'body': error.read().decode(errors='replace')[:20000]})
            raise RuntimeError(f'PostHog {method or ("POST" if data else "GET")} {path}: HTTP {error.code}') from error

    def project(self, env):
        return self.credentials['projects'][env]['id']

    def query(self, env, sql):
        result = self.request(f'/api/projects/{self.project(env)}/query/', {
            'query': {'kind': 'HogQLQuery', 'query': sql}, 'refresh': 'force_blocking'})
        if 'results' not in result:
            raise RuntimeError('Blocking query has no results')
        return result['results']

    def cohorts(self, env):
        path = f'/api/projects/{self.project(env)}/cohorts/?limit=100'
        results = []
        while path:
            result = self.request(path)
            results.extend(result['results'])
            next_path = result.get('next')
            if next_path and not next_path.startswith(self.base + '/'):
                raise RuntimeError('Unexpected cohort pagination origin')
            path = next_path[len(self.base):] if next_path else None
        return results


def validate_definitions(cohorts):
    """Fail closed if these archived rules are different from the audited six."""
    if len(cohorts) != 6 or set(x['name'] for x in cohorts) != set(NAMES):
        raise ValueError('Expected exactly the six audited source cohorts')
    for source in cohorts:
        definition = source['definition']
        if source.get('appId') != 734469 or definition.get('cohortType') != 'UNIQUES':
            raise ValueError('Source project or cohort type differs')
        if definition.get('countGroup') != {'name': 'User', 'is_computed': False}:
            raise ValueError('Only person cohorts supported')
        rules = definition['andClauses']
        clauses = [x['orClauses'][0] for x in rules]
        if any(len(x['orClauses']) != 1 for x in rules):
            raise ValueError('Unexpected compound source rule')
        if any(x.get('time_type') != 'rolling' or x.get('offset') != 0
               or x.get('exclude_current_interval') is not False for x in clauses):
            raise ValueError('Unexpected date window')
        index = NAMES.index(source['name'])
        expected_types = [['event'], ['new_active'], ['retention'],
                          ['distinct_interval'], ['event'], ['new_active', 'event', 'event']][index]
        expected_windows = [[30], [30], [45], [28], [7], [8, 8, 7]][index]
        if [x['type'] for x in clauses] != expected_types or [x['time_value'] for x in clauses] != expected_windows:
            raise ValueError('Source behavioral rules differ')
        if [x['negated'] for x in rules] != ([False, False, True] if index == 5 else [False]):
            raise ValueError('Source negation differs')
        for x in clauses:
            if x['type'] == 'event' and (x.get('type_value') != '_active'
                    or x.get('operator') != '>=' or x.get('operator_value') != (3 if index == 4 else 1)
                    or x.get('group_by')):
                raise ValueError('Source event rule differs')
            if x['type'] == 'new_active' and x.get('type_value') != 'new':
                raise ValueError('Source first-seen rule differs')
            if x['type'] == 'retention' and (x.get('retention_method') != 'bracket'
                    or x.get('base_bracket') != [14, 15] or x.get('retained') is not True
                    or x.get('start_event') != {'filters': [], 'group_by': [], 'event_type': '_new'}
                    or x.get('return_event') != {'filters': [], 'group_by': [], 'event_type': '_active'}):
                raise ValueError('Source retention bracket differs')
            if x['type'] == 'distinct_interval' and (x.get('period') != 7 or x.get('count') != 1
                    or x.get('stickiness_type') != 'STICKY_IN_ALL_PERIODS'
                    or x.get('event') != {'filters': [], 'group_by': [], 'event_type': '_active'}):
                raise ValueError('Source consecutive-period rule differs')


def plans(cohorts, inactive, scope=None):
    validate_definitions(cohorts)
    active = '1 = 1' if not inactive else 'event NOT IN (' + ','.join(quote(x) for x in inactive) + ')'
    scope_pred = '' if scope is None else ' AND distinct_id LIKE ' + quote(scope + '%')
    base = 'FROM events WHERE timestamp <= now()' + scope_pred
    having = [
        f'countIf(({active}) AND timestamp >= now() - INTERVAL 30 DAY) >= 1',
        'min(timestamp) >= now() - INTERVAL 30 DAY',
        None,
        ' AND '.join(f'countIf(({active}) AND timestamp >= now() - INTERVAL {7*(i+1)} DAY '
                     f'AND timestamp < now() - INTERVAL {7*i} DAY) >= 1' for i in range(4)),
        f'countIf(({active}) AND timestamp >= now() - INTERVAL 7 DAY) >= 3',
        f'min(timestamp) >= now() - INTERVAL 8 DAY AND '
        f'countIf(({active}) AND timestamp >= now() - INTERVAL 8 DAY) >= 1 AND '
        f'countIf(({active}) AND timestamp >= now() - INTERVAL 7 DAY) = 0',
    ]
    descriptions = [
        'At least one active event in the rolling last 30 days.',
        'First-ever recorded event in the rolling last 30 days, including inactive events.',
        'First-ever event in the last 45 days and an active return 14 to less than 15 days later.',
        'At least one active event in each of four consecutive rolling seven-day periods.',
        'At least three active events in the rolling last seven days.',
        'First event in the last eight days, active in that window, with no active event in the last seven days.',
    ]
    result = []
    by_name = {x['name']: x for x in cohorts}
    for i, name in enumerate(NAMES):
        if i == 2:
            active_e = '1 = 1' if not inactive else 'e.event NOT IN (' + ','.join(quote(x) for x in inactive) + ')'
            scope_e = '' if scope is None else ' AND e.distinct_id LIKE ' + quote(scope + '%')
            sql = (f'SELECT DISTINCT e.person_id FROM events e INNER JOIN '
                   f'(SELECT person_id,min(timestamp) AS first_seen {base} GROUP BY person_id) b '
                   f'ON e.person_id = b.person_id WHERE e.timestamp <= now() AND ({active_e}){scope_e} '
                   'AND b.first_seen >= now() - INTERVAL 45 DAY '
                   'AND e.timestamp >= b.first_seen + INTERVAL 14 DAY '
                   'AND e.timestamp < b.first_seen + INTERVAL 15 DAY')
        else:
            sql = f'SELECT person_id {base} GROUP BY person_id HAVING {having[i]}'
        filters = {'properties': {'type': 'AND', 'values': [{
            'type': 'behavioral', 'key': '__all_analytics_events__', 'value': 'performed_event', 'event_type': 'actions',
            'time_value': 45, 'time_interval': 'day',
            'event_filters': [{'type': 'hogql', 'key': 'person_id IN (' + sql + ')'}],
        }]}}
        result.append({'name': name, 'description': MARKER + ' ' + descriptions[i],
                       'is_static': False, 'filters': filters, 'membership_sql': sql,
                       'source_definition': by_name[name]['definition']})
    return result


def ensure_all_events_action(client, env):
    """A native catch-all action includes future event names without emitting events."""
    result = client.request(f'/api/projects/{client.project(env)}/actions/?limit=100')
    if result.get('next'):
        raise RuntimeError('Action inventory exceeds preflight page; do not create duplicates')
    matches = [x for x in result['results'] if not x.get('deleted') and x['name'] == ACTION_NAME]
    payload = {'name': ACTION_NAME, 'description': MARKER + ' Every analytics event.',
               'steps': [{'event': None, 'properties': [{'type': 'hogql', 'key': 'true'}]}],
               'post_to_slack': False}
    if matches:
        if len(matches) != 1 or matches[0].get('description') != payload['description']:
            raise RuntimeError('Existing all-events action is not owned by this recreation')
        action = matches[0]
        steps = action.get('steps', [])
        if len(steps) != 1 or steps[0].get('event') is not None or steps[0].get('properties') != payload['steps'][0]['properties']:
            raise RuntimeError('Owned all-events action was modified; preserve it')
    else:
        action = client.request(f'/api/projects/{client.project(env)}/actions/', payload)
    return action['id']


def bind_action(plan, action_id):
    plan['filters']['properties']['values'][0]['key'] = action_id


def ensure_cohort(client, env, plan, existing):
    matches = [x for x in existing if not x.get('deleted') and x['name'] == plan['name']]
    if len(matches) > 1:
        raise RuntimeError('Duplicate named cohorts; preserve existing objects')
    payload = {k: plan[k] for k in ['name', 'description', 'is_static', 'filters']}
    if matches:
        current = matches[0]
        if current.get('description') != payload['description'] or current.get('is_static'):
            raise RuntimeError('Existing cohort is not owned by this recreation; preserve it')
        # Persisted native filters may include generated bytecode. Compare only author fields.
        def clean(value):
            if isinstance(value, list): return [clean(x) for x in value]
            if isinstance(value, dict):
                return {k: clean(v) for k, v in value.items()
                        if k not in ['bytecode', 'bytecode_error', 'conditionHash'] and v is not None
                        and not (k == 'negation' and v is False)}
            return value
        if clean(current['filters']) != clean(payload['filters']):
            if env == 'dev' and plan['name'].startswith('[Cohort validation '):
                return client.request(f'/api/projects/{client.project(env)}/cohorts/{current["id"]}/', payload, method='PATCH')
            raise RuntimeError('Existing owned cohort rules changed; preserve them')
        return current
    cohort = client.request(f'/api/projects/{client.project(env)}/cohorts/', payload)
    existing.append(cohort)
    return cohort


def wait_cohort(client, env, cohort_id):
    for _ in range(100):
        value = client.request(f'/api/projects/{client.project(env)}/cohorts/{cohort_id}/')
        if value.get('errors_calculating') or value.get('last_error_message'):
            raise RuntimeError(f'Cohort {cohort_id} calculation failed')
        if not value['is_calculating'] and value.get('last_calculation'):
            return value
        time.sleep(3)
    raise RuntimeError(f'Cohort {cohort_id} did not calculate')


def validate_dev(client, cohorts, inactive, evidence):
    marker_file = evidence / 'dev-fixtures.json'
    if marker_file.exists():
        fixtures = json.loads(marker_file.read_text())
    else:
        prefix = 'kasanova-cohort-validation-' + uuid.uuid4().hex + '-'
        reference = dt.datetime.now(dt.timezone.utc)
        histories = {
            'regular': [27.5, 20.5, 13.5, 6.5], 'qualified': [2.5, 1.5, .5],
            'new': [1], 'no_return': [7.5], 'oldactive': [60, 2],
            'return14': [20, 6], 'return13': [20, 7], 'return15': [20, 5],
            'older45return14': [60, 46, 2], 'weekly_gap': [27.5, 20.5, 6.5],
            'new31': [31.5, .5], 'inactive_only': [.5],
        }
        events = []
        for label, ages in histories.items():
            for age in ages:
                events.append({'uuid': str(uuid.uuid4()), 'event': 'kasanova_cohort_inactive_fixture'
                               if label == 'inactive_only' else 'kasanova_cohort_rule_fixture',
                               'distinct_id': prefix + label,
                               'timestamp': (reference - dt.timedelta(days=age)).isoformat(),
                               'properties': {'__kasanova_cohort_validation': True}})
        fixtures = {'prefix': prefix, 'events': events, 'reference': reference.isoformat()}
        atomic_json(marker_file, fixtures)
    before_prod = client.query('prod', 'SELECT count(),uniqExact(uuid) FROM events')[0]
    if not fixtures.get('capture_acknowledged'):
        observed = client.query('dev', 'SELECT count(),uniqExact(uuid) FROM events WHERE distinct_id LIKE '
                                + quote(fixtures['prefix'] + '%'))[0]
        if observed != [len(fixtures['events']), len(fixtures['events'])]:
            if observed != [0, 0] or fixtures.get('capture_pending'):
                raise RuntimeError('Uncertain or partial DEV fixture delivery; do not resend')
            fixtures['capture_pending'] = True
            atomic_json(marker_file, fixtures)
            result = client.request('/batch/', {
                'api_key': client.credentials['projects']['dev']['api_key'], 'batch': fixtures['events']})
            if result.get('status') not in (1, 'Ok'):
                raise RuntimeError('DEV fixture capture not acknowledged')
        fixtures['capture_acknowledged'] = True
        atomic_json(marker_file, fixtures)
    prefix = fixtures['prefix']
    for _ in range(60):
        count = client.query('dev', 'SELECT count() FROM events WHERE distinct_id LIKE ' + quote(prefix + '%'))[0][0]
        if count == len(fixtures['events']): break
        time.sleep(2)
    else: raise RuntimeError('DEV fixture events did not all arrive')
    expected = [
        {'regular', 'qualified', 'new', 'no_return', 'oldactive', 'return14', 'return13', 'return15', 'older45return14', 'weekly_gap', 'new31'},
        {'regular', 'qualified', 'new', 'no_return', 'return14', 'return13', 'return15', 'weekly_gap', 'inactive_only'},
        {'regular', 'return14'}, {'regular'}, {'qualified'}, {'no_return'},
    ]
    dev_plans = plans(cohorts, inactive + ['kasanova_cohort_inactive_fixture'], prefix)
    action_id = ensure_all_events_action(client, 'dev')
    existing = client.cohorts('dev')
    report = []
    for i, plan in enumerate(dev_plans):
        plan['name'] = '[Cohort validation ' + prefix[-9:-1] + '] ' + plan['name']
        bind_action(plan, action_id)
        actual = {r[0][len(prefix):] for r in client.query('dev',
            'SELECT DISTINCT distinct_id FROM events WHERE person_id IN (' + plan['membership_sql'] + ')')}
        if actual != expected[i]:
            raise RuntimeError(f'DEV query rule {i} disagrees with acceptance fixtures: {sorted(actual)}')
        cohort = ensure_cohort(client, 'dev', plan, existing)
        calculated = wait_cohort(client, 'dev', cohort['id'])
        native = {r[0][len(prefix):] for r in client.query('dev',
            f'SELECT DISTINCT distinct_id FROM events WHERE person_id IN COHORT {cohort["id"]}')}
        if native != expected[i] or calculated['count'] != len(expected[i]):
            raise RuntimeError(f'DEV native cohort rule {i} disagrees with acceptance fixtures')
        report.append({'source_name': NAMES[i], 'dev_cohort_id': cohort['id'],
                       'members': len(native), 'positive_and_negative_fixtures_match': True})
        atomic_json(evidence / 'dev-progress.json', report)
        print(json.dumps({'dev_rule_verified': NAMES[i], 'members': len(native)}), flush=True)
    if client.query('prod', 'SELECT count(),uniqExact(uuid) FROM events')[0] != before_prod:
        raise RuntimeError('PROD event inventory changed during DEV verification')
    if client.query('prod', 'SELECT count() FROM events WHERE distinct_id LIKE ' + quote(prefix + '%'))[0][0] != 0:
        raise RuntimeError('DEV fixture leaked into PROD')
    atomic_json(evidence / 'dev-verification.json', {
        'verified_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'passed': True,
        'rules': report, 'fixture_count': len(fixtures['events']),
        'prod_unchanged': True, 'prod_fixture_count': 0,
        'fixture_prefix': prefix, 'plan_sha256': plan_hash(cohorts, inactive)})


def plan_hash(cohorts, inactive):
    return hashlib.sha256(json.dumps(plans(cohorts, inactive), sort_keys=True).encode()).hexdigest()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['plan', 'validate-dev', 'apply-prod'])
    parser.add_argument('source_cohorts', type=Path)
    parser.add_argument('source_taxonomy', type=Path)
    parser.add_argument('evidence', type=Path)
    args = parser.parse_args()
    source = json.loads(args.source_cohorts.read_text())
    cohorts = source['cohorts']
    taxonomy = json.loads(args.source_taxonomy.read_text())['data']
    inactive = sorted(x['event_type'] for x in taxonomy if x.get('is_active') is False)
    definition_plans = plans(cohorts, inactive)
    args.evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    client = Client(args.evidence)
    if client.project('prod') == client.project('dev'):
        raise RuntimeError('Project isolation is invalid')
    if args.mode == 'validate-dev':
        return validate_dev(client, cohorts, inactive, args.evidence)
    result = {'checked_at': dt.datetime.now(dt.timezone.utc).isoformat(),
              'plan_sha256': plan_hash(cohorts, inactive),
              'prod_project': client.project('prod'), 'rules': []}
    if args.mode == 'apply-prod':
        proof = json.loads((args.evidence / 'dev-verification.json').read_text())
        if not proof.get('passed') or proof['plan_sha256'] != result['plan_sha256']:
            raise RuntimeError('Matching real DEV verification is required')
    before = client.query('prod', 'SELECT count(),uniqExact(uuid) FROM events')[0]
    existing = client.cohorts('prod')
    dev_before = [x['id'] for x in client.cohorts('dev')]
    # Compile and execute every rule before the first PROD cohort mutation.
    for plan in definition_plans:
        count = client.query('prod', 'SELECT count() FROM (' + plan['membership_sql'] + ')')[0][0]
        result['rules'].append({'name': plan['name'], 'queried_members': count})
    atomic_json(args.evidence / 'prod-plan.json', result)
    if args.mode == 'plan':
        print(json.dumps(result), flush=True)
        return
    action_id = ensure_all_events_action(client, 'prod')
    result['all_events_action_id'] = action_id
    exported = []
    for plan, rule in zip(definition_plans, result['rules']):
        bind_action(plan, action_id)
        cohort = ensure_cohort(client, 'prod', plan, existing)
        calculated = wait_cohort(client, 'prod', cohort['id'])
        native = client.query('prod', f'SELECT uniqExact(person_id) FROM events WHERE person_id IN COHORT {cohort["id"]}')[0][0]
        fresh_expected = client.query('prod', 'SELECT count() FROM (' + plan['membership_sql'] + ')')[0][0]
        if calculated['count'] != fresh_expected or native != fresh_expected:
            raise RuntimeError('PROD native calculation differs from saved rule query')
        if fresh_expected >= 10000:
            raise RuntimeError('Membership verification needs pagination; refuse a truncated proof')
        native_ids = {r[0] for r in client.query('prod',
            f'SELECT DISTINCT person_id FROM events WHERE person_id IN COHORT {cohort["id"]} LIMIT 10000')}
        expected_ids = {r[0] for r in client.query('prod', plan['membership_sql'] + ' LIMIT 10000')}
        if native_ids != expected_ids or len(native_ids) != fresh_expected:
            raise RuntimeError('PROD native member identities differ from saved rule query')
        rule.update({'cohort_id': cohort['id'], 'native_members': native, 'is_static': calculated['is_static'],
                     'last_calculation': calculated['last_calculation'], 'native_query_matches': True,
                     'all_member_identities_match': True,
                     'member_set_sha256': hashlib.sha256(json.dumps(sorted(native_ids)).encode()).hexdigest(),
                     'url': client.base + f'/project/{client.project("prod")}/cohorts/{cohort["id"]}'})
        exported.append(calculated)
        atomic_json(args.evidence / 'prod-progress.json', result)
        print(json.dumps(rule), flush=True)
    after = client.query('prod', 'SELECT count(),uniqExact(uuid) FROM events')[0]
    if before != after or before != [557054, 557054]:
        raise RuntimeError('Original PROD event inventory changed')
    if dev_before != [x['id'] for x in client.cohorts('dev')]:
        raise RuntimeError('PROD recreation changed DEV cohort inventory')
    result.update({'verified_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                   'prod_original_events': after[0], 'prod_original_events_unchanged': True,
                   'dev_cohort_inventory_unchanged': True, 'original_membership_snapshot_exported': False,
                   'all_six_recreated_as_dynamic_cohorts': True, 'application_connector_changed': False})
    atomic_json(args.evidence / 'prod-native-definitions.json', {
        'cohorts': exported,
        'all_events_action': client.request(f'/api/projects/{client.project("prod")}/actions/{action_id}/')})
    atomic_json(args.evidence / 'prod-verification.json', result)


if __name__ == '__main__':
    main()
