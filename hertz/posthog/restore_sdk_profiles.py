"""Restore missing SDK profile values through PostHog's maintenance `$set` event.

Ordinary source events are never re-ingested. The new maintenance event is
explicitly auditable; it is not counted as an original historical event. Caller
must seal source/project bindings and the fresh current profile before capture.
"""
import hashlib
import json
import uuid

from import_amplitude_events import canonical, json_values_equal
from prepare_amplitude_import import time_us
from restore_missing_sdk import prepare as prepare_legacy

MARKER = '__kasanova_sdk_profile_maintenance'
NAMESPACE = uuid.UUID('c28cce91-c80b-48fe-825c-30b4c8cb0798')


def prepare(event, source_person, before, context, executed_at):
    if (context.get('environment') not in ('dev', 'prod')
            or any(type(context.get(key)) is not int or context[key] <= 0
                   for key in ('source_project_id', 'cloud_project_id'))
            or not isinstance(context.get('source_manifest_sha256'), str)
            or len(context['source_manifest_sha256']) != 64
            or any(c not in '0123456789abcdef' for c in context['source_manifest_sha256'])):
        raise RuntimeError('Missing sealed source/project context')
    # Reuse the established source identity, state, time and SDK-whitelist guards.
    missing = prepare_legacy(event, source_person, before)['properties']['$set_once']
    if time_us(executed_at) % 1000:
        raise RuntimeError('Maintenance timestamp must use exact millisecond precision')
    if time_us(executed_at) < max(time_us(event['timestamp']), time_us(before['created_at'])):
        raise RuntimeError('Maintenance timestamp predates the established person/source')
    binding = {
        **context, 'source_person_sha256': hashlib.sha256(canonical(source_person).encode()).hexdigest(),
        'before_person_sha256': hashlib.sha256(canonical(before).encode()).hexdigest(),
        'source_event_sha256': hashlib.sha256(canonical(event).encode()).hexdigest(),
        'executed_at': executed_at, 'purpose': 'restore_missing_sdk_profile_values',
    }
    audit_id = str(uuid.uuid5(NAMESPACE, canonical(binding)))
    if audit_id == event['uuid']:
        raise RuntimeError('Maintenance event must not reuse a source UUID')
    return {'uuid': audit_id, 'event': '$set', 'distinct_id': event['distinct_id'],
            'timestamp': executed_at, 'properties': {
                '$set_once': missing, '$process_person_profile': True,
                '$geoip_disable': True, '$update_person_last_seen_at': False,
                MARKER: binding,
            }}


def verify(event, source_person, before, context, executed_at, after, stored, audit_rows):
    expected = prepare(event, source_person, before, context, executed_at)
    uid, name, identity, stamp, props = stored
    if (type(audit_rows) is not int or audit_rows < 1
            or uid != expected['uuid'] or name != '$set' or identity != expected['distinct_id']
            or time_us(stamp) != time_us(executed_at) or not isinstance(props, dict)):
        raise RuntimeError('Maintenance event header/count differs from the sealed plan')
    if (after['id'] != before['id'] or set(after['distinct_ids']) != set(before['distinct_ids'])
            or type(after.get('is_identified')) is not bool
            or after['is_identified'] != before['is_identified']
            or after['created_at'] != before['created_at']
            or 'last_seen_at' not in before or 'last_seen_at' not in after
            or not json_values_equal(after['last_seen_at'], before['last_seen_at'])):
        raise RuntimeError('Maintenance changed current identity/state/creation/last-seen')
    if not json_values_equal(after['properties'], {**before['properties'], **expected['properties']['$set_once']}):
        raise RuntimeError('Maintenance overwrote current values or failed missing SDK values')
    if any(key in props for key in ('$set', '$unset', '$is_identified', '$anon_distinct_id',
                                    '$groups', '$group_key', '$group_set', '$group_set_once', '$group_unset',
                                    '$session_id', '$device_id')):
        raise RuntimeError('Unexpected identity/group/session or overwriting operation')
    for key, value in expected['properties'].items():
        if key == '$process_person_profile':
            if key in props and props[key] is not True:
                raise RuntimeError('Invalid person-processing control')
        elif key not in props or not json_values_equal(props[key], value):
            raise RuntimeError('Maintenance payload differs from the sealed plan')
    return {'verified': True, 'audit_uuid': uid, 'audit_event': '$set',
            'audit_physical_rows': audit_rows, 'audit_rows_deduplicated': audit_rows == 1,
            'missing_sdk_values_restored': len(expected['properties']['$set_once']),
            'current_profile_identity_and_creation_preserved': True,
            'last_seen_at_preserved': True,
            'ordinary_event_immutability_requires_separate_readback': True}


def verify_ordinary_events(before, after):
    """Compare unfiltered original UUID counts and persisted values, not a dedup view."""
    if not before or set(before) != set(after):
        raise RuntimeError('Original UUID coverage changed')
    for uid, old in before.items():
        new = after[uid]
        if (old[0] != uid or new[0] != uid or type(old[2]) is not int or old[2] != 1
                or type(new[2]) is not int or new[2] != 1
                or old[1][:2] != new[1][:2] or time_us(old[1][2]) != time_us(new[1][2])):
            raise RuntimeError('An original ordinary event header or unfiltered count changed')
        old_props = json.loads(old[1][3]) if isinstance(old[1][3], str) else old[1][3]
        new_props = json.loads(new[1][3]) if isinstance(new[1][3], str) else new[1][3]
        if not json_values_equal(old_props, new_props):
            raise RuntimeError('Original ordinary event properties changed')
    return {'original_uuid_count': len(before), 'unfiltered_count_each': 1,
            'original_headers_timestamps_typed_properties_unchanged': True}
