"""Source-bound missing-only SDK metadata repair using an original event UUID.

This is a separate, explicit migration proof. It does not weaken the general
event reconciler or authorize arbitrary historical profile operations.
"""
import hashlib

from daily_refill import IDENTITY_EVENTS, IDENTIFY_PROFILE_FIELDS, PERSON_OPERATIONS
from import_amplitude_events import canonical, json_values_equal
from prepare_amplitude_import import time_us
from prepare_cloud_import import retain_original, ORIGINAL, ORIGINAL_SHA


SDK_FIELDS = frozenset({
    '$app_build', '$app_name', '$app_namespace', '$app_version', '$device_type',
    '$os', '$os_name', '$os_version', '$screen_height', '$screen_width', '$raw_user_agent',
})
SDK_FIELDS |= frozenset('$initial_' + key[1:] for key in SDK_FIELDS)
MARKER = '__kasanova_missing_sdk_metadata_restore'
ARCHIVED = frozenset((*PERSON_OPERATIONS, *IDENTIFY_PROFILE_FIELDS, '$is_identified',
                      '$process_person_profile', '$geoip_disable', '$update_person_last_seen_at'))


def prepare(event, source_person, before):
    if event['event'] in (*IDENTITY_EVENTS, '$set', '$unset', '$snapshot') or any(event['properties'].get(k) for k in (
            '$groups', '$group_key', '$group_set', '$group_set_once', '$group_unset', '$anon_distinct_id')):
        raise RuntimeError('SDK restoration cannot execute identity or group operations')
    if (set(source_person['distinct_ids']) != set(before['distinct_ids'])
            or event['distinct_id'] not in source_person['distinct_ids']
            or type(source_person.get('is_identified')) is not bool
            or type(before.get('is_identified')) is not bool
            or source_person['is_identified'] != before.get('is_identified')
            or time_us(event['timestamp']) < time_us(before['created_at'])):
        raise RuntimeError('SDK restoration requires the exact established source identity/state')
    missing = {k: v for k, v in source_person['properties'].items()
               if k in SDK_FIELDS and k not in before['properties']}
    if not missing:
        raise RuntimeError('No missing source SDK metadata')
    if MARKER in event['properties']:
        raise RuntimeError('SDK restoration namespace collision')
    replay = retain_original(event)
    for key in ARCHIVED:
        replay['properties'].pop(key, None)
    binding = hashlib.sha256(canonical(source_person).encode()).hexdigest()
    replay['properties'].update({MARKER: binding, '$set_once': missing,
                                '$process_person_profile': True, '$geoip_disable': True,
                                '$update_person_last_seen_at': False})
    return replay


def verify(event, source_person, before, after, stored, logical_count):
    """Require trusted sealed source context and exact current profile readback."""
    expected = prepare(event, source_person, before)
    uid, name, identity, stamp, props = stored
    if (type(logical_count) is not int or logical_count != 1 or uid != event['uuid'] or name != event['event'] or identity != event['distinct_id']
            or time_us(stamp) != time_us(event['timestamp']) or not isinstance(props, dict)):
        raise RuntimeError('SDK restoration changed event headers or normal activity count')
    if (after['id'] != before['id'] or set(after['distinct_ids']) != set(before['distinct_ids'])
            or type(after.get('is_identified')) is not bool
            or after.get('is_identified') != before.get('is_identified')
            or after['created_at'] != before['created_at']):
        raise RuntimeError('SDK restoration changed current identity/state/creation')
    wanted_properties = {**before['properties'], **expected['properties']['$set_once']}
    if not json_values_equal(after['properties'], wanted_properties):
        raise RuntimeError('SDK restoration overwrote current properties or failed missing values')
    # Real Cloud fixture retains the exact $set_once and consumes the true
    # processing control. No generic person-operation suppression is allowed.
    if ('$set' in props or '$unset' in props or '$is_identified' in props
            or any(key in props for key in IDENTIFY_PROFILE_FIELDS)
            or ('$process_person_profile' in props and props['$process_person_profile'] is not True)
            or any(props.get(k) for k in ('$groups', '$group_key', '$group_set', '$group_set_once',
                                         '$group_unset', '$anon_distinct_id'))):
        raise RuntimeError('SDK restoration contains an unapproved profile operation')
    for key, value in expected['properties'].items():
        if key == '$process_person_profile':
            continue
        if key not in props or not json_values_equal(props[key], value):
            raise RuntimeError('SDK restoration differs from trusted source-bound replay')
    raw = props.get(ORIGINAL)
    if (raw != canonical(event) or props.get(ORIGINAL_SHA) != hashlib.sha256(raw.encode()).hexdigest()):
        raise RuntimeError('SDK restoration lost the exact original event')
    return {'verified': True, 'original_uuid': event['uuid'], 'normal_activity_count': logical_count,
            'missing_sdk_properties_restored': len(expected['properties']['$set_once']),
            'source_person_sha256': expected['properties'][MARKER],
            'current_properties_identity_and_creation_preserved': True}
