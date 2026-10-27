import copy
import unittest

from restore_missing_sdk import prepare, verify


class MissingSDKTest(unittest.TestCase):
    def setUp(self):
        self.event = {'uuid': '12345678-1234-4234-9234-123456789abc', 'event': '$screen',
                      'distinct_id': 'device', 'timestamp': '2026-10-10T13:00:00+00:00',
                      'properties': {'$os': 'source-os', '$session_id': 'session',
                                     '$set': {'must_not_apply': 'historic'}, 'custom': [True, 1, None]}}
        self.source = {'id': 'native', 'distinct_ids': ['device'], 'is_identified': False,
                       'properties': {'$os': 'source-os', '$app_build': 174, 'custom_person': 'historic'}}
        self.before = {'id': 'cloud', 'distinct_ids': ['device'], 'is_identified': False,
                       'created_at': '2026-10-10T12:00:00+00:00',
                       'properties': {'$app_build': 178, 'custom_person': 'current'}}

    def valid(self):
        replay = prepare(self.event, self.source, self.before)
        after = copy.deepcopy(self.before)
        after['properties']['$os'] = 'source-os'
        props = copy.deepcopy(replay['properties'])
        props.pop('$process_person_profile')  # Actual Cloud fixture behavior.
        record = [replay['uuid'], replay['event'], replay['distinct_id'], replay['timestamp'], props]
        return after, record

    def test_original_uuid_and_source_custom_session_values_preserved_newer_profile_kept(self):
        replay = prepare(self.event, self.source, self.before)
        self.assertEqual(replay['uuid'], self.event['uuid'])
        self.assertEqual(replay['properties']['$set_once'], {'$os': 'source-os'})
        self.assertNotIn('$os', replay['properties'])
        self.assertNotIn('$set', replay['properties'])
        after, record = self.valid()
        result = verify(self.event, self.source, self.before, after, record, 1)
        self.assertTrue(result['verified'])
        self.assertEqual(after['properties']['$app_build'], 178)

    def test_identity_group_state_creation_and_empty_repairs_fail_closed(self):
        for change in ('identify', '$set', '$unset', '$snapshot', 'group', 'alias', 'state', 'created', 'complete'):
            event, source, before = copy.deepcopy((self.event, self.source, self.before))
            if change == 'identify': event['event'] = '$identify'
            if change in ('$set', '$unset', '$snapshot'): event['event'] = change
            if change == 'group': event['properties']['$group_set'] = {'bad': 1}
            if change == 'alias': before['distinct_ids'].append('unrelated')
            if change == 'state': before['is_identified'] = True
            if change == 'created': before['created_at'] = '2026-10-11T00:00:00+00:00'
            if change == 'complete': before['properties']['$os'] = 'current'
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                prepare(event, source, before)

    def test_unbound_or_changed_stored_data_and_profile_side_effects_are_rejected(self):
        for change in ('uuid', 'set', 'unset', 'map', 'sdk', 'session', 'original', 'profile', 'identified', 'membership', 'created', 'duplicates'):
            after, record = self.valid()
            count = 1
            if change == 'uuid': record[0] = 'wrong'
            if change == 'set': record[4]['$set'] = {'arbitrary': 'write'}
            if change == 'unset': record[4]['$unset'] = ['current']
            if change == 'map': record[4]['$set_once']['$os'] = 'wrong'
            if change == 'sdk': record[4]['$os'] = 'unsafe-auto-profile'
            if change == 'session': record[4]['$session_id'] = 'wrong'
            if change == 'original': record[4]['__kasanova_posthog_original_event'] = '{}'
            if change == 'profile': after['properties']['$app_build'] = 174
            if change == 'identified': after['is_identified'] = True
            if change == 'membership': after['distinct_ids'].append('unrelated')
            if change == 'created': after['created_at'] = '2026-10-01T00:00:00+00:00'
            if change == 'duplicates': count = 2
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                verify(self.event, self.source, self.before, after, record, count)

    def test_processing_control_requires_absence_or_literal_true(self):
        for value in (False, 0, 1, 'false', None):
            after, record = self.valid()
            record[4]['$process_person_profile'] = value
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                verify(self.event, self.source, self.before, after, record, 1)
        after, record = self.valid()
        record[4]['$process_person_profile'] = True
        self.assertTrue(verify(self.event, self.source, self.before, after, record, 1)['verified'])
