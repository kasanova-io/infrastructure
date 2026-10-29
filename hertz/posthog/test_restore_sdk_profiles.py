import copy
import unittest

from restore_sdk_profiles import prepare, verify, verify_ordinary_events


class SDKProfileMaintenanceTest(unittest.TestCase):
    def setUp(self):
        self.event = {'uuid': '12345678-1234-4234-9234-123456789abc', 'event': 'wallet_opened',
                      'distinct_id': 'device', 'timestamp': '2026-10-10T13:00:00+00:00',
                      'properties': {'$session_id': 'original-session', 'custom': [True, 1, None]}}
        self.source = {'id': 'source', 'distinct_ids': ['device'], 'is_identified': False,
                       'properties': {'$os': 'source-os', '$app_build': 174, 'custom': 'old'}}
        self.before = {'id': 'cloud', 'distinct_ids': ['device'], 'is_identified': False,
                       'created_at': '2026-10-10T12:00:00+00:00',
                       'last_seen_at': '2026-10-10T13:00:00+00:00',
                       'properties': {'$app_build': 178, 'custom': 'current'}}
        self.context = {'environment': 'dev', 'source_project_id': 2,
                        'cloud_project_id': 123, 'source_manifest_sha256': 'a' * 64}
        self.stamp = '2026-10-29T00:00:00+00:00'

    def prepared(self):
        return prepare(self.event, self.source, self.before, self.context, self.stamp)

    def valid(self):
        audit = self.prepared()
        after = copy.deepcopy(self.before)
        after['properties']['$os'] = 'source-os'
        props = copy.deepcopy(audit['properties'])
        props.pop('$process_person_profile')
        return after, [audit['uuid'], audit['event'], audit['distinct_id'], audit['timestamp'], props]

    def test_new_maintenance_event_keeps_source_immutable_and_only_fills_missing_values(self):
        original = copy.deepcopy((self.event, self.source, self.before))
        audit = self.prepared()
        self.assertEqual(audit, self.prepared())
        self.assertNotEqual(audit['uuid'], self.event['uuid'])
        self.assertEqual(audit['event'], '$set')
        self.assertEqual(audit['properties']['$set_once'], {'$os': 'source-os'})
        self.assertNotIn('$session_id', audit['properties'])
        self.assertNotIn('$set', audit['properties'])
        self.assertEqual(original, (self.event, self.source, self.before))
        after, stored = self.valid()
        self.assertTrue(verify(self.event, self.source, self.before, self.context, self.stamp, after, stored, 1)['verified'])

    def test_source_binding_and_context_cannot_be_silently_changed(self):
        audit = self.prepared()
        context = {**self.context, 'cloud_project_id': 456}
        self.assertNotEqual(audit['uuid'], prepare(self.event, self.source, self.before, context, self.stamp)['uuid'])
        for context in ({}, {**self.context, 'cloud_project_id': True},
                        {**self.context, 'source_manifest_sha256': 'invalid'}):
            with self.assertRaises(RuntimeError):
                prepare(self.event, self.source, self.before, context, self.stamp)
        with self.assertRaisesRegex(RuntimeError, 'millisecond precision'):
            prepare(self.event, self.source, self.before, self.context, '2026-10-29T00:00:00.000072+00:00')

    def test_profile_side_effects_wrong_payload_and_audit_headers_are_rejected(self):
        for change in ('overwrite', 'membership', 'identified', 'created', 'last_seen', 'uuid', 'ordinary', 'set', 'session', 'map', 'control'):
            after, stored = self.valid()
            if change == 'overwrite': after['properties']['custom'] = 'old'
            if change == 'membership': after['distinct_ids'].append('other')
            if change == 'identified': after['is_identified'] = True
            if change == 'created': after['created_at'] = self.stamp
            if change == 'last_seen': after['last_seen_at'] = self.stamp
            if change == 'uuid': stored[0] = self.event['uuid']
            if change == 'ordinary': stored[1] = self.event['event']
            if change == 'set': stored[4]['$set'] = {'bad': 'write'}
            if change == 'session': stored[4]['$session_id'] = 'fabricated'
            if change == 'map': stored[4]['$set_once']['$os'] = 'wrong'
            if change == 'control': stored[4]['$process_person_profile'] = 1
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                verify(self.event, self.source, self.before, self.context, self.stamp, after, stored, 1)

    def test_ordinary_event_count_checks_do_not_hide_duplicates_or_property_changes(self):
        uid = self.event['uuid']
        before = {uid: [uid, ['wallet_opened', 'device', self.event['timestamp'], {'exact': [True, 1, None]}], 1]}
        self.assertEqual(verify_ordinary_events(before, copy.deepcopy(before))['unfiltered_count_each'], 1)
        for change in ('count', 'property', 'header', 'coverage'):
            after = copy.deepcopy(before)
            if change == 'count': after[uid][2] = 2
            if change == 'property': after[uid][1][3]['exact'][0] = 1
            if change == 'header': after[uid][1][0] = 'changed'
            if change == 'coverage': after = {}
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                verify_ordinary_events(before, after)

    def test_admin_retry_rows_are_disclosed_and_never_ordinary_count_one_proof(self):
        after, stored = self.valid()
        result = verify(self.event, self.source, self.before, self.context, self.stamp, after, stored, 2)
        self.assertEqual(result['audit_physical_rows'], 2)
        self.assertFalse(result['audit_rows_deduplicated'])
        self.assertTrue(result['ordinary_event_immutability_requires_separate_readback'])
