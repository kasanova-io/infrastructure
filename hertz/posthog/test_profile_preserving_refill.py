"""Regression coverage for safe reconciliation of acknowledged historical events."""
from contextlib import ExitStack, closing
import copy
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import daily_refill as refill


class ProfilePreservingRefillTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = {
            'cloud_credentials': 'fixture-only',
            'hertz_projects': {'prod': {'id': 1}, 'dev': {'id': 2}},
            'native_delta_floor': '2020-01-01T00:00:00+00:00',
        }
        self.state = {'native_cutoff': '2026-10-12T00:00:00+00:00'}
        self.native = {
            'uuid': '00000000-0000-4000-8000-000000000001',
            'event': 'screen_viewed', 'distinct_id': 'wallet-device-id',
            'timestamp': '2026-10-10T18:00:00.123456+00:00',
            'properties': {'custom': {'typed': [True, 1, None, 0.10000000000000002]}},
        }
        self.person = {
            'id': 'cloud-person', 'distinct_ids': ['wallet-device-id', 'device-alias'],
            'is_identified': True,
            'created_at': '2020-01-01T00:00:00Z',
            'properties': {'version': 'current', 'nested': [True, 1, None]},
        }

    def row(self, event=None):
        event = copy.deepcopy(event or self.native)
        return {k: event[k] for k in ('uuid', 'event', 'distinct_id')} | {
            'timestamp_us': refill.time_us(event['timestamp']),
            'properties': json.dumps(event['properties']),
        }

    def record(self, event):
        return [event['event'], event['distinct_id'], event['timestamp'], copy.deepcopy(event['properties'])]

    def with_operations(self):
        event = copy.deepcopy(self.native)
        event['properties'].update({
            '$set': {'version': 'old', 'typed': [True, 1, None]},
            '$set_once': {'first': {'value': 0.10000000000000002}},
            '$unset': ['obsolete'], '$process_person_profile': True,
        })
        return event

    def journal(self, entries):
        path = self.root / 'journal.sqlite'
        with closing(sqlite3.connect(path)) as con:
            con.execute('CREATE TABLE records(project TEXT,id TEXT,native TEXT)')
            con.executemany('INSERT INTO records VALUES (?,?,?)',
                            [(str(project), uid, json.dumps(native)) for project, uid, native in entries])
            con.commit()
        return path

    def test_journal_source_projection_ignores_only_backend_enrichment(self):
        path = self.journal([(1, self.native['uuid'], self.native)])
        config = {**self.config, 'journal': str(path)}
        row = self.row()
        row['properties'] = json.dumps({**self.native['properties'], '$transformations_succeeded': ['hertz-only']})
        owned = refill.journal_envelopes(config, 1, [row])
        projected = refill.source_wire_envelope(row, config, 1, owned)
        self.assertEqual(projected, self.native)
        self.assertIn('$transformations_succeeded', json.loads(row['properties']))

    def test_journal_does_not_read_same_uuid_from_other_project(self):
        other = copy.deepcopy(self.native)
        other['properties']['custom'] = 'DEV private data'
        path = self.journal([(2, self.native['uuid'], other)])
        self.assertEqual(refill.journal_envelopes({'journal': str(path)}, 1, [self.row()]), {})
        self.assertEqual(refill.journal_envelopes({'journal': str(path)}, 2, [self.row()]),
                         {self.native['uuid']: other})

    def test_corrupt_journal_native_uuid_fails_before_projection(self):
        native = copy.deepcopy(self.native)
        native['uuid'] = '00000000-0000-4000-8000-000000000099'
        path = self.journal([(1, self.native['uuid'], native)])
        with self.assertRaisesRegex(RuntimeError, 'UUID'):
            refill.journal_envelopes({'journal': str(path)}, 1, [self.row()])

    def test_source_projection_rejects_changed_headers_or_typed_custom_fields(self):
        variants = []
        for key, value in [('event', 'changed'), ('distinct_id', 'other-device'),
                           ('timestamp_us', refill.time_us(self.native['timestamp']) + 1)]:
            row = self.row(); row[key] = value; variants.append(row)
        for value in ([1, 1, None, 0.10000000000000002],
                      [True, 1, None, 0.10000000000000003], {'typed': []}):
            row = self.row(); row['properties'] = json.dumps({'custom': {'typed': value}})
            variants.append(row)
        row = self.row(); row['properties'] = '{}'; variants.append(row)
        for row in variants:
            with self.subTest(row=row), self.assertRaisesRegex(RuntimeError, 'journal payload'):
                refill.source_wire_envelope(row, self.config, 1, {self.native['uuid']: self.native})

    def test_faithful_original_history_is_not_replayed_for_backend_flags(self):
        path = self.journal([(1, self.native['uuid'], self.native)])
        row = self.row()
        row['properties'] = json.dumps({**self.native['properties'], '$transformations_succeeded': ['hertz-only']})
        client = MagicMock()
        with patch.object(refill, 'CloudClient', return_value=client), \
             patch.object(refill, 'source_query', return_value=[row]), \
             patch.object(refill, 'cloud_readback', return_value={row['uuid']: self.record(self.native)}) as readback:
            result = refill.cloud_gap({**self.config, 'journal': str(path)}, self.state, 'prod', self.root, dt.datetime.now(refill.UTC))
        client.capture.assert_not_called()
        self.assertEqual(readback.call_count, 1)
        self.assertEqual(result['source_events_checked'], 1)
        self.assertEqual(result['existing_versions_repaired_and_verified'], 0)

    def test_safe_replay_keeps_sealed_original_and_disables_person_operations(self):
        native = self.with_operations()
        before = copy.deepcopy(native)
        replay = refill.profile_replay(native)
        self.assertEqual(native, before)
        self.assertEqual(json.loads(replay['properties'][refill.ORIGINAL]), native)
        self.assertEqual(hashlib.sha256(replay['properties'][refill.ORIGINAL].encode()).hexdigest(),
                         replay['properties'][refill.ORIGINAL_SHA])
        for key in refill.PERSON_OPERATIONS:
            self.assertNotIn(key, replay['properties'])
        self.assertIs(replay['properties']['$process_person_profile'], False)
        self.assertIs(replay['properties'][refill.PROFILE_REPLAY], True)
        for key in ('uuid', 'event', 'distinct_id', 'timestamp'):
            self.assertEqual(replay[key], native[key])
        self.assertEqual(replay['properties']['custom'], native['properties']['custom'])
        self.assertTrue(refill.cloud_fields_match(native, self.record(replay)))

    def test_safe_replay_match_requires_checksum_and_unchanged_original_headers(self):
        native = self.with_operations()
        for key, value in [('uuid', 'another-uuid'), ('event', 'other-event'),
                           ('distinct_id', 'another-device'), ('timestamp', '2026-10-10T18:00:00.123457Z')]:
            with self.subTest(key=key):
                replay = refill.profile_replay(native)
                original = json.loads(replay['properties'][refill.ORIGINAL]); original[key] = value
                raw = refill.canonical(original)
                replay['properties'][refill.ORIGINAL] = raw
                replay['properties'][refill.ORIGINAL_SHA] = hashlib.sha256(raw.encode()).hexdigest()
                self.assertFalse(refill.cloud_fields_match(native, self.record(replay)))
        replay = refill.profile_replay(native)
        replay['properties'][refill.ORIGINAL_SHA] = '0' * 64
        self.assertFalse(refill.cloud_fields_match(native, self.record(replay)))

    def test_safe_replay_match_rejects_typed_operation_corruption_with_valid_checksum(self):
        native = self.with_operations()
        for key in refill.PERSON_OPERATIONS + ('$process_person_profile',):
            with self.subTest(key=key):
                replay = refill.profile_replay(native)
                original = json.loads(replay['properties'][refill.ORIGINAL])
                original['properties'][key] = False if key == '$process_person_profile' else []
                raw = refill.canonical(original)
                replay['properties'][refill.ORIGINAL] = raw
                replay['properties'][refill.ORIGINAL_SHA] = hashlib.sha256(raw.encode()).hexdigest()
                self.assertFalse(refill.cloud_fields_match(native, self.record(replay)))

    def test_safe_replay_match_still_checks_direct_custom_properties(self):
        native = self.with_operations()
        replay = refill.profile_replay(native)
        replay['properties']['custom']['typed'][0] = 1
        self.assertFalse(refill.cloud_fields_match(native, self.record(replay)))

    def test_safe_replay_match_rejects_reenabled_or_reintroduced_person_operations(self):
        native = self.with_operations()
        variants = [{'$process_person_profile': True}, {'$set': {'unexpected': 'write'}},
                    {'$set_once': {'unexpected': 'write'}}, {'$unset': ['unexpected']}]
        for extra in variants:
            with self.subTest(extra=extra):
                replay = refill.profile_replay(native); replay['properties'].update(extra)
                self.assertFalse(refill.cloud_fields_match(native, self.record(replay)))

    def test_safe_replay_match_rejects_malformed_archives_without_certifying(self):
        native = self.with_operations()
        originals = [None, [], 'not-json', {'properties': {}},
                     {**native, 'timestamp': 'not-a-timestamp'},
                     {k: v for k, v in native.items() if k != 'timestamp'}]
        for original in originals:
            with self.subTest(original=original):
                replay = refill.profile_replay(native)
                raw = original if original == 'not-json' else json.dumps(original)
                replay['properties'][refill.ORIGINAL] = raw
                replay['properties'][refill.ORIGINAL_SHA] = hashlib.sha256(raw.encode()).hexdigest()
                self.assertFalse(refill.cloud_fields_match(native, self.record(replay)))

    def test_safe_replay_match_rejects_changed_direct_event_headers(self):
        native = self.with_operations()
        for index, value in [(0, 'changed-event'), (1, 'other-wallet'),
                             (2, '2026-10-10T18:00:00.123457Z')]:
            with self.subTest(index=index):
                record = self.record(refill.profile_replay(native)); record[index] = value
                self.assertFalse(refill.cloud_fields_match(native, record))

    def test_preflight_requires_existing_matching_memberships_and_established_person(self):
        variants = [[], [{**self.person, 'distinct_ids': ['wallet-device-id']}],
                    [{**self.person, 'created_at': dt.datetime.now(refill.UTC).isoformat()}]]
        for people in variants:
            with self.subTest(people=people), patch.object(refill, 'HertzClient'), \
                 patch.object(refill, 'source_people', return_value=[self.person]), \
                 patch.object(refill, 'cloud_persons', return_value=people), self.assertRaises(RuntimeError):
                refill.profile_replay_preflight(MagicMock(), 1, [self.native], self.root)
        with patch.object(refill, 'HertzClient'), patch.object(refill, 'source_people', return_value=[]), \
             patch.object(refill, 'cloud_persons', return_value=[self.person]), self.assertRaises(RuntimeError):
            refill.profile_replay_preflight(MagicMock(), 1, [self.native], self.root)

    def test_preserved_profile_verification_rejects_typed_property_membership_and_age_drift(self):
        before = {self.person['id']: self.person}
        variants = [[], [{**self.person, 'distinct_ids': ['wallet-device-id']}],
                    [{**self.person, 'created_at': '2020-01-02T00:00:00Z'}],
                    [{**self.person, 'is_identified': False}]]
        changed = copy.deepcopy(self.person); changed['properties']['nested'][0] = 1
        variants.append([changed])
        changed = copy.deepcopy(self.person); changed['properties']['extra'] = 'new'
        variants.append([changed])
        for after in variants:
            with self.subTest(after=after), self.assertRaisesRegex(RuntimeError, 'profile changed'):
                refill.verify_preserved_profiles(before, after)
        refill.verify_preserved_profiles(before, [copy.deepcopy(self.person)])

    def gap_stack(self, row, client, initial=None, people=None):
        stack = ExitStack()
        stack.enter_context(patch.object(refill, 'CloudClient', return_value=client))
        stack.enter_context(patch.object(refill, 'source_query', return_value=[row]))
        stack.enter_context(patch.object(refill, 'HertzClient'))
        stack.enter_context(patch.object(refill, 'source_people', return_value=people or [self.person]))
        stack.enter_context(patch.object(refill, 'cloud_persons', return_value=[self.person]))
        stored = dict(initial or {})
        def query(sql):
            # Exercise cloud_readback's real stable-latest header/body/header
            # protocol before and after capture rather than bypassing readback.
            created = '2026-10-15 01:00:00.123456'
            if 'groupUniqArray(person_id)' in sql:
                return [[uid, [self.person['id']]] for uid in stored]
            if 'timestamp,properties' in sql:
                return [[uid, copy.deepcopy(record) + [created]] for uid, record in stored.items()]
            return [[uid, copy.deepcopy(record[:3]) + [created]] for uid, record in stored.items()]
        client.query.side_effect = query
        def capture(events, **kwargs):
            for event in events:
                stored[event['uuid']] = self.record(event)
        client.capture.side_effect = capture
        return stack

    def test_full_missing_person_operation_event_replay_verifies_both_environments_without_profile_changes(self):
        for environment in ('prod', 'dev'):
            with self.subTest(environment=environment):
                native = self.with_operations(); row = self.row(native); client = MagicMock()
                with self.gap_stack(row, client), patch.object(refill, 'cloud_persons', return_value=[copy.deepcopy(self.person)]) as profiles:
                    result = refill.cloud_gap(self.config, self.state, environment, self.root / environment, dt.datetime.now(refill.UTC))
                captured = client.capture.call_args.args[0]
                self.assertEqual(len(captured), 1)
                self.assertEqual(captured[0]['uuid'], native['uuid'])
                self.assertTrue(refill.cloud_fields_match(native, self.record(captured[0])))
                self.assertEqual(result['missing_events_captured_and_verified'], 1)
                self.assertEqual(result['profile_preserving_events_verified'], 1)
                self.assertEqual(client.query.call_count, 7)
                self.assertGreaterEqual(profiles.call_count, 3)
                receipt = json.loads((self.root / environment / 'reconciliation-000000.json').read_text())
                self.assertEqual(receipt['profile_preserving_events_verified'], 1)

    def test_stale_existing_person_update_fails_before_capture(self):
        native = self.with_operations(); row = self.row(native); client = MagicMock()
        wrong = self.record(native); wrong[3]['custom'] = 'wrong'
        with self.gap_stack(row, client, {row['uuid']: wrong}), self.assertRaisesRegex(RuntimeError, 'Stale native person'):
            refill.cloud_gap(self.config, self.state, 'prod', self.root, dt.datetime.now(refill.UTC))
        client.capture.assert_not_called()

    def test_missing_person_operation_preflight_rejects_before_capture(self):
        native = self.with_operations()
        variants = [([], [self.person]), ([self.person], []),
                    ([self.person], [{**self.person, 'distinct_ids': ['wallet-device-id']}]),
                    ([self.person], [{**self.person, 'created_at': dt.datetime.now(refill.UTC).isoformat()}])]
        for source_people, cloud_people in variants:
            with self.subTest(source_people=source_people, cloud_people=cloud_people):
                client = MagicMock()
                with self.gap_stack(self.row(native), client), \
                     patch.object(refill, 'source_people', return_value=source_people), \
                     patch.object(refill, 'cloud_persons', return_value=cloud_people), self.assertRaises(RuntimeError):
                    refill.cloud_gap(self.config, self.state, 'dev', self.root, dt.datetime.now(refill.UTC))
                client.capture.assert_not_called()

    def test_missing_identity_and_group_events_fail_before_capture(self):
        variants = []
        for name in refill.IDENTITY_EVENTS:
            if name == '$identify':
                continue  # Recorded SDK identify relations have a separate guarded path.
            event = copy.deepcopy(self.native); event['event'] = name; variants.append(event)
        for key in ('$group_set', '$group_set_once', '$group_unset', '$groups', '$group_key'):
            event = copy.deepcopy(self.native); event['properties'][key] = {'group': 'old'}; variants.append(event)
        for event in variants:
            with self.subTest(event=event['event'], properties=event['properties']):
                client = MagicMock()
                with self.gap_stack(self.row(event), client), self.assertRaisesRegex(RuntimeError, '(identity/group|group fields)'):
                    refill.cloud_gap(self.config, self.state, 'prod', self.root, dt.datetime.now(refill.UTC))
                client.capture.assert_not_called()

    def test_stale_unsupported_identity_events_fail_before_capture_without_person_operations(self):
        for name in ('$create_alias', '$merge_dangerously', '$groupidentify'):
            with self.subTest(event=name):
                event = copy.deepcopy(self.native); event['event'] = name
                wrong = self.record(event); wrong[3]['custom'] = {'changed': 'persisted value'}
                client = MagicMock()
                with self.gap_stack(self.row(event), client, {event['uuid']: wrong}), \
                     self.assertRaisesRegex(RuntimeError, 'identity/group'):
                    refill.cloud_gap(self.config, self.state, 'prod', self.root, dt.datetime.now(refill.UTC))
                client.capture.assert_not_called()
                self.assertFalse(list(self.root.glob('reconciliation-*.json')))

    def identify_event(self):
        event = self.with_operations()
        event['event'] = '$identify'
        event['properties']['$anon_distinct_id'] = 'device-alias'
        event['properties']['$session_id'] = 'source-native-session'
        event['properties'].update({'$os': 'old OS', '$app_version': 'old version',
                                    '$os_name': 'old OS name',
                                    'utm_source': 'historical campaign', 'gclid': 'historical click',
                                    '$raw_user_agent': 'historical agent',
                                    '$useragent': 'old alias agent', '$user-agent': 'old dash agent',
                                    '$user_agent': 'old underscore agent', 'segment_userAgent': 'old segment agent',
                                    '$unrecognized_custom_field': {'typed': [True, 1, None]}})
        return event

    def test_identify_archives_auto_profile_fields_but_keeps_custom_and_session_native(self):
        event = self.identify_event()
        replay = refill.profile_replay(event)
        fields = ('$os', '$os_name', '$app_version', 'utm_source', 'gclid', '$raw_user_agent',
                  '$useragent', '$user-agent', '$user_agent', 'segment_userAgent')
        for key in fields:
            self.assertNotIn(key, replay['properties'])
            self.assertEqual(json.loads(replay['properties'][refill.ORIGINAL])['properties'][key],
                             event['properties'][key])
        for key in ('$session_id', '$anon_distinct_id', '$unrecognized_custom_field', 'custom'):
            self.assertEqual(replay['properties'][key], event['properties'][key])
        self.assertTrue(refill.cloud_fields_match(event, self.record(replay)))
        for key in fields:
            with self.subTest(key=key):
                unsafe = copy.deepcopy(replay); unsafe['properties'][key] = event['properties'][key]
                self.assertFalse(refill.cloud_fields_match(event, self.record(unsafe)))

    def test_identify_archived_sdk_fields_still_require_exact_source_values_and_controls(self):
        event = self.identify_event()
        for key in ('$os', '$os_name', '$app_version', 'utm_source', 'gclid', '$raw_user_agent',
                    '$useragent', '$user-agent', '$user_agent', 'segment_userAgent'):
            with self.subTest(key=key):
                replay = refill.profile_replay(event)
                original = json.loads(replay['properties'][refill.ORIGINAL])
                original['properties'][key] = {'changed': [True, 1]}
                raw = refill.canonical(original)
                replay['properties'][refill.ORIGINAL] = raw
                replay['properties'][refill.ORIGINAL_SHA] = hashlib.sha256(raw.encode()).hexdigest()
                self.assertFalse(refill.cloud_fields_match(event, self.record(replay)))
        for key, value in [('$process_person_profile', False), ('$geoip_disable', False),
                           ('$update_person_last_seen_at', True)]:
            with self.subTest(key=key):
                replay = refill.profile_replay(event); replay['properties'][key] = value
                self.assertFalse(refill.cloud_fields_match(event, self.record(replay)))

    def split_people(self):
        target = copy.deepcopy(self.person)
        target['distinct_ids'] = ['wallet-device-id']
        target['created_at'] = '2021-01-01T00:00:00Z'
        anonymous = {'id': 'anonymous-cloud-person', 'distinct_ids': ['device-alias'],
                     'created_at': '2020-01-01T00:00:00Z', 'is_identified': False,
                     'properties': {'anonymous_only': {'typed': [True, None, 1]}}}
        merged = copy.deepcopy(self.person)
        merged['properties'] = {**anonymous['properties'], **target['properties']}
        return target, anonymous, merged

    def test_recorded_identify_already_grouped_replays_without_historical_profile_writes(self):
        event = self.identify_event()
        for environment in ('prod', 'dev'):
            with self.subTest(environment=environment):
                client = MagicMock()
                with self.gap_stack(self.row(event), client):
                    result = refill.cloud_gap(self.config, self.state, environment,
                                              self.root / environment, dt.datetime.now(refill.UTC))
                captured = client.capture.call_args.args[0][0]
                self.assertEqual(captured['properties'][refill.PROFILE_REPLAY], 'identify')
                self.assertIs(captured['properties']['$process_person_profile'], True)
                self.assertIs(captured['properties']['$geoip_disable'], True)
                self.assertIs(captured['properties']['$update_person_last_seen_at'], False)
                for key in refill.PERSON_OPERATIONS:
                    self.assertNotIn(key, captured['properties'])
                for key in ('$anon_distinct_id', '$session_id', 'custom'):
                    self.assertEqual(captured['properties'][key], event['properties'][key])
                self.assertEqual(json.loads(captured['properties'][refill.ORIGINAL]), event)
                self.assertTrue(refill.cloud_fields_match(event, self.record(captured)))
                self.assertEqual(result['profile_preserving_events_verified'], 1)

    def test_recorded_identify_split_restores_exact_source_relation_and_verified_current_profiles(self):
        event = self.identify_event(); target, anonymous, merged = self.split_people()
        client = MagicMock()
        with self.gap_stack(self.row(event), client), \
             patch.object(refill, 'cloud_persons', side_effect=[[target, anonymous], [merged], [merged]]):
            result = refill.cloud_gap(self.config, self.state, 'prod', self.root, dt.datetime.now(refill.UTC))
        self.assertEqual(result['missing_events_captured_and_verified'], 1)
        self.assertEqual(result['profile_preserving_events_verified'], 1)
        plans = list(self.root.glob('identity-replay-plan-*-private.json'))
        self.assertEqual(len(plans), 1)
        plan = json.loads(plans[0].read_text())[0]
        self.assertEqual(plan['expected']['id'], target['id'])
        self.assertEqual(set(plan['expected']['distinct_ids']), set(self.person['distinct_ids']))
        self.assertEqual(plan['expected']['properties'], merged['properties'])
        self.assertEqual(plan['expected']['created_at'], anonymous['created_at'])
        self.assertIs(plan['expected']['is_identified'], True)

    def test_identify_rejects_unknown_identified_conflicting_or_outsider_alias_before_capture(self):
        event = self.identify_event(); target, anonymous, _ = self.split_people()
        identified = {**anonymous, 'is_identified': True}
        conflicting = copy.deepcopy(anonymous); conflicting['properties']['nested'] = [1, 1, None]
        outsider = {**anonymous, 'distinct_ids': ['device-alias', 'unrecorded-outsider']}
        unknown = copy.deepcopy(event); unknown['properties']['$anon_distinct_id'] = 'unrecorded-anonymous-id'
        variants = [(unknown, [target]), (event, [target, identified]),
                    (event, [target, conflicting]), (event, [target, outsider])]
        older = copy.deepcopy(event); older['timestamp'] = '2019-01-01T00:00:00Z'
        variants.append((older, [target, anonymous]))
        for native, people in variants:
            with self.subTest(event=native, people=people):
                client = MagicMock()
                with self.gap_stack(self.row(native), client), \
                     patch.object(refill, 'cloud_persons', return_value=people), self.assertRaises(RuntimeError):
                    refill.cloud_gap(self.config, self.state, 'dev', self.root, dt.datetime.now(refill.UTC))
                client.capture.assert_not_called()

    def test_recorded_identify_attaches_unclaimed_source_alias_preserving_target_profile_and_retry(self):
        event = self.identify_event(); target, _, _ = self.split_people()
        target['is_identified'] = False  # Exact live shape: target exists, source alias has no Cloud owner.
        expected = copy.deepcopy(target)
        expected['distinct_ids'] = sorted(self.person['distinct_ids'])
        expected['is_identified'] = True
        for environment in ('prod', 'dev'):
            with self.subTest(environment=environment):
                client = MagicMock(); evidence = self.root / environment
                with self.gap_stack(self.row(event), client), \
                     patch.object(refill, 'cloud_persons', side_effect=[[target], [expected], [expected]]):
                    result = refill.cloud_gap(self.config, self.state, environment, evidence, dt.datetime.now(refill.UTC))
                self.assertEqual(result['missing_events_captured_and_verified'], 1)
                self.assertEqual(result['profile_preserving_events_verified'], 1)
                self.assertEqual(client.query.call_count, 7)
                captured = client.capture.call_args.args[0][0]
                self.assertTrue(refill.cloud_fields_match(event, self.record(captured)))
                plan = json.loads(next(evidence.glob('identity-replay-plan-*-private.json')).read_text())[0]
                self.assertEqual(plan['expected']['id'], target['id'])
                self.assertEqual(plan['expected']['properties'], target['properties'])
                self.assertEqual(plan['expected']['created_at'], target['created_at'])
                self.assertEqual(set(plan['expected']['distinct_ids']), set(self.person['distinct_ids']))
                self.assertIs(plan['expected']['is_identified'], True)
                retry = MagicMock()
                with self.gap_stack(self.row(event), retry, {event['uuid']: self.record(captured)}), \
                     patch.object(refill, 'cloud_persons', return_value=[expected]):
                    retried = refill.cloud_gap(self.config, self.state, environment, evidence / 'retry', dt.datetime.now(refill.UTC))
                retry.capture.assert_not_called()
                self.assertEqual(retry.query.call_count, 4)
                self.assertIn('groupUniqArray(person_id)', retry.query.call_args.args[0])
                self.assertEqual(retried['missing_events_captured_and_verified'], 0)

    def test_unclaimed_alias_requires_exact_recorded_source_group_and_target_predating_event(self):
        event = self.identify_event(); target, _, _ = self.split_people()
        unknown = copy.deepcopy(event); unknown['properties']['$anon_distinct_id'] = 'unrecorded-anonymous-id'
        outsider = copy.deepcopy(target); outsider['distinct_ids'].append('unrecorded-cloud-outsider')
        older = copy.deepcopy(event); older['timestamp'] = '2020-01-01T00:00:00Z'
        for native, person in [(unknown, target), (event, outsider), (older, target)]:
            with self.subTest(event=native, person=person):
                client = MagicMock()
                with self.gap_stack(self.row(native), client), \
                     patch.object(refill, 'cloud_persons', return_value=[person]), self.assertRaises(RuntimeError):
                    refill.cloud_gap(self.config, self.state, 'dev', self.root, dt.datetime.now(refill.UTC))
                client.capture.assert_not_called()

    def test_wrong_or_ambiguous_canonical_event_person_never_certifies(self):
        event = self.with_operations()
        for people in ([], ['wrong-person'], [self.person['id'], 'wrong-person']):
            with self.subTest(people=people):
                client = MagicMock()
                with self.gap_stack(self.row(event), client), patch.object(refill.time, 'sleep'):
                    original_query = client.query.side_effect
                    def query(sql):
                        if 'groupUniqArray(person_id)' in sql:
                            return [[event['uuid'], people]]
                        return original_query(sql)
                    client.query.side_effect = query
                    with self.assertRaisesRegex(RuntimeError, 'canonical person'):
                        refill.cloud_gap(self.config, self.state, 'dev', self.root, dt.datetime.now(refill.UTC))
                self.assertEqual(client.query.call_count, 36)
                self.assertFalse(list(self.root.glob('reconciliation-*.json')))
                self.assertFalse(list(self.root.glob('event-person-verification-*.json')))

    def test_identify_requires_recorded_identified_source_and_valid_alias_type(self):
        event = self.identify_event()
        variants = [(event, {**self.person, 'is_identified': False})]
        for invalid in ('', 123, ['device-alias']):
            native = copy.deepcopy(event); native['properties']['$anon_distinct_id'] = invalid
            variants.append((native, self.person))
        for native, source_person in variants:
            with self.subTest(source=source_person, event=native):
                client = MagicMock()
                with self.gap_stack(self.row(native), client), \
                     patch.object(refill, 'source_people', return_value=[source_person]), \
                     self.assertRaisesRegex(RuntimeError, 'invalid'):
                    refill.cloud_gap(self.config, self.state, 'dev', self.root, dt.datetime.now(refill.UTC))
                client.capture.assert_not_called()

    def test_failed_profile_preservation_keeps_both_watermarks_and_identity_unchanged(self):
        runtime = self.root / 'runtime'; (runtime / 'operations').mkdir(parents=True)
        state = {**self.state, 'amplitude_project_id': 734469,
                 'amplitude_upload_cutoff': '2026-10-12T00:00:00+00:00'}
        state_path = self.root / 'state.json'; state_path.write_text(json.dumps(state))
        identity = self.root / 'identity.json'; identity.write_text('{"canonical_map":{"existing":"existing"}}')
        before = (state_path.read_bytes(), identity.read_bytes())
        config = {**self.config, 'runtime': str(runtime), 'state': str(state_path),
                  'identity_map': str(identity), 'amplitude': {'project_id': 734469},
                  'hertz_ingestion_host': 'http://127.0.0.1:18084'}
        config_path = self.root / 'config.json'; config_path.write_text(json.dumps(config))
        native = self.with_operations(); client = MagicMock()
        changed = copy.deepcopy(self.person); changed['properties']['version'] = 'historically overwritten'
        with self.gap_stack(self.row(native), client), \
             patch.object(refill, 'cloud_persons', side_effect=[[self.person], [changed]]), \
             patch.object(refill, 'export_window', return_value=(object(), dt.datetime.now(refill.UTC))), \
             patch.object(refill, 'refill_amplitude', return_value=(dt.datetime.now(refill.UTC), 1, {'wrong': 'not committed'})), \
             self.assertRaisesRegex(RuntimeError, 'profile changed'):
            refill.run(config_path)
        self.assertEqual((state_path.read_bytes(), identity.read_bytes()), before)
        self.assertFalse(list(runtime.glob('operations/refill/*/success.json')))

    def test_stored_identify_retry_rechecks_completed_membership_and_canonical_person_without_capture(self):
        event = self.identify_event(); replay = refill.profile_replay(event)
        for environment in ('prod', 'dev'):
            with self.subTest(environment=environment):
                client = MagicMock(); evidence = self.root / environment
                with self.gap_stack(self.row(event), client, {event['uuid']: self.record(replay)}), \
                     patch.object(refill, 'cloud_persons', return_value=[copy.deepcopy(self.person)]) as people:
                    result = refill.cloud_gap(self.config, self.state, environment, evidence, dt.datetime.now(refill.UTC))
                client.capture.assert_not_called()
                self.assertEqual(client.query.call_count, 4)
                self.assertIn('groupUniqArray(person_id)', client.query.call_args.args[0])
                self.assertEqual(people.call_count, 2)  # Retry membership plus final protected-profile check.
                self.assertEqual(result['missing_events_captured_and_verified'], 0)
                self.assertEqual(result['source_events_checked'], 1)
                self.assertTrue(list(evidence.glob('event-person-verification-*.json')))
                self.assertTrue((evidence / 'profile-reconciliation.json').is_file())

    def test_stored_identify_with_unrestored_split_keeps_run_watermarks_and_identity_unchanged(self):
        runtime = self.root / 'runtime'; (runtime / 'operations').mkdir(parents=True)
        state = {**self.state, 'amplitude_project_id': 734469,
                 'amplitude_upload_cutoff': '2026-10-12T00:00:00+00:00'}
        state_path = self.root / 'state.json'; state_path.write_text(json.dumps(state))
        identity = self.root / 'identity.json'; identity.write_text('{"canonical_map":{"existing":"existing"}}')
        before = (state_path.read_bytes(), identity.read_bytes())
        config = {**self.config, 'runtime': str(runtime), 'state': str(state_path),
                  'identity_map': str(identity), 'amplitude': {'project_id': 734469},
                  'hertz_ingestion_host': 'http://127.0.0.1:18084'}
        config_path = self.root / 'config.json'; config_path.write_text(json.dumps(config))
        event = self.identify_event(); replay = refill.profile_replay(event)
        target, anonymous, _ = self.split_people(); client = MagicMock()
        with self.gap_stack(self.row(event), client, {event['uuid']: self.record(replay)}), \
             patch.object(refill, 'cloud_persons', return_value=[target, anonymous]), \
             patch.object(refill, 'export_window', return_value=(object(), dt.datetime.now(refill.UTC))), \
             patch.object(refill, 'refill_amplitude', return_value=(dt.datetime.now(refill.UTC), 0, {'wrong': 'not committed'})), \
             self.assertRaisesRegex(RuntimeError, 'Persisted replay identity membership'):
            refill.run(config_path)
        client.capture.assert_not_called()
        self.assertEqual((state_path.read_bytes(), identity.read_bytes()), before)
        self.assertFalse(list(runtime.glob('operations/refill/*/success.json')))
        self.assertFalse(list(runtime.glob('operations/refill/*/*/reconciliation-*.json')))

    def test_stored_identify_retry_requires_identification_state_even_with_matching_event_fields(self):
        event = self.identify_event(); replay = refill.profile_replay(event); client = MagicMock()
        with self.gap_stack(self.row(event), client, {event['uuid']: self.record(replay)}), \
             patch.object(refill, 'cloud_persons', return_value=[{**self.person, 'is_identified': False}]), \
             self.assertRaisesRegex(RuntimeError, 'identification state'):
            refill.cloud_gap(self.config, self.state, 'prod', self.root, dt.datetime.now(refill.UTC))
        client.capture.assert_not_called()
        self.assertFalse(list(self.root.glob('reconciliation-*.json')))

    def test_stored_ordinary_replay_retry_still_rejects_wrong_canonical_person(self):
        event = self.with_operations(); replay = refill.profile_replay(event); client = MagicMock()
        with self.gap_stack(self.row(event), client, {event['uuid']: self.record(replay)}), \
             patch.object(refill.time, 'sleep'):
            original_query = client.query.side_effect
            def query(sql):
                if 'groupUniqArray(person_id)' in sql:
                    return [[event['uuid'], ['wrong-person']]]
                return original_query(sql)
            client.query.side_effect = query
            with self.assertRaisesRegex(RuntimeError, 'canonical person'):
                refill.cloud_gap(self.config, self.state, 'dev', self.root, dt.datetime.now(refill.UTC))
        client.capture.assert_not_called()
        self.assertEqual(client.query.call_count, 33)
        self.assertFalse(list(self.root.glob('reconciliation-*.json')))
        self.assertFalse(list(self.root.glob('event-person-verification-*.json')))


if __name__ == '__main__':
    unittest.main()
