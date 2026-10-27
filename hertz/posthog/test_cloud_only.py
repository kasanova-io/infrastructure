"""Cloud retirement must preserve real journal data and isolate destinations."""
import copy
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from analytics_ingress import Journal, delivery_operations, validate_cloud_routes


class CloudOnlyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.routes = {
            key: {'project_id': project, 'project_token': 'retired-native-' + key,
                  'legacy_app': key, 'cloud': {'host': 'https://us.i.posthog.com',
                  'project_id': project + 100, 'project_token': 'cloud-' + key}}
            for key, project in [('prod', 1), ('dev', 2)]
        }
        self.path = Path(self.tmp.name) / 'journal.sqlite'
        self.journal = Journal(self.path, self.routes)
        self.journal.accept({'api_key': 'prod', 'events': [{
            'event_type': 'wallet_created', 'device_id': 'same-device',
            'user_id': 'same-wallet', 'insert_id': 'same-retry',
            'time': 1791662400123, 'event_properties': {'exact': [True, 1, None]},
        }]})

    def test_cloud_only_does_not_contact_native_or_forge_native_ack(self):
        with self.journal.connect() as con:
            before = con.execute('SELECT id,raw,native,sent FROM records').fetchall()
            identities = con.execute('SELECT * FROM identities').fetchall()
        with patch.object(self.journal, 'deliver', side_effect=AssertionError('retired destination contacted')), \
                patch('analytics_ingress.urllib.request.urlopen', return_value=io.BytesIO(b'{"status":1}')) as send:
            operations = delivery_operations(self.journal, 'http://retired.invalid', threading.Event(), True)
            self.assertEqual(sum(operation() for operation in operations), 1)
        self.assertEqual(send.call_args.args[0].full_url, 'https://us.i.posthog.com/batch/')
        with self.journal.connect() as con:
            self.assertEqual(con.execute('SELECT id,raw,native,sent FROM records').fetchall(), before)
            self.assertEqual(con.execute('SELECT * FROM identities').fetchall(), identities)
            self.assertEqual(con.execute('SELECT cloud_destination FROM records').fetchone()[0],
                             'https://us.i.posthog.com:101')
        restarted = Journal(self.path, self.routes)
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=AssertionError('duplicate capture')):
            self.assertEqual(restarted.deliver_cloud(), 0)

    def test_cloud_outage_preserves_original_pending_retry_after_restart(self):
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=OSError('offline')):
            with self.assertRaises(OSError):
                delivery_operations(self.journal, 'http://retired.invalid', threading.Event(), True)[0]()
        with sqlite3.connect(self.path) as con:
            self.assertEqual(con.execute('SELECT sent,cloud_destination FROM records').fetchone(), (0, None))
        with patch('analytics_ingress.urllib.request.urlopen', return_value=io.BytesIO(b'{"status":1}')):
            self.assertEqual(Journal(self.path, self.routes).deliver_cloud(), 1)

    def test_mirror_remains_available_for_rollback(self):
        with patch.object(self.journal, 'deliver', return_value=1) as native, \
                patch.object(self.journal, 'deliver_cloud', return_value=1) as cloud:
            operations = delivery_operations(self.journal, 'http://native', threading.Event())
            self.assertEqual(sum(operation() for operation in operations), 2)
        native.assert_called_once()
        cloud.assert_called_once()

    def test_standby_gate_is_checked_for_each_batch_and_preserves_pending_data(self):
        marker = Path(self.tmp.name) / 'cloud-delivery.enabled'
        operations = delivery_operations(self.journal, 'http://retired.invalid', threading.Event(),
                                         True, marker)
        with patch.object(self.journal, 'deliver', side_effect=AssertionError('native contacted')), \
                patch('analytics_ingress.urllib.request.urlopen', return_value=io.BytesIO(b'{"status":1}')) as send:
            self.assertEqual(operations[0](), 0)
            send.assert_not_called()
            # A directory is not an enable marker.
            marker.mkdir()
            self.assertEqual(operations[0](), 0)
            send.assert_not_called()
            marker.rmdir()
            marker.write_text('incumbent stopped\n')
            self.assertEqual(operations[0](), 1)
            marker.unlink()
            self.journal.accept({'api_key': 'dev', 'events': [{
                'event_type': 'standby', 'device_id': 'device', 'insert_id': 'after-disable',
                'time': 1791662400123,
            }]})
            self.assertEqual(operations[0](), 0)
            self.assertEqual(send.call_count, 1)
        with self.journal.connect() as con:
            self.assertEqual(con.execute('SELECT count(*) FROM records WHERE cloud_destination IS NULL').fetchone()[0], 1)

    def test_delivery_gate_rejects_mirror_mode(self):
        with self.assertRaisesRegex(ValueError, 'requires Cloud-only'):
            delivery_operations(self.journal, 'http://native', threading.Event(),
                                enable_file=Path(self.tmp.name) / 'enable')

    def test_marker_removed_during_prod_capture_prevents_following_dev_batch(self):
        marker = Path(self.tmp.name) / 'enabled'
        marker.touch()
        self.journal.accept({'api_key': 'dev', 'events': [{
            'event_type': 'standby', 'device_id': 'device', 'insert_id': 'dev-pending',
            'time': 1791662400123,
        }]})

        def capture(request, **kwargs):
            marker.unlink()
            return io.BytesIO(b'{"status":1}')

        with patch('analytics_ingress.urllib.request.urlopen', side_effect=capture) as send:
            operation = delivery_operations(self.journal, 'http://retired.invalid',
                                            threading.Event(), True, marker)[0]
            self.assertEqual(operation(), 1)
            self.assertEqual(send.call_count, 1)
        with self.journal.connect() as con:
            self.assertEqual(con.execute('SELECT project FROM records WHERE cloud_destination IS NULL').fetchall(), [('2',)])

    def test_missing_or_invalid_cloud_route_fails_before_delivery(self):
        for update in [None, {}, {'host': 'http://untrusted', 'project_id': 101, 'project_token': 'x'},
                       {'host': 'https://us.i.posthog.com', 'project_id': True, 'project_token': 'x'},
                       {'host': 'https://us.i.posthog.com', 'project_id': 101, 'project_token': ''}]:
            with self.subTest(update=update):
                routes = copy.deepcopy(self.routes)
                routes['prod']['cloud'] = update
                with self.assertRaises(ValueError):
                    validate_cloud_routes(routes)
        with self.assertRaises(ValueError):
            validate_cloud_routes({})

    def test_environment_destination_collision_is_rejected(self):
        routes = copy.deepcopy(self.routes)
        routes['dev']['cloud'] = copy.deepcopy(routes['prod']['cloud'])
        with self.assertRaisesRegex(ValueError, 'Distinct source environments'):
            validate_cloud_routes(routes)

    def test_same_project_route_alias_must_keep_same_destination(self):
        routes = copy.deepcopy(self.routes)
        routes['old-prod-client'] = copy.deepcopy(routes['prod'])
        validate_cloud_routes(routes)
        routes['old-prod-client']['cloud']['project_id'] = 999
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            validate_cloud_routes(routes)

    def test_source_alias_tokens_and_environment_tokens_remain_consistent(self):
        routes = copy.deepcopy(self.routes)
        routes['old-prod-client'] = copy.deepcopy(routes['prod'])
        routes['old-prod-client']['cloud']['project_token'] = 'different'
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            validate_cloud_routes(routes)
        routes = copy.deepcopy(self.routes)
        routes['dev']['cloud']['project_token'] = routes['prod']['cloud']['project_token']
        with self.assertRaisesRegex(ValueError, 'Cloud token'):
            validate_cloud_routes(routes)


if __name__ == '__main__':
    unittest.main()
