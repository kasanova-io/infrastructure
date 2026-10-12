import io
import json
from decimal import Decimal
import math
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from analytics_ingress import Journal, precise_json, cloud_event, NUMERIC_REPAIRS
from backup import journal_inventory, snapshot_journal


class CloudDeliveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.routes = {
            name: {'project_id': project, 'project_token': 'hertz-' + name,
                   'legacy_app': name, 'cloud': {'host': 'https://us.i.posthog.com',
                   'project_id': project + 100, 'project_token': 'cloud-' + name}}
            for name, project in [('prod', 1), ('dev', 2)]}
        self.path = self.root / 'journal.sqlite'
        self.journal = Journal(self.path, self.routes)
        self.event = {'event_type': 'wallet_created', 'device_id': 'existing-device',
                      'user_id': 'existing-user', 'insert_id': 'existing-id',
                      'time': 1791662400123,
                      'event_properties': {'decimal': 0.10000000000000002,
                                           'large': 9007199254740991,
                                           'null': None, 'bool': True}}
        self.journal.accept({'api_key': 'prod', 'events': [self.event]})

    @staticmethod
    def ack(request, timeout):
        return io.BytesIO(b'{"status":1}')

    def test_independent_destination_acknowledgements_and_restart(self):
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=self.ack) as send:
            self.assertEqual(self.journal.deliver_cloud(), 1)
            self.assertEqual(self.journal.counts(), {0: 1})
            body = json.loads(send.call_args.args[0].data)
            with self.journal.connect() as con:
                original = json.loads(con.execute('SELECT native FROM records').fetchone()[0])
            self.assertEqual(body['batch'], [cloud_event(original)])
            self.assertEqual(body['api_key'], 'cloud-prod')
            self.assertEqual(send.call_args.args[0].full_url, 'https://us.i.posthog.com/batch/')
            restarted = Journal(self.path, self.routes)
            self.assertEqual(restarted.deliver_cloud(), 0)
            self.assertEqual(restarted.deliver('http://proxy'), 1)
            self.assertEqual(restarted.counts(), {1: 1})
            self.assertEqual(restarted.deliver_cloud(), 0)

    def test_cloud_wire_preserves_problematic_fraction_and_original_journal_bytes(self):
        value = 0.09081934846989141
        event = {**self.event, 'insert_id':'fraction-regression',
                 'event_properties':{'fraction':value, 'nested':[value,True,None], 'integer':9007199254740991}}
        self.journal.accept({'api_key':'dev','events':[event]})
        with self.journal.connect() as con:
            original = con.execute("SELECT raw,native FROM records WHERE project='2'").fetchone()
        with patch('analytics_ingress.urllib.request.urlopen',side_effect=self.ack) as send:
            self.journal.deliver_cloud()
        request = next(call.args[0] for call in send.call_args_list if json.loads(call.args[0].data)['api_key']=='cloud-dev')
        self.assertIn(str(Decimal.from_float(value)).encode(),request.data)
        actual=json.loads(request.data)['batch'][0]
        self.assertEqual(actual,cloud_event(json.loads(original[1])))
        self.assertEqual(actual['properties']['fraction'],value)
        with self.journal.connect() as con:
            self.assertEqual(con.execute("SELECT raw,native FROM records WHERE project='2'").fetchone(),original)

    def test_numeric_repairs_preserve_originals_types_and_nested_paths(self):
        properties = {'price': 0.10000000000000002, 'nested': [{'price': 0.09081934846989141}],
                      'zero': -0.0, 'integer': 9007199254740991, 'boolean': True, 'null': None}
        event = {'properties': properties, 'uuid': 'original'}
        result = cloud_event(event)
        self.assertNotIn(NUMERIC_REPAIRS, properties)
        self.assertEqual(result['uuid'], event['uuid'])
        repairs = json.loads(result['properties'][NUMERIC_REPAIRS])
        self.assertEqual([r['path'] for r in repairs], [['price'], ['nested', 1, 'price'], ['zero']])
        self.assertEqual([r['value'] for r in repairs], [repr(properties['price']), repr(properties['nested'][0]['price']), '-0.0'])
        for key in properties:
            self.assertEqual(result['properties'][key], properties[key])
        self.assertIs(cloud_event({'properties': {'integer': 1}})['properties']['integer'], 1)
        with self.assertRaises(ValueError): cloud_event({'properties': {NUMERIC_REPAIRS: 'spoofed'}})
        with self.assertRaises(ValueError): cloud_event({'properties': {'price': float('inf')}})

    def test_precise_json_retains_numeric_boolean_null_and_string_types(self):
        value={'nested':[0.09081934846989141,True,None,'0.09081934846989141'],
               'large_integer':9007199254740991,'negative':-0.10000000000000002}
        self.assertEqual(json.loads(precise_json(value)),value)
        for number in [-0.0,0.0,1.0,-1.0]:
            parsed=json.loads(precise_json(number))
            self.assertIs(type(parsed),float)
            self.assertEqual(parsed,number)
            self.assertEqual(math.copysign(1,parsed),math.copysign(1,number))
        for bad in [float('nan'),float('inf'),float('-inf')]:
            with self.assertRaises(ValueError):precise_json({'number':bad})

    def test_failed_or_rejected_cloud_delivery_never_marks_acknowledged(self):
        for failure in [OSError('offline'), io.BytesIO(b'{"status":0}')]:
            with self.subTest(failure=type(failure).__name__):
                kwargs = {'side_effect': failure} if isinstance(failure, Exception) else {'return_value': failure}
                with patch('analytics_ingress.urllib.request.urlopen', **kwargs):
                    with self.assertRaises((OSError, RuntimeError)):
                        self.journal.deliver_cloud()
                with self.journal.connect() as con:
                    self.assertIsNone(con.execute('SELECT cloud_destination FROM records').fetchone()[0])
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=self.ack):
            self.assertEqual(self.journal.deliver('http://proxy'), 1)
            self.assertEqual(self.journal.deliver_cloud(), 1)

    def test_project_mapping_preserves_source_ids_and_dev_isolation(self):
        self.journal.accept({'api_key': 'dev', 'events': [self.event]})
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=self.ack) as send:
            self.assertEqual(self.journal.deliver_cloud(), 2)
            payloads = [json.loads(call.args[0].data) for call in send.call_args_list]
        self.assertEqual({p['api_key'] for p in payloads}, {'cloud-prod', 'cloud-dev'})
        self.assertEqual(len({p['batch'][0]['uuid'] for p in payloads}), 2)
        for payload in payloads:
            self.assertEqual(payload['batch'][0]['properties']['__kasanova_transport_record'],
                             json.dumps(self.event, sort_keys=True, separators=(',', ':')))

    def test_prod_cloud_failure_does_not_starve_dev(self):
        self.journal.accept({'api_key': 'dev', 'events': [self.event]})
        attempted = []
        def capture(request, timeout):
            token = json.loads(request.data)['api_key']
            attempted.append(token)
            if token == 'cloud-prod':
                raise OSError('PROD rejected')
            return self.ack(request, timeout)
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=capture):
            self.assertEqual(self.journal.deliver_cloud(), 1)
        self.assertEqual(attempted, ['cloud-prod', 'cloud-dev'])
        with self.journal.connect() as con:
            states = dict(con.execute('SELECT project,cloud_destination FROM records'))
        self.assertIsNone(states['1'])
        self.assertEqual(states['2'], 'https://us.i.posthog.com:102')

    def test_shutdown_does_not_start_another_project_request(self):
        self.journal.accept({'api_key': 'dev', 'events': [self.event]})
        for operation in [lambda stop: self.journal.deliver('http://proxy', stop),
                          self.journal.deliver_cloud]:
            stopping = threading.Event()
            def capture(request, timeout):
                stopping.set()
                return self.ack(request, timeout)
            with patch('analytics_ingress.urllib.request.urlopen', side_effect=capture) as send:
                self.assertEqual(operation(stopping), 1)
                self.assertEqual(send.call_count, 1)

    def test_existing_schema_upgrade_retains_acknowledged_rows(self):
        legacy = self.root / 'legacy.sqlite'
        with sqlite3.connect(legacy) as con:
            con.execute('CREATE TABLE records(project TEXT,id TEXT,raw TEXT,native TEXT,sent INTEGER,received REAL,PRIMARY KEY(project,id))')
            con.execute('INSERT INTO records VALUES (?,?,?,?,?,?)',
                        ('1', 'original', 'raw', '{}', 1, 1.0))
        upgraded = Journal(legacy, self.routes)
        with upgraded.connect() as con:
            self.assertEqual(con.execute('SELECT project,id,raw,native,sent,received,cloud_destination FROM records').fetchone(),
                             ('1', 'original', 'raw', '{}', 1, 1.0, None))
        self.assertEqual(upgraded.counts(), {1: 1})

    def test_backup_restores_cloud_acknowledgements_and_pending_events(self):
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=self.ack):
            self.journal.deliver_cloud()
        destination = self.root / 'snapshot.sqlite'
        expected = snapshot_journal(self.path, destination)
        self.assertEqual(expected, journal_inventory(destination))
        restored = Journal(destination, self.routes)
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=self.ack) as send:
            self.assertEqual(restored.deliver_cloud(), 0)
            send.assert_not_called()
        self.assertEqual(restored.counts(), {0: 1})

    def test_changed_destination_replays_with_same_uuid_and_invalid_host_rejected(self):
        with patch('analytics_ingress.urllib.request.urlopen', side_effect=self.ack) as send:
            self.journal.deliver_cloud()
            first = json.loads(send.call_args.args[0].data)['batch']
            self.routes['prod']['cloud']['project_id'] = 999
            self.assertEqual(self.journal.deliver_cloud(), 1)
            self.assertEqual(json.loads(send.call_args.args[0].data)['batch'], first)
        self.routes['prod']['cloud']['host'] = 'https://unrelated.example'
        with patch('analytics_ingress.urllib.request.urlopen') as send:
            with self.assertRaises(ValueError):
                self.journal.deliver_cloud()
            send.assert_not_called()


if __name__ == '__main__':
    unittest.main()
