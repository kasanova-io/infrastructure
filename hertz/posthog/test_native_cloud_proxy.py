"""Real SDK wire shapes retain payloads and map only configured project tokens."""
import copy
import gzip
import io
import json
import unittest
import urllib.error
from unittest.mock import patch

from analytics_ingress import precise_json
from native_cloud_proxy import NativeCloudProxy, MAX_BODY, NoRedirect


class Response(io.BytesIO):
    code = 200
    headers = {'Content-Type': 'application/json'}


class NativeCloudProxyTest(unittest.TestCase):
    def setUp(self):
        self.routes = {env: {'project_token': 'old-' + env,
                            'cloud': {'host': 'https://us.i.posthog.com',
                                      'project_token': 'cloud-' + env}}
                       for env in ('prod', 'dev')}
        self.proxy = NativeCloudProxy(self.routes, precise_json)
        self.event = {'uuid': 'same-original-uuid', 'event': '$snapshot',
                      'timestamp': '2026-10-27T16:00:00.123456Z',
                      'properties': {'$session_id': 'same-session',
                                     'nested': [True, 1, None, 0.10000000000000002]}}

    def forward(self, target, payload, compressed=False):
        raw = json.dumps(payload).encode()
        if compressed:
            raw = gzip.compress(raw)
        with patch.object(self.proxy.opener, 'open', return_value=Response(b'{"status":1}')) as send:
            result = self.proxy.request('POST', target, raw, 'gzip' if compressed else '')
        return result, send.call_args.args[0]

    def test_ios_android_gzip_batch_translates_only_top_level_token(self):
        payload = {'api_key': 'old-prod', 'batch': [self.event], 'sent_at': 'same-send-time'}
        original = copy.deepcopy(payload)
        result, request = self.forward('/batch/', payload, compressed=True)
        expected = {**payload, 'api_key': 'cloud-prod'}
        self.assertEqual(json.loads(request.data), expected)
        self.assertEqual(payload, original)
        self.assertEqual(request.full_url, 'https://us.i.posthog.com/batch/')
        self.assertNotIn('Content-encoding', request.headers)
        self.assertEqual(result[0], 200)

    def test_gzip_replay_array_preserves_all_snapshot_session_and_typed_fields(self):
        payload = [{**self.event, 'api_key': 'old-dev'},
                   {**self.event, 'uuid': 'next-snapshot', 'api_key': 'old-dev'}]
        result, request = self.forward('/s/', payload, compressed=True)
        self.assertEqual(json.loads(request.data), [{**x, 'api_key': 'cloud-dev'} for x in payload])
        self.assertEqual(result[0], 200)

    def test_current_cloud_key_is_also_forwarded_and_flag_query_is_preserved(self):
        payload = {'api_key': 'cloud-prod', 'distinct_id': 'same-wallet', 'groups': {}}
        _, request = self.forward('/flags/?v=2', payload)
        self.assertEqual(request.full_url, 'https://us.i.posthog.com/flags/?v=2')
        self.assertEqual(json.loads(request.data), payload)

    def test_remote_config_translates_path_and_uses_configured_region_assets(self):
        with patch.object(self.proxy.opener, 'open', return_value=Response(b'{}')) as send:
            self.proxy.request('GET', '/array/old-dev/config?v=2')
        self.assertEqual(send.call_args.args[0].full_url,
                         'https://us-assets.i.posthog.com/array/cloud-dev/config?v=2')
        routes = copy.deepcopy(self.routes)
        routes['prod']['cloud']['host'] = 'https://eu.i.posthog.com'
        eu = NativeCloudProxy(routes, precise_json)
        with patch.object(eu.opener, 'open', return_value=Response(b'{}')) as send:
            eu.request('GET', '/array/old-prod/config')
        self.assertEqual(send.call_args.args[0].full_url,
                         'https://eu-assets.i.posthog.com/array/cloud-prod/config')

    def test_unknown_tokens_and_mixed_project_replays_never_contact_upstream(self):
        invalid = [{'api_key': 'unknown'}, [{'api_key': 'old-prod'}, {'api_key': 'old-dev'}],
                   [{'api_key': 'old-prod'}, {}], []]
        for payload in invalid:
            with self.subTest(payload=payload), patch.object(self.proxy.opener, 'open') as send:
                with self.assertRaises(ValueError):
                    self.proxy.request('POST', '/s/', json.dumps(payload).encode())
                send.assert_not_called()

    def test_paths_and_gzip_expansion_are_bounded_before_upstream(self):
        for path in ['https://attacker.test/batch', '//attacker.test/batch', '/api/projects/1/',
                     '/batch#fragment', '/batch/../api']:
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.proxy.request('POST', path, b'{}')
        with patch('native_cloud_proxy.MAX_BODY', 100), patch.object(self.proxy.opener, 'open') as send:
            with self.assertRaises(ValueError):
                self.proxy.request('POST', '/batch', gzip.compress(b' ' * 101), 'gzip')
            send.assert_not_called()

    def test_cloud_errors_and_transport_failure_are_never_forged_as_success(self):
        error = urllib.error.HTTPError('https://us.i.posthog.com/batch', 429, 'rate limit', {}, io.BytesIO(b'{}'))
        with patch.object(self.proxy.opener, 'open', side_effect=error):
            self.assertEqual(self.proxy.request('POST', '/batch', b'{"api_key":"old-prod"}')[0], 429)
        with patch.object(self.proxy.opener, 'open', side_effect=OSError('offline')):
            with self.assertRaises(OSError):
                self.proxy.request('POST', '/batch', b'{"api_key":"old-prod"}')
        with self.assertRaises(RuntimeError):
            NoRedirect().redirect_request(None, None, None, None, None)

    def test_conflicting_old_native_token_mapping_is_rejected(self):
        self.routes['dev']['project_token'] = self.routes['prod']['project_token']
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            NativeCloudProxy(self.routes, precise_json)


if __name__ == '__main__':
    unittest.main()
