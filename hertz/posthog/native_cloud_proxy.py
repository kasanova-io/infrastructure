"""Keep installed native SDK/replay requests on their configured Cloud projects.

Only legacy public project tokens are translated. The caller acknowledges after
Cloud responds, leaving the native SDK's durable retry queue authoritative.
"""
import copy
import gzip
import io
import json
import urllib.error
import urllib.parse
import urllib.request

MAX_BODY = 32 * 1024 * 1024
POST_PATHS = frozenset(('/batch', '/s', '/e', '/capture', '/i/v0/e', '/flags', '/decide'))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('Cloud redirects are not followed')


class NativeCloudProxy:
    def __init__(self, routes, serialize):
        self.serialize = serialize
        self.tokens = {}
        self.opener = urllib.request.build_opener(NoRedirect())
        for route in routes.values():
            cloud = route['cloud']
            host = cloud['host'].rstrip('/')
            if host not in ('https://us.i.posthog.com', 'https://eu.i.posthog.com'):
                raise ValueError('Unsupported configured Cloud origin')
            destination = (host, cloud['project_token'])
            for token in (route['project_token'], cloud['project_token']):
                if token in self.tokens and self.tokens[token] != destination:
                    raise ValueError('Native token maps to conflicting Cloud projects')
                self.tokens[token] = destination

    def destination(self, token):
        if not isinstance(token, str) or token not in self.tokens:
            raise ValueError('Unknown native project token')
        return self.tokens[token]

    def request(self, method, target, body=b'', encoding='', user_agent=''):
        url = urllib.parse.urlsplit(target)
        if url.scheme or url.netloc or url.fragment:
            raise ValueError('Only native relative SDK paths are accepted')
        if method == 'GET':
            parts = url.path.strip('/').split('/')
            if len(parts) != 3 or parts[0] != 'array' or parts[2] != 'config':
                raise ValueError('Unknown native configuration path')
            host, token = self.destination(parts[1])
            region = 'us' if host == 'https://us.i.posthog.com' else 'eu'
            origin = f'https://{region}-assets.i.posthog.com'
            path = '/array/' + urllib.parse.quote(token, safe='') + '/config'
            data = None
        elif method == 'POST' and url.path.rstrip('/') in POST_PATHS:
            if len(body) > MAX_BODY:
                raise ValueError('Native body exceeds size limit')
            if encoding.lower() == 'gzip':
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                    body = stream.read(MAX_BODY + 1)
            elif encoding:
                raise ValueError('Unsupported native content encoding')
            if len(body) > MAX_BODY:
                raise ValueError('Uncompressed native body exceeds size limit')
            payload = json.loads(body)
            records = payload if isinstance(payload, list) else [payload]
            if not records or any(not isinstance(record, dict) for record in records):
                raise ValueError('Native SDK payload must contain objects')
            destinations = {self.destination(record.get('api_key')) for record in records}
            if len(destinations) != 1:
                raise ValueError('Mixed native environments rejected before forwarding')
            origin, token = destinations.pop()
            translated = copy.deepcopy(payload)
            for record in translated if isinstance(translated, list) else [translated]:
                record['api_key'] = token
            data = self.serialize(translated).encode()
            path = url.path
        else:
            raise ValueError('Unknown native SDK path')
        if url.query:
            path += '?' + url.query
        headers = {'Content-Type': 'application/json'}
        if user_agent:
            headers['User-Agent'] = user_agent
        request = urllib.request.Request(origin + path, data=data, headers=headers, method=method)
        try:
            response = self.opener.open(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read(MAX_BODY + 1)
            if len(raw) > MAX_BODY:
                raise RuntimeError('Cloud response exceeds size limit')
            return response.code, response.headers.get('Content-Type', 'application/json'), raw
