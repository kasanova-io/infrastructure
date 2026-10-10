#!/usr/bin/env python3
"""Read-only HTTP acceptance against an isolated candidate container and staged history."""
import argparse
import json
import re
import subprocess
from urllib.parse import urlencode


def request(container, endpoint, params):
    url = 'http://127.0.0.1:3001' + endpoint + '?' + urlencode({'limit': 10, **params})
    result = subprocess.run(['docker', 'exec', container, 'wget', '-S', '-O', '-', url], capture_output=True, text=True)
    statuses = re.findall(r'HTTP/\S+ (\d{3})', result.stderr)
    status = int(statuses[-1]) if statuses else 0
    return status, json.loads(result.stdout) if result.stdout.strip().startswith(('{', '[')) else None


def verify(container, author, quote_id):
    viewer = '02' + '0' * 64
    checks = []
    targets = [('/get-posts', 'user'), ('/get-replies', 'user'), ('/get-mentions', 'user'),
               ('/get-user-details', 'user'), ('/get-users-following', 'userPubkey'),
               ('/get-users-followers', 'userPubkey'), ('/search-users', 'searchedUserPubkey')]
    for key in [author, '02' + author, '03' + author]:
        for endpoint, parameter in targets:
            status, data = request(container, endpoint, {parameter: key, 'requesterPubkey': viewer, 'limit': 10})
            assert status == 200, (endpoint, len(key), status, data)
            if endpoint == '/get-user-details':
                assert data['userPublicKey'] == key, data
                assert isinstance(data['followedUser'], bool) and isinstance(data['blockedUser'], bool)
            if endpoint == '/get-posts':
                if key == author:
                    assert data['posts'], 'Expected staged historical posts'
                    assert all(post['userPublicKey'] == author for post in data['posts'])
                else:
                    assert not data['posts'], 'Historical identity was incorrectly aliased to compressed key'
            checks.append({'endpoint': endpoint, 'key_length': len(key), 'status': status})
    for bad in ['a' * 63, 'g' * 64, '04' + 'a' * 64]:
        for endpoint, parameter in targets:
            status, _ = request(container, endpoint, {parameter: bad, 'requesterPubkey': viewer})
            assert status == 400, (endpoint, bad[:4], status)
            checks.append({'endpoint': endpoint, 'invalid_key': True, 'status': status})
    # Public posts include quotes; details/replies accept their existing transaction-ID contract.
    status, page = request(container, '/get-posts-watching', {'requesterPubkey': viewer, 'limit': 100})
    assert status == 200 and page['posts']
    for post in page['posts']:
        if len(post['userPublicKey']) == 64 or post.get('isQuote'):
            status, detail = request(container, '/get-post-details', {'id': post['id'], 'requesterPubkey': viewer})
            assert status == 200 and detail['post']['userPublicKey'] == post['userPublicKey'], (status, detail)
            status, replies = request(container, '/get-replies', {'post': post['id'], 'requesterPubkey': viewer})
            assert status == 200, (status, replies)
            checks.append({'endpoint': '/get-post-details + /get-replies', 'key_length': len(post['userPublicKey']), 'quote': post.get('isQuote', False), 'status': status})
            if sum('quote' in check for check in checks) >= 3:
                break
    status, detail = request(container, '/get-post-details', {'id': quote_id, 'requesterPubkey': viewer})
    assert status == 200 and detail['post']['id'] == quote_id
    status, _ = request(container, '/get-replies', {'post': quote_id, 'requesterPubkey': viewer})
    assert status == 200
    checks.append({'endpoint': '/get-post-details + /get-replies', 'staged_quote_id': quote_id, 'status': status})
    print(json.dumps({'checks': checks, 'passed': len(checks)}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--container', required=True)
    parser.add_argument('--author', required=True)
    parser.add_argument('--quote-id', required=True)
    args = parser.parse_args()
    verify(args.container, args.author, args.quote_id)
