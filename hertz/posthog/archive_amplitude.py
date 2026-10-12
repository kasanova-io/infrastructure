#!/usr/bin/env python3
"""Read-only Amplitude export; preserves source bytes and explicit coverage receipts."""
import argparse
import base64
import collections
import concurrent.futures
import datetime as dt
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

UTC = dt.timezone.utc
HOUR = dt.timedelta(hours=1)
ROOT = Path(__file__).resolve().parents[2]


def now():
    return dt.datetime.now(UTC).isoformat()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    temp.replace(path)


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        result = super().redirect_request(req, fp, code, msg, headers, newurl)
        if result is not None:
            old = urllib.parse.urlsplit(req.full_url)
            new = urllib.parse.urlsplit(newurl)
            if new.scheme != 'https':
                raise RuntimeError('Refused a non-HTTPS redirect')
            if (old.scheme, old.netloc) != (new.scheme, new.netloc):
                result.remove_header('Authorization')
        return result


class Archive:
    def __init__(self, args):
        self.args = args
        self.base = args.output.resolve() / args.environment
        self.base.mkdir(parents=True, exist_ok=True)
        self.base.chmod(0o700)
        project_key_var = getattr(args, 'project_key_var', None)
        if project_key_var:
            project_key = os.environ.get(project_key_var)
        else:
            config = (ROOT / 'apps/kasanova/lib/config/environment_config.dart').read_text()
            block = config.split('static String get amplitudeApiKey', 1)[1]
            match = re.search(r"case '" + args.environment + r"':\s*return '([^']+)'", block)
            project_key = match[1] if match else None
        if not project_key:
            raise RuntimeError('Configured project key not found')
        secret = os.environ.get(args.secret_var)
        if not secret:
            raise RuntimeError('Required secret variable is unavailable: ' + args.secret_var)
        self.fingerprint = hashlib.sha256(project_key.encode()).hexdigest()
        existing_summary = self.base / 'events-summary.json'
        if existing_summary.exists():
            archived_fingerprint = json.loads(existing_summary.read_text()).get('project_key_sha256')
            if archived_fingerprint != self.fingerprint:
                raise RuntimeError('Configured source project differs from the existing archive')
        self.auth = 'Basic ' + base64.b64encode((project_key + ':' + secret).encode()).decode()
        self.opener = urllib.request.build_opener(SafeRedirect())
        self.cutoff = dt.datetime.strptime(args.cutoff, '%Y%m%dT%H').replace(tzinfo=UTC)
        self.start = dt.datetime.strptime(args.start, '%Y%m%dT%H').replace(tzinfo=UTC)
        if self.start >= self.cutoff:
            raise RuntimeError('Start must precede exclusive cutoff')
        self.journal = self.base / 'requests.jsonl'

    def receipt(self, item):
        with self.journal.open('a') as f:
            f.write(json.dumps({'recorded_at': now(), **item}, ensure_ascii=False) + '\n')

    def fetch(self, endpoint, params, dest, auth=True):
        url = endpoint if endpoint.startswith('https://') else self.args.host + endpoint
        if params:
            url += ('&' if '?' in url else '?') + urllib.parse.urlencode(params)
        if urllib.parse.urlsplit(url).scheme != 'https':
            raise RuntimeError('Refused non-HTTPS export URL')
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp = dest.with_name(dest.name + '.partial')
        for attempt in range(4):
            try:
                req = urllib.request.Request(url, headers={'Authorization': self.auth} if auth else {})
                with self.opener.open(req, timeout=getattr(self.args, 'timeout_seconds', 60)) as response, temp.open('wb') as f:
                    size = 0
                    for block in iter(lambda: response.read(1024 * 1024), b''):
                        if shutil.disk_usage(dest.parent).free < 2 * 1024**3:
                            raise RuntimeError('Less than 2 GiB free; partial download retained')
                        f.write(block)
                        size += len(block)
                    status = response.status
                temp.replace(dest)
                return {'status': status, 'path': str(dest.relative_to(self.base)),
                        'bytes': size, 'sha256': sha(dest), 'fetched_at': now()}
            except urllib.error.HTTPError as e:
                code = e.code
                body = e.read(1024 * 1024)
                if code == 429 or code in (500, 502, 503):
                    if attempt < 3:
                        wait = min(30, max(2**attempt, int(e.headers.get('Retry-After', '0')) if e.headers.get('Retry-After', '0').isdigit() else 0))
                        time.sleep(wait)
                        continue
                err = dest.with_name(dest.name + '.http-' + str(code) + '.response')
                err.write_bytes(body)
                return {'status': code, 'response_path': str(err.relative_to(self.base)),
                        'response_sha256': sha(err), 'fetched_at': now()}
            except (TimeoutError, urllib.error.URLError):
                if endpoint == '/api/2/export':
                    return {'status': 504, 'reason': 'Transport timeout; split this export window', 'fetched_at': now()}
                if attempt == 3:
                    raise
                time.sleep(2**attempt)
        raise RuntimeError('Download attempts exhausted')

    @staticmethod
    def event_stats(path):
        counts = collections.Counter()
        amplitudes = set()
        apps = set()
        uuids = set()
        missing_uuid = 0
        duplicate_uuid = 0
        upload_min = upload_max = event_min = event_max = None
        n = 0
        with zipfile.ZipFile(path) as z:
            files = [e for e in z.infolist() if not e.is_dir()]
            for entry in files:
                with z.open(entry) as stream:
                    buffered = io.BufferedReader(stream)
                    raw = gzip.GzipFile(fileobj=buffered) if buffered.peek(2)[:2] == b'\x1f\x8b' else buffered
                    with io.TextIOWrapper(raw, encoding='utf-8') as lines:
                        for line in lines:
                            if not line.strip():
                                continue
                            item = json.loads(line)
                            if not isinstance(item, dict):
                                raise RuntimeError('Non-object export record')
                            n += 1
                            counts[item.get('event_type', '<missing>')] += 1
                            if item.get('amplitude_id') is not None:
                                amplitudes.add(str(item['amplitude_id']))
                            if item.get('app') is not None:
                                apps.add(str(item['app']))
                            uuid = item.get('uuid')
                            if uuid is None:
                                missing_uuid += 1
                            else:
                                duplicate_uuid += uuid in uuids
                                uuids.add(uuid)
                            for field in ['server_upload_time', 'event_time']:
                                value = item.get(field)
                                if value is not None and not isinstance(value, str):
                                    value = str(value)
                                if value is None:
                                    continue
                                if field == 'server_upload_time':
                                    upload_min = value if upload_min is None else min(upload_min, value)
                                    upload_max = value if upload_max is None else max(upload_max, value)
                                else:
                                    event_min = value if event_min is None else min(event_min, value)
                                    event_max = value if event_max is None else max(event_max, value)
        return {'records': n, 'files': len(files), 'event_types': dict(counts),
                'amplitude_ids': sorted(amplitudes), 'project_ids': sorted(apps),
                'missing_uuid': missing_uuid, 'duplicate_uuid_within_window': duplicate_uuid,
                'upload_time_min': upload_min, 'upload_time_max': upload_max,
                'event_time_min': event_min, 'event_time_max': event_max,
                'validation': 'Every original ZIP member read to EOF (CRC checked); gzip and all JSON records parsed'}

    def window(self, start, end):
        first, last = start.strftime('%Y%m%dT%H'), (end - HOUR).strftime('%Y%m%dT%H')
        name = first + '-' + last
        receipt_path = self.base / 'events' / (name + '.receipt.json')
        if receipt_path.exists():
            old = json.loads(receipt_path.read_text())
            if old.get('status') == 404 or (old.get('status') == 200 and sha(self.base / old['path']) == old['sha256']):
                return [old]
        result = self.fetch('/api/2/export', {'start': first, 'end': last}, self.base / 'events' / (name + '.zip'))
        result.update({'start': first, 'end_exclusive': end.strftime('%Y%m%dT%H'),
                       'project_key_sha256': self.fingerprint})
        self.receipt({'kind': 'events', **result})
        if result['status'] in (400, 504) and end - start > HOUR:
            hours = int((end - start) / HOUR)
            mid = start + HOUR * (hours // 2)
            return self.window(start, mid) + self.window(mid, end)
        if result['status'] == 200:
            result.update(self.event_stats(self.base / result['path']))
        elif result['status'] == 404:
            result['records'] = 0
            result['coverage'] = 'Export API reports no data in this upload-time window'
        atomic_json(receipt_path, result)
        print(json.dumps({'kind': 'events', 'start': first, 'end': last,
                          'status': result['status'], 'records': result.get('records'),
                          'bytes': result.get('bytes')}), flush=True)
        return [result]

    def events(self):
        summary_path = self.base / 'events-summary.json'
        summary = {'project_key_sha256': self.fingerprint, 'started_at': now(),
                   'start': self.args.start, 'cutoff_exclusive': self.args.cutoff,
                   'range_basis': 'server_upload_time, UTC; API end hour is inclusive',
                   'windows': [], 'records': 0, 'coverage_complete': False}
        end = self.cutoff
        if summary_path.exists():
            prior = json.loads(summary_path.read_text())
            if any(prior.get(k) != summary[k] for k in ('project_key_sha256', 'start', 'cutoff_exclusive')):
                raise RuntimeError('Existing event archive has different source or time boundaries')
            cursor = self.args.cutoff
            for window in sorted(prior['windows'], key=lambda w: w['start'], reverse=True):
                if window['end_exclusive'] != cursor or window['status'] not in (200, 404):
                    raise RuntimeError('Cannot resume an archive with a gap or unresolved failure')
                if window['status'] == 200 and sha(self.base / window['path']) != window['sha256']:
                    raise RuntimeError('Existing event archive checksum changed')
                cursor = window['start']
            summary = prior
            end = dt.datetime.strptime(cursor, '%Y%m%dT%H').replace(tzinfo=UTC)
            if summary.get('coverage_complete'):
                print(json.dumps({'kind': 'events_already_complete', 'records': summary['records']}), flush=True)
                return
        populated_width = self.args.days_per_window
        width_days = populated_width
        while end > self.start:
            ranges = []
            cursor = end
            for _ in range(3 if width_days != 365 else 1):
                if cursor <= self.start:
                    break
                begin = max(self.start, cursor - dt.timedelta(days=width_days))
                ranges.append((begin, cursor))
                cursor = begin
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                batches = list(pool.map(lambda r: self.window(*r), ranges))
            results = [result for batch in batches for result in batch]
            start = cursor
            summary['windows'].extend(results)
            summary['records'] += sum(r.get('records', 0) for r in results)
            summary['updated_at'] = now()
            atomic_json(summary_path, summary)
            if any(r['status'] not in (200, 404) for r in results):
                print('Event archive has an unresolved API failure; see private receipts.', flush=True)
                return
            width_days = 365 if all(r['status'] == 404 for r in results) else populated_width
            end = start
        spans = sorted(summary['windows'], key=lambda r: r['start'])
        cursor = self.args.start
        for span in spans:
            if span['start'] != cursor:
                raise RuntimeError('Coverage windows have a gap or overlap')
            cursor = span['end_exclusive']
        if cursor != self.args.cutoff:
            raise RuntimeError('Coverage ends before fixed cutoff')
        summary['coverage_complete'] = True
        summary['completed_at'] = now()
        summary['limitation'] = 'Source API coverage and byte/CRC/JSON validation; excludes data not exportable or already deleted by Amplitude; not an organization-wide or real-time snapshot'
        atomic_json(summary_path, summary)
        print(json.dumps({'kind': 'events_complete', 'records': summary['records'], 'windows': len(spans)}), flush=True)

    def metadata(self):
        out = self.base / 'metadata'
        endpoints = [
            ('visible-events', '/api/2/events/list', {}),
            ('taxonomy-events', '/api/2/taxonomy/event', {'showDeleted': 'true'}),
            ('taxonomy-user-properties', '/api/2/taxonomy/user-property', {'showDeleted': 'true'}),
            ('cohorts', '/api/3/cohorts', {'includeSyncInfo': 'true'}),
        ]
        report = {'started_at': now(), 'endpoints': []}
        for label, endpoint, params in endpoints:
            result = self.fetch(endpoint, params, out / (label + '.json'))
            result['kind'] = label
            self.receipt(result)
            report['endpoints'].append(result)
            print(json.dumps({'kind': label, 'status': result['status']}), flush=True)
        report['limitations'] = ['Visible-only taxonomy APIs omit hidden definitions; raw events are not filtered.', 'Current profiles, private cohorts, saved charts, dashboards and organization project inventory require additional enumeration or access.']
        atomic_json(self.base / 'metadata-summary.json', report)

    def cohorts(self):
        listing = self.base / 'metadata/cohorts.json'
        cohorts = json.loads(listing.read_text())['cohorts']
        report = {'started_at': now(), 'discoverable_cohorts': len(cohorts), 'exports': [], 'complete': False}
        for cohort in cohorts:
            cid = cohort['id']
            folder = self.base / 'cohorts' / hashlib.sha256(cid.encode()).hexdigest()
            folder.mkdir(parents=True, exist_ok=True)
            atomic_json(folder / 'definition.json', cohort)
            result = self.fetch('/api/5/cohorts/request/' + urllib.parse.quote(cid, safe=''), {'props': 1}, folder / 'request.json')
            record = {'id_sha256': folder.name, 'request': result, 'download_complete': False}
            self.receipt({'kind': 'cohort-export-request', **result})
            if result['status'] in (200, 202):
                data = json.loads((self.base / result['path']).read_text())
                request_id = data['request_id']
                for attempt in range(60):
                    status = self.fetch('/api/5/cohorts/request-status/' + urllib.parse.quote(request_id, safe=''), {}, folder / ('status-' + str(attempt).zfill(3) + '.json'))
                    self.receipt({'kind': 'cohort-export-status', **status})
                    if status['status'] == 200:
                        state = json.loads((self.base / status['path']).read_text())
                        if state.get('async_status') != 'JOB COMPLETED':
                            record['status_schema_unexpected'] = True
                            break
                        download = self.fetch('/api/5/cohorts/request/' + urllib.parse.quote(request_id, safe='') + '/file', {}, folder / 'members-original.bin')
                        self.receipt({'kind': 'cohort-members', **download})
                        record['download'] = download
                        record['download_complete'] = download['status'] == 200
                        break
                    if status['status'] != 202:
                        record['status_failure'] = status['status']
                        break
                    time.sleep(5)
            report['exports'].append(record)
            atomic_json(self.base / 'cohorts-summary.json', report)
            print(json.dumps({'kind': 'cohort-export', 'number': len(report['exports']),
                              'download_complete': record['download_complete']}), flush=True)
            # The provider permits four parallel/single export requests per minute.
            if len(report['exports']) < len(cohorts):
                time.sleep(15)
        report['complete'] = all(e['download_complete'] for e in report['exports'])
        report['completed_at'] = now()
        report['limitation'] = 'All discoverable listed cohorts only; users outside them and undiscoverable private cohorts are not covered by this listing'
        atomic_json(self.base / 'cohorts-summary.json', report)

    def schemas(self):
        listing = json.loads((self.base / 'metadata/taxonomy-events.json').read_text())['data']
        names = {item['event_type'] for item in listing}
        event_summary = self.base / 'events-summary.json'
        if event_summary.exists():
            for window in json.loads(event_summary.read_text()).get('windows', []):
                names.update(window.get('event_types', {}))
        out = self.base / 'schemas'
        shared = self.fetch('/api/2/taxonomy/event-property', {}, out / 'shared-event-properties.json')
        report = {'started_at': now(), 'shared': shared, 'event_properties': []}

        def one(name):
            hashed = hashlib.sha256(name.encode()).hexdigest()
            folder = out / 'events' / hashed
            folder.mkdir(parents=True, exist_ok=True)
            atomic_json(folder / 'event-name.json', {'event_type': name})
            result = self.fetch('/api/2/taxonomy/event-property', {'event_type': name}, folder / 'properties.json')
            self.receipt({'kind': 'event-property-schema', 'event_name_sha256': hashed, **result})
            return {'event_name_sha256': hashed, **result}

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            for result in pool.map(one, sorted(names)):
                report['event_properties'].append(result)
                atomic_json(self.base / 'schemas-summary.json', report)
        report['completed_at'] = now()
        report['complete_for_enumerated_event_names'] = shared['status'] == 200 and all(r['status'] == 200 for r in report['event_properties'])
        report['limitation'] = 'Provider can omit hidden/deleted definitions; original records remain in event archives'
        atomic_json(self.base / 'schemas-summary.json', report)
        print(json.dumps({'kind': 'schemas_complete', 'events': len(names), 'complete': report['complete_for_enumerated_event_names']}), flush=True)

    def profiles(self):
        report = {'started_at': now(), 'scope': 'Every Amplitude ID discovered in archived raw events',
                  'exports': {}, 'complete_for_event_discovered_ids': False,
                  'provider_limit': '360 user activity/search queries per hour; this job reserves at most 300 per rolling hour'}
        report_path = self.base / 'profiles-summary.json'
        if report_path.exists():
            report = json.loads(report_path.read_text())
        attempts = []
        if self.journal.exists():
            for line in self.journal.read_text().splitlines():
                item = json.loads(line)
                if item.get('kind') == 'user-profile':
                    attempts.append(dt.datetime.fromisoformat(item['recorded_at']))
        while True:
            source = self.base / 'events-summary.json'
            if not source.exists():
                time.sleep(5)
                continue
            events = json.loads(source.read_text())
            ids = sorted({i for w in events.get('windows', []) for i in w.get('amplitude_ids', [])})
            pending = [i for i in ids if hashlib.sha256(i.encode()).hexdigest() not in report['exports']]
            report['known_amplitude_ids'] = len(ids)
            for identifier in pending:
                attempts = [t for t in attempts if dt.datetime.now(UTC) - t < dt.timedelta(hours=1)]
                while len(attempts) >= 300:
                    report['waiting_for_provider_quota'] = True
                    atomic_json(report_path, report)
                    time.sleep(min(30, max(1, 3601 - (dt.datetime.now(UTC) - min(attempts)).total_seconds())))
                    attempts = [t for t in attempts if dt.datetime.now(UTC) - t < dt.timedelta(hours=1)]
                report['waiting_for_provider_quota'] = False
                hashed = hashlib.sha256(identifier.encode()).hexdigest()
                dest = self.base / 'profiles' / (hashed + '.json')
                if dest.exists():
                    result = {'status': 200, 'path': str(dest.relative_to(self.base)), 'bytes': dest.stat().st_size,
                              'sha256': sha(dest), 'reused_original_export': True}
                else:
                    result = self.fetch('/api/2/useractivity', {'user': identifier, 'limit': 1, 'direction': 'latest'}, dest)
                    self.receipt({'kind': 'user-profile', **result})
                    attempts.append(dt.datetime.now(UTC))
                if result['status'] == 200:
                    data = json.loads(dest.read_text())
                    result['userdata_present'] = isinstance(data.get('userData'), dict)
                if result['status'] == 429:
                    report['provider_rate_limited'] = True
                    atomic_json(report_path, report)
                    time.sleep(30)
                    break
                report['exports'][hashed] = result
                atomic_json(report_path, report)
                if len(report['exports']) % 50 == 0:
                    print(json.dumps({'kind': 'profiles', 'exported': len(report['exports']), 'currently_known_ids': len(ids)}), flush=True)
                if result['status'] in (401, 403):
                    return
            else:
                if events.get('coverage_complete'):
                    # The event summary can grow while this batch is running. Read it again.
                    final = json.loads(source.read_text())
                    final_ids = {i for w in final.get('windows', []) for i in w.get('amplitude_ids', [])}
                    if all(hashlib.sha256(i.encode()).hexdigest() in report['exports'] for i in final_ids):
                        report['known_amplitude_ids'] = len(final_ids)
                        report['complete_for_event_discovered_ids'] = all(r['status'] == 200 and r.get('userdata_present') for r in report['exports'].values())
                        report['completed_at'] = now()
                        report['limitation'] = 'IDs discovered in retained raw events only; profile-only users absent from source event exports cannot be enumerated by this endpoint'
                        atomic_json(report_path, report)
                        print(json.dumps({'kind': 'profiles_complete', 'exported': len(report['exports']), 'complete': report['complete_for_event_discovered_ids']}), flush=True)
                        return
            time.sleep(5)

    def replays(self):
        out = self.base / 'replays'
        summary_path = self.base / 'replays-summary.json'
        summary = {'started_at': now(), 'cutoff_exclusive': self.args.cutoff,
                   'recordings': [], 'list_complete': False, 'files_complete': False,
                   'playback_verified': False, 'errors': []}
        token = None
        seen_tokens = set()
        ids = set()
        page = 0
        while True:
            params = {'page_size': 200, 'sort_order': 'asc',
                      'start_time': self.start.isoformat(),
                      'end_time': (self.cutoff - dt.timedelta(microseconds=1)).isoformat()}
            if token:
                params['page_token'] = token
            result = self.fetch('/api/1/session-replays', params, out / 'list' / (str(page).zfill(5) + '.json'))
            self.receipt({'kind': 'replay-list', **result})
            if result['status'] != 200:
                summary['errors'].append({'kind': 'list', 'status': result['status']})
                break
            data = json.loads((self.base / result['path']).read_text())
            if not isinstance(data.get('session_replays'), list) or 'next_page_token' not in data:
                raise RuntimeError('Unexpected replay pagination schema')
            for replay in data['session_replays']:
                rid = replay['replay_id']
                if rid in ids:
                    summary['errors'].append({'kind': 'duplicate_replay_listing'})
                    continue
                ids.add(rid)
                folder = out / 'recordings' / hashlib.sha256(rid.encode()).hexdigest()
                folder.mkdir(parents=True, exist_ok=True)
                atomic_json(folder / 'recording.json', replay)
                record = {'id_sha256': folder.name, 'files': [], 'files_complete': False}
                ftoken = None
                ftokens = set()
                fpage = 0
                while True:
                    fparams = {'replay_id': rid, 'version': 3, 'page_size': 1000}
                    if ftoken:
                        fparams['page_token'] = ftoken
                    fres = self.fetch('/api/1/session-replays/files', fparams, folder / ('file-list-' + str(fpage).zfill(5) + '.json'))
                    self.receipt({'kind': 'replay-files-list', 'id_sha256': folder.name, **fres})
                    if fres['status'] != 200:
                        record['error_status'] = fres['status']
                        break
                    fdata = json.loads((self.base / fres['path']).read_text())
                    if not isinstance(fdata.get('files'), list) or 'next_page_token' not in fdata:
                        raise RuntimeError('Unexpected recording-file pagination schema')
                    for url in fdata['files']:
                        dest = folder / ('chunk-' + str(len(record['files'])).zfill(6) + '.bin')
                        chunk = self.fetch(url, {}, dest, auth=False)
                        if chunk['status'] == 200:
                            blob = dest.read_bytes()
                            if blob[:2] == b'\x1f\x8b':
                                blob = gzip.decompress(blob)
                            parsed = json.loads(blob)
                            chunk['parsed_items'] = len(parsed) if isinstance(parsed, list) else None
                            chunk['json_validated'] = True
                        record['files'].append(chunk)
                        self.receipt({'kind': 'replay-chunk', 'id_sha256': folder.name, **chunk})
                    ftoken = fdata['next_page_token']
                    if not ftoken:
                        record['files_complete'] = all(c['status'] == 200 for c in record['files'])
                        break
                    if ftoken in ftokens:
                        raise RuntimeError('Repeated recording pagination cursor')
                    ftokens.add(ftoken)
                    fpage += 1
                atomic_json(folder / 'receipt.json', record)
                summary['recordings'].append(record)
                atomic_json(summary_path, summary)
            print(json.dumps({'kind': 'replays', 'page': page, 'recordings': len(summary['recordings']),
                              'files': sum(len(r['files']) for r in summary['recordings'])}), flush=True)
            token = data['next_page_token']
            if not token:
                summary['list_complete'] = True
                break
            if token in seen_tokens:
                raise RuntimeError('Repeated replay pagination cursor')
            seen_tokens.add(token)
            page += 1
        summary['files_complete'] = summary['list_complete'] and not summary['errors'] and all(r['files_complete'] for r in summary['recordings'])
        summary['completed_at'] = now()
        summary['limitation'] = 'Available recordings whose start precedes cutoff; fetched files preserved, playback and expiring/changed source contents not certified'
        atomic_json(summary_path, summary)
        print(json.dumps({'kind': 'replays_complete', 'recordings': len(summary['recordings']),
                          'files_complete': summary['files_complete']}), flush=True)


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['events', 'metadata', 'replays', 'cohorts', 'schemas', 'profiles'])
    p.add_argument('--environment', choices=['prod', 'dev'], required=True)
    p.add_argument('--secret-var', default='AMPLITUDE_SECRET_KEY')
    p.add_argument('--project-key-var', help='Read the selected source project key from this environment variable')
    p.add_argument('--host', default='https://amplitude.com')
    p.add_argument('--start', default='20100101T00')
    p.add_argument('--days-per-window', type=int, choices=[1, 7, 14, 30], default=7)
    p.add_argument('--cutoff', required=True, help='Exclusive UTC upload hour boundary')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    archive = Archive(args)
    getattr(archive, args.mode)()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Network exceptions can contain presigned URLs; never print their details.
        print(json.dumps({'fatal_error_type': type(error).__name__, 'completed': False}), flush=True)
        raise SystemExit(1)
