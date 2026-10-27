"""Project-bound real native people after final public detachment and drain."""
import gzip
import hashlib
import json


class NativePeopleArchive:
    def __init__(self, directory, environment, manifest_sha256):
        if environment not in ('prod', 'dev'):
            raise ValueError('Explicit native source environment required')
        raw = (directory / 'manifest.json').read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest_sha256:
            raise RuntimeError('Final native manifest changed')
        manifest = json.loads(raw)
        if (manifest['state'] != 'final_native_tail_export_complete'
                or manifest.get('public_native_detached') is not True
                or manifest.get('native_pipeline_drained') is not True):
            raise RuntimeError('Native archive is not a final detached/drained source')
        self.project = manifest['projects'][environment]['source_project_id']
        if type(self.project) is not int or self.project <= 0:
            raise RuntimeError('Native source project invalid')
        name = environment + '-metadata-final.json.gz'
        raw = (directory / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest['files'][name]['sha256']:
            raise RuntimeError('Final native metadata changed')
        data = json.loads(gzip.decompress(raw))
        if type(data['project']['id']) is not int or data['project']['id'] != self.project:
            raise RuntimeError('Archived API project differs from source binding')
        self._people = data['collections']['persons']
        if not isinstance(self._people, list):
            raise RuntimeError('Invalid archived native persons')
        aliases = set()
        for person in self._people:
            ids = person['distinct_ids']
            if (not ids or not all(isinstance(i, str) and i for i in ids)
                    or len(ids) != len(set(ids)) or aliases.intersection(ids)):
                raise RuntimeError('Archived native alias membership repeated or invalid')
            aliases.update(ids)

    def people(self, project):
        if type(project) is not int or project != self.project:
            raise RuntimeError('Archived native persons requested for another project')
        return json.loads(json.dumps(self._people))
