#!/usr/bin/env python3
"""Read-only destination-bound check before sealing the retired Cloud journal."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3

from analytics_ingress import validate_cloud_routes


def verify(journal, config):
    if journal.is_symlink() or not journal.is_file():
        raise ValueError('Missing or aliased authoritative journal')
    routes = json.loads(config.read_text())['routes']
    validate_cloud_routes(routes)
    destinations = {}
    for route in routes.values():
        project = str(route['project_id'])
        cloud = route['cloud']
        destination = cloud['host'].rstrip('/') + ':' + str(int(cloud['project_id']))
        if project in destinations and destinations[project] != destination:
            raise ValueError('Conflicting configured Cloud destinations')
        destinations[project] = destination
    pending = 0
    unmapped = 0
    with closing(sqlite3.connect(journal.resolve().as_uri() + '?mode=ro', uri=True)) as con:
        for project, destination, count in con.execute(
                'SELECT project,cloud_destination,count(*) FROM records '
                'GROUP BY project,cloud_destination'):
            expected = destinations.get(str(project))
            if expected is None:
                unmapped += count
            elif destination != expected:
                pending += count
    return {'drained': pending == unmapped == 0,
            'pending_or_wrong_destination': pending, 'unmapped_records': unmapped}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--journal', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.journal, args.config)
    print(json.dumps(result))
    raise SystemExit(0 if result['drained'] else 1)
