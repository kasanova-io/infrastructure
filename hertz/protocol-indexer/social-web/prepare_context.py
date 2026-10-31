#!/usr/bin/env python3
"""Prepare only the reviewed shared API inputs in a new build directory."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

HERE = Path(__file__).resolve().parent

def prepare(output):
    if output.exists():
        raise ValueError('fresh build directory required; preserve existing work')
    lock = json.loads((HERE / 'SOURCE.lock').read_text())
    inputs = {name: HERE / name for name in [
        'Dockerfile', 'prepare_web.py', 'shared-api.patch', 'shared_rooms.rs',
        'shared_room_tests.rs', 'Cargo.lock',
    ]}
    inputs['shared_room_schema.sql'] = HERE.parent / 'social/rooms.sql'
    for name in ['historical-author-keys.patch', 'exact-vote-details.patch', 'content-search.patch']:
        inputs[name] = HERE.parent.parent / 'social-indexer-dev/patches' / name
    if set(inputs) != set(lock['files_sha256']):
        raise ValueError('reviewed input inventory mismatch')
    for name, path in inputs.items():
        if hashlib.sha256(path.read_bytes()).hexdigest() != lock['files_sha256'][name]:
            raise ValueError('reviewed build input changed: ' + name)
    output.mkdir(parents=True)
    for name, path in inputs.items():
        shutil.copy2(path, output / name)
    return output

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(prepare(args.output))

if __name__ == '__main__':
    main()
