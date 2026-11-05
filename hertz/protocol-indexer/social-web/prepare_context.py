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
    history = json.loads((HERE / 'SOURCE.lock').read_text())
    # Keep the accepted reader's historical inputs/images intact. A new frozen
    # candidate records compatibility with the current native-owned SQL assets.
    lock = history.get('legacy_action_candidate') or history.get('retained_root_candidate') or history.get('unified_feed_candidate', history.get('action_protocol_candidate',
                       history.get('tip_counts_candidate', history.get('bounded_read_candidate', history.get('retained_schema_candidate', history)))))
    inputs = {name: HERE / name for name in [
        'Dockerfile', 'prepare_web.py', 'shared-api.patch', 'shared_rooms.rs',
        'shared_room_tests.rs', 'shared_reads.rs', 'shared_read_tests.rs', 'shared-read-api.patch', 'Cargo.lock',
        'tip_counts.rs', 'tip-counts.patch', 'protocol-metadata.patch', 'unified-feed.patch',
    ]}
    inputs['shared_room_schema.sql'] = HERE.parent / 'social/rooms.sql'
    for name, native in {'shared_read_schema.sql':'read_views.sql', 'shared_edit_authorization.sql':'edit_authorization.sql', 'shared_unquote_order.sql':'unquote_order.sql', 'shared_social_schema.sql':'shared_social.sql', 'shared_kaposts_schema.sql':'kaposts_projection.sql'}.items():
        inputs[name] = HERE.parent / 'social' / native
    for name in ['historical-author-keys.patch', 'exact-vote-details.patch', 'content-search.patch']:
        inputs[name] = HERE.parent.parent / 'social-indexer-dev/patches' / name
    if set(inputs) != set(lock['files_sha256']):
        raise ValueError('reviewed input inventory mismatch')
    for name, path in inputs.items():
        if hashlib.sha256(path.read_bytes()).hexdigest() != lock['files_sha256'][name]:
            raise ValueError('reviewed build input changed: ' + name)
    identity = hashlib.sha256(json.dumps(lock['files_sha256'], sort_keys=True,
                                        separators=(',', ':')).encode()).hexdigest()
    if identity != lock['source_manifest']:
        raise ValueError('reviewed build manifest identity mismatch')
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
