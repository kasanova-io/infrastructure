#!/usr/bin/env python3
"""Prepare the frozen native build, including the common journal-owned fence."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

HERE = Path(__file__).resolve().parent
FILES = [
    'Cargo.lock', 'Dockerfile', 'LICENSE.K-indexer', 'prepare_manifests.py',
    'real_kpost.json', 'real_tip.json', 'release_tip.json', 'transaction_tests.rs', 'unvote_tests.rs',
    'transactional-protocol.patch', 'shared_social.rs', 'shared_social.sql',
    'notification_capture.sql', 'notification_capture.sha256', 'tips.rs',
    'tip_evidence.rs', 'tip_projection.rs', 'kaposts.rs', 'kaposts_projection.rs',
    'kaposts_projection.sql', 'transaction_evidence.rs', 'rooms.rs', 'rooms.sql',
    'read_views.rs', 'read_views.sql', 'edit_authorization.sql', 'unquote_order.sql',
    'retained_signature_audit.rs', 'retained_history.rs',
]


def prepare(output):
    if output.exists():
        raise ValueError('fresh build directory required; preserve existing work')
    history = json.loads((HERE / 'SOURCE.lock').read_text())
    lock = history.get('bounded_vote_reads_candidate') or history.get('bounded_edit_reads_candidate') or history.get('room_sender_enrichment_candidate') or history.get('unvote_candidate') or history.get('retained_root_candidate') or history.get('unified_feed_candidate') or history.get('bounded_read_candidate', history['retained_history_candidate'])
    inputs = {name: HERE / name for name in FILES}
    # The journal installer and native verifier compile this SAME owned asset.
    inputs['source_fence.sql'] = HERE.parent / 'protocol_indexer/source_fence.sql'
    if set(inputs) != set(lock['files_sha256']):
        raise ValueError('reviewed native build input inventory mismatch')
    actual = {name: hashlib.sha256(path.read_bytes()).hexdigest()
              for name, path in inputs.items()}
    if actual != lock['files_sha256']:
        raise ValueError('reviewed native build input changed')
    identity = hashlib.sha256(json.dumps(actual, sort_keys=True,
                                        separators=(',', ':')).encode()).hexdigest()
    if identity != lock['build_source_manifest']:
        raise ValueError('native build manifest identity mismatch')
    output.mkdir(parents=True)
    for name, path in inputs.items():
        shutil.copy2(path, output / name)
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    print(prepare(parser.parse_args().output))
