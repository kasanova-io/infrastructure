#!/usr/bin/env python3
"""Build the installed capture revision with exact f64 JSON roundtrip parsing."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import urllib.request

ROOT = Path(__file__).resolve().parent
REVISION = '14a43b7a00ca32de5e85bfd913a8cecb858343c3'
COMPILER = 'rust@sha256:c1e5f19e773b7878c3f7a805dd00a495e747acbdc76fb2337a4ebf0418896b33'
RUNTIME = 'ghcr.io/posthog/posthog/capture@sha256:ab7d81924f8753012dea569fb539b893864b412fa8fc1483d3c10d36bc6a5e0c'
WORK = ROOT / 'operations' / 'capture-precise-build'


def run(command, **kwargs):
    return subprocess.check_output(command, stderr=subprocess.STDOUT, **kwargs)


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''): h.update(block)
    return h.hexdigest()


def main():
    os.umask(0o077)
    WORK.mkdir(mode=0o700, parents=True, exist_ok=True)
    if shutil.disk_usage(WORK).free < 30 * 1024**3:
        raise RuntimeError('Need 30 GiB free for the isolated build')
    source = WORK / 'source'
    source.mkdir(mode=0o700, exist_ok=True)
    receipt = WORK / 'source.json'
    if not receipt.exists():
        archive = WORK / 'source.tar.gz'
        with urllib.request.urlopen('https://codeload.github.com/PostHog/posthog/tar.gz/' + REVISION, timeout=60) as response, archive.open('wb') as output:
            shutil.copyfileobj(response, output)
        source_sha = sha(archive)
        files = 0
        with tarfile.open(archive, 'r:gz') as incoming:
            for entry in incoming:
                relative = '/'.join(PurePosixPath(entry.name).parts[1:])
                if not relative.startswith(('rust/', 'proto/')): continue
                if '..' in PurePosixPath(relative).parts or PurePosixPath(relative).is_absolute():
                    raise RuntimeError('Unsafe source member')
                path = source / relative
                if entry.isdir(): path.mkdir(mode=0o755, parents=True, exist_ok=True)
                elif entry.isfile():
                    path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
                    with incoming.extractfile(entry) as content, path.open('wb') as output: shutil.copyfileobj(content, output)
                    path.chmod(0o755 if entry.mode & 0o111 else 0o644)
                    files += 1
                elif entry.issym():
                    # Rust workspace fixtures may have symlinks; the selected
                    # capture package does not require them. Do not extract them.
                    continue
                else: raise RuntimeError('Unexpected source member type')
        receipt.write_text(json.dumps({'revision': REVISION, 'archive_sha256': source_sha, 'selected_files': files}) + '\n')
        archive.unlink()
    builder_file = WORK / 'Builder.Dockerfile'
    builder_file.write_text('FROM ' + COMPILER + '\nRUN apt-get update && apt-get install -y --no-install-recommends build-essential libssl-dev pkg-config cmake libclang-dev protobuf-compiler && rm -rf /var/lib/apt/lists/*\n')
    (WORK / '.dockerignore').write_text('**\n!Builder.Dockerfile\n')
    builder_tag = 'kasanova-posthog-capture-builder:' + REVISION[:12]
    log_path = WORK / 'build.log'
    with log_path.open('ab') as log:
        result = subprocess.run(['docker', 'build', '-f', str(builder_file), '-t', builder_tag, str(WORK)], stdout=log, stderr=subprocess.STDOUT)
        if result.returncode: raise RuntimeError('Compiler image build failed; inspect private build.log')
        builder_id = json.loads(run(['docker', 'image', 'inspect', builder_tag]))[0]['Id']
        cache = WORK / 'cargo-cache'; cache.mkdir(mode=0o700, exist_ok=True)
        for directory in ('registry', 'git'): (cache / directory).mkdir(mode=0o700, exist_ok=True)
        target = WORK / 'target'; target.mkdir(mode=0o700, exist_ok=True)
        command = ['docker', 'run', '--rm', '--name', 'kasanova-posthog-capture-precise-build',
            '--label', 'io.kasanova.posthog_build=precise-capture', '--cpus', '2', '--memory', '8g',
            '--mount', 'type=bind,source=' + str(source) + ',target=/source,readonly',
            '--mount', 'type=bind,source=' + str(cache / 'registry') + ',target=/usr/local/cargo/registry',
            '--mount', 'type=bind,source=' + str(cache / 'git') + ',target=/usr/local/cargo/git',
            '--mount', 'type=bind,source=' + str(target) + ',target=/target',
            '-e', 'CARGO_TARGET_DIR=/target', '-e', 'CARGO_BUILD_JOBS=2', '-e', 'PROTO_ROOT=../proto',
            '-w', '/source/rust', builder_id,
            'cargo', 'build', '--locked', '--release', '-p', 'capture', '--bin', 'capture',
            '--features', 'serde_json/float_roundtrip']
        print(json.dumps({'state': 'building_precise_capture', 'source_revision': REVISION,
                          'build_cpu_limit': 2, 'build_memory_gib': 8}), flush=True)
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode: raise RuntimeError('Capture compilation failed; inspect private build.log')
    binary = target / 'release/capture'
    context = WORK / 'runtime-context'; context.mkdir(mode=0o700, exist_ok=True)
    shutil.copyfile(binary, context / 'capture'); (context / 'capture').chmod(0o755)
    dockerfile = context / 'Dockerfile'
    dockerfile.write_text('FROM ' + RUNTIME + '\nCOPY capture /usr/local/bin/capture\nLABEL io.kasanova.capture.source-revision="' + REVISION + '" io.kasanova.capture.float-roundtrip="true"\n')
    tag = 'kasanova-posthog-capture:' + REVISION[:12] + '-float-roundtrip'
    with log_path.open('ab') as log:
        result = subprocess.run(['docker', 'build', '-t', tag, str(context)], stdout=log, stderr=subprocess.STDOUT)
        if result.returncode: raise RuntimeError('Runtime image build failed')
    image = json.loads(run(['docker', 'image', 'inspect', tag]))[0]
    report = {'completed_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'source': json.loads(receipt.read_text()),
        'compiler_image': COMPILER, 'builder_image_id': builder_id, 'base_runtime': RUNTIME,
        'cargo_feature': 'serde_json/float_roundtrip', 'upstream_source_modified': False,
        'binary_sha256': sha(binary), 'runtime_image_id': image['Id'], 'tag': tag,
        'runtime_source_or_license_features_changed': False, 'deployed': False}
    (WORK / 'build-manifest.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report), flush=True)


if __name__ == '__main__': main()
