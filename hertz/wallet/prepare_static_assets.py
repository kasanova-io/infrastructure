"""Copy a built Flutter web bundle to a new release directory with Caddy sidecars."""

import argparse
import gzip
import hashlib
from pathlib import Path
import shutil
import subprocess
import tempfile


COMPRESSIBLE = {".css", ".html", ".js", ".json", ".otf", ".svg", ".ttf", ".wasm"}


def write_etag(path, data):
    digest = hashlib.sha256(data).hexdigest()
    Path(str(path) + ".etag").write_text(f'"{digest}"\n', encoding="ascii")


def prepare(source, output):
    source = source.resolve()
    output = output.resolve()
    if output.exists() or output.is_relative_to(source):
        raise ValueError("Choose a new output directory outside the Flutter build")
    if not (source / "flutter_bootstrap.js").is_file():
        raise ValueError("The source must be a Flutter web build")

    # Publish the complete directory only after each representation and its validator exist.
    with tempfile.TemporaryDirectory(dir=output.parent) as scratch:
        staged = Path(scratch) / "bundle"
        shutil.copytree(source, staged, symlinks=True)
        for path in sorted(staged.rglob("*")):
            if path.is_symlink() or path.suffix in {".br", ".gz", ".etag"}:
                raise ValueError(f"Unexpected build entry: {path.relative_to(staged)}")
            if not path.is_file():
                continue
            raw = path.read_bytes()
            write_etag(path, raw)
            if path.suffix not in COMPRESSIBLE or len(raw) < 1024:
                continue
            brotli = subprocess.run(
                ["brotli", "--stdout", "--quality=9", "--", str(path)],
                stdout=subprocess.PIPE, check=True,
            ).stdout
            for extension, data in ((".br", brotli), (".gz", gzip.compress(raw, compresslevel=9, mtime=0))):
                if len(data) < len(raw):
                    representation = Path(str(path) + extension)
                    representation.write_bytes(data)
                    write_etag(representation, data)
        staged.rename(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    prepare(args.source, args.output)
