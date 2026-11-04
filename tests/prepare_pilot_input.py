"""Copy a bounded photo sample into a new owned directory; never alter sources."""
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def prepare(source, target, count=25):
    source, target = Path(source), Path(target)
    if not source.is_absolute() or not target.is_absolute() or source.is_symlink() or not source.is_dir():
        raise ValueError('absolute, real source and target required')
    source, target = source.resolve(strict=True), target.absolute()
    if target.exists() or target.is_symlink() or not target.parent.is_dir() or target.parent.resolve() != target.parent:
        raise ValueError('target must be a new directory under a real parent')
    if source == target.parent or source in target.parents or target in source.parents:
        raise ValueError('source and target must be separate')
    if type(count) is not int or not 20 <= count <= 25:
        raise ValueError('sample count must be 20 through 25')
    images = sorted((p for p in source.iterdir() if p.suffix.lower() in ('.jpg', '.jpeg')),
                    key=lambda p: p.name.casefold())
    if len(images) < count or len({p.name.casefold() for p in images[:count]}) != count:
        raise ValueError('insufficient unique JPEGs')
    selected = images[:count]
    if any(p.is_symlink() or not p.is_file() for p in selected):
        raise ValueError('regular JPEG inputs required')
    target.mkdir(mode=0o700)
    (target / 'input').mkdir(mode=0o700)
    owner = uuid.uuid4().hex
    # Write ownership before copying; partial directories are preserved on error.
    marker = target / 'owner.json'
    with marker.open('x', encoding='utf-8') as out:
        json.dump({'schemaVersion': 1, 'owner': owner, 'state': 'preparing'}, out)
        out.flush(); os.fsync(out.fileno())
    files = []
    for source_file in selected:
        before = source_file.stat()
        dst = target / 'input' / source_file.name
        h = hashlib.sha256()
        with source_file.open('rb') as inp, dst.open('xb') as out:
            for chunk in iter(lambda: inp.read(1024 * 1024), b''):
                h.update(chunk); out.write(chunk)
            out.flush(); os.fsync(out.fileno())
        after = source_file.stat()
        if ((before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
                or dst.stat().st_size != before.st_size or sha(dst) != h.hexdigest()
                or sha(source_file) != h.hexdigest()):
            raise ValueError('source changed or copy verification failed; owned data retained')
        files.append({'relativePath': source_file.name, 'byteSize': before.st_size, 'sha256': h.hexdigest()})
    manifest = {'schemaVersion': 1, 'owner': owner, 'files': files}
    with (target / 'input-manifest.json').open('x', encoding='utf-8') as out:
        json.dump(manifest, out, separators=(',', ':')); out.flush(); os.fsync(out.fileno())
    return {'owner': owner, 'count': len(files), 'bytes': sum(x['byteSize'] for x in files)}


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit('usage: prepare_pilot_input.py SOURCE_DIR NEW_OWNED_DIR')
    # No source paths, filenames or file hashes are written to stdout.
    print(json.dumps(prepare(sys.argv[1], sys.argv[2])))
