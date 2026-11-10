"""Verify the Viewer code used by the synthetic paired test without Git."""
import hashlib
import hmac
import json
import os
import stat
from pathlib import Path

SOURCE_COMMIT = '80bb4be0a6fe293146dc5cc37091f0a27045211f'
MANIFEST_SHA256 = '70f18634e4fcd5596a8edbca4714ecf22bb93f8086dca403b7903fd4f377a3d3'
MANIFEST = Path(__file__).with_name('native_viewer_80bb_manifest.json')


def verify_viewer_source(root):
    """Check all server files and two app-root dependencies against Git blobs.

    This proves the covered content, not the Git HEAD or unrelated repo files.
    Text line endings are normalized exactly as in the checked-in manifest.
    """
    def reject():
        raise ValueError('Viewer source does not match the pinned test manifest')

    raw = MANIFEST.read_bytes().replace(b'\r\n', b'\n')
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), MANIFEST_SHA256):
        reject()
    try:
        manifest = json.loads(raw)
        if (manifest['sourceCommit'] != SOURCE_COMMIT
                or manifest['algorithm'] != 'sha256-lf-normalized-text-v1'
                or type(manifest['files']) is not dict or not manifest['files']):
            reject()
        root = Path(root)
        if root.is_symlink() or not root.is_dir():
            reject()
        expected = manifest['files']
        server = root / 'apps/viewer/server'
        for part in (root/'apps', root/'apps/viewer', server):
            if part.is_symlink() or not part.is_dir():
                reject()
        actual = set()
        for directory, subdirs, files in os.walk(server, followlinks=False):
            directory = Path(directory)
            if any((directory / name).is_symlink() for name in subdirs):
                reject()
            for name in files:
                actual.add((directory / name).relative_to(root).as_posix())
        actual.update(('apps/viewer/lod-converter-policy.cjs', 'apps/viewer/package.json'))
        if actual != set(expected):
            reject()
        for relative, digest in expected.items():
            if (type(relative) is not str or relative.startswith('/') or '..' in Path(relative).parts
                    or type(digest) is not str or len(digest) != 64):
                reject()
            target = root / relative
            details = target.lstat()
            if not stat.S_ISREG(details.st_mode) or target.is_symlink() or details.st_size > 16 * 1024 * 1024:
                reject()
            data = target.read_bytes()
            if b'\0' in data or not hmac.compare_digest(hashlib.sha256(data.replace(b'\r\n', b'\n')).hexdigest(), digest):
                reject()
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        reject()
