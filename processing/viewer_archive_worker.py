"""Private one-shot archive capture child; no queue ownership or acknowledgements.

The parent passes credentials through stdin and retains per-task exclusion. The
child inherits the parent's coordinator flocks so a surviving child cannot race
a replacement coordinator after parent death.
"""
import json
from pathlib import Path
import sys
from types import SimpleNamespace

from .viewer_service import NativeEngine, ViewerService


class _CaptureStore:
    def __init__(self, task):
        self.value = task

    def task(self, identifier):
        if identifier != self.value['uuid']:
            raise ValueError('capture identity changed')
        return self.value.copy()

    def update(self, identifier, **fields):
        if identifier != self.value['uuid'] or set(fields) != {'archive_path'}:
            raise ValueError('capture update invalid')
        self.value.update(fields)


def main():
    # Never emit exceptions or payloads: input contains a node credential.
    try:
        raw = sys.stdin.buffer.read(16385)
        if len(raw) > 16384:
            return 1
        value = json.loads(raw)
        if type(value) is not dict or set(value) != {'task', 'origin', 'token', 'maxBytes', 'freeFloor'}:
            return 1
        task = value['task']
        config = SimpleNamespace(viewer_max_archive_bytes=value['maxBytes'], viewer_archive_free_floor=value['freeFloor'])
        service = ViewerService(None, config, store=_CaptureStore(task),
                                engines={task['node_id']: NativeEngine(value['origin'], value['token'])})
        stream, _ = service.download(task['uuid'], automatic=True)
        stream.close()
        return 0
    except BaseException:
        # Only fixed exit codes cross the boundary; never reveal exception text.
        # A partial left by this or an earlier attempt requires inspection.
        try:
            directory = Path(task['directory'])
            if any(p.exists() or p.is_symlink() for p in
                   (directory / 'all.zip.partial', directory / '.all.zip.restore',
                    directory / '.all.zip.restore-pending')):
                return 2
        except BaseException:
            pass
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
